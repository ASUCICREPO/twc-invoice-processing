import boto3
import os
import io
import email
import pandas as pd
import numpy as np
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.utils import simpleSplit

s3 = boto3.client('s3')

def extract_excel_data(excel_binary):
    excel_buffer = io.BytesIO(excel_binary)
    
    # Try different Excel engines
    engines = ['openpyxl', 'xlrd']
    last_error = None
    
    for engine in engines:
        try:
            return pd.read_excel(excel_buffer, engine=engine)
        except Exception as e:
            last_error = str(e)
            excel_buffer.seek(0)  # Reset buffer for next attempt
    
    # If all engines fail, raise an error with helpful message
    error_msg = f"Failed to read Excel file. Last error: {last_error}. "
    error_msg += "Please ensure your Lambda layer includes pandas, openpyxl (for .xlsx), and xlrd (for .xls)"
    raise Exception(error_msg)

def create_pdf_from_excel(df):
    buffer = io.BytesIO()
    page_width, page_height = landscape(letter)
    c = canvas.Canvas(buffer, pagesize=landscape(letter))
    
    try:
        # Calculate column widths and positions
        num_columns = len(df.columns)
        margin = 50
        available_width = page_width - (2 * margin)
        col_width = available_width / num_columns
        
        def draw_text_cell(text, x, y, width, font_size=10):
            if pd.isna(text):  # Check for NaN
                return y
            
            text = str(text).strip()
            if not text:  # Skip empty strings
                return y
            
            # Split text into lines that fit within column width
            lines = simpleSplit(text, c._fontname, font_size, width)
            
            for line in lines:
                c.drawString(x, y, line)
                y -= 14  # Line spacing
            
            return y
        
        y = page_height - margin
        row_height = 20
        
        # Write headers
        c.setFont("Helvetica-Bold", 10)
        min_y = y
        for col_idx, col in enumerate(df.columns):
            x = margin + (col_idx * col_width)
            header_y = draw_text_cell(col, x, y, col_width)
            min_y = min(min_y, header_y)
        
        y = min_y - 10  # Add some space after headers
        
        # Write data
        c.setFont("Helvetica", 10)
        for _, row in df.iterrows():
            if y < margin + 50:  # Check if we need a new page
                c.showPage()
                c.setFont("Helvetica", 10)
                y = page_height - margin
            
            min_y = y
            for col_idx, value in enumerate(row):
                x = margin + (col_idx * col_width)
                cell_y = draw_text_cell(value, x, y, col_width)
                min_y = min(min_y, cell_y)
            
            y = min_y - 10  # Move to next row, adding some space
        
        c.save()
        return buffer.getvalue()
    except Exception as e:
        raise Exception(f"Error creating PDF: {str(e)}")

def handler(event, context):
    print("Processing Excel attachment...")
    try:
        email_bucket_name = os.environ['EMAIL_BUCKET_NAME']
        artefact_bucket_name = os.environ['ARTEFACT_BUCKET_NAME']
        message_id = event['messageId']
        attachment_filename = event['filename']
        
        # Get email from S3
        try:
            obj = s3.get_object(Bucket=email_bucket_name, Key=message_id)
            email_content = obj['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error reading from S3: {str(e)}")
            raise Exception(f"Failed to read email from S3: {str(e)}")
        
        # Extract Excel attachment
        excel_data = None
        email_message = email.message_from_string(email_content)
        for part in email_message.walk():
            if part.get_content_maintype() == 'application' and part.get_filename() == attachment_filename:
                excel_data = part.get_payload(decode=True)
                break
        
        if not excel_data:
            return {
                'statusCode': 404,
                'status': 'error',
                'body': f'No Excel attachment found'
            }
        
        # Process Excel file
        print("Extracting data from Excel...")
        df = extract_excel_data(excel_data)
        
        # Create PDF
        print("Creating PDF...")
        pdf_data = create_pdf_from_excel(df)
        
        # Save PDF to S3
        original_filename = os.path.splitext(attachment_filename)[0]
        pdf_key = f'invoices/{message_id}/{original_filename}.pdf'
        print(f"Saving PDF to S3: {pdf_key}")
        s3.put_object(
            Bucket=artefact_bucket_name,
            Key=pdf_key,
            Body=pdf_data,
            ContentType='application/pdf'
        )
        
        return {
            'statusCode': 200,
            'status': 'success',
            'pdfKey': pdf_key
        }
        
    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
        return {
            'statusCode': 500,
            'status': 'error',
            'error': str(e)
        }