import boto3
import os
import io
import email
import base64
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

s3 = boto3.client('s3')

def handler(event, context):
    print(f"Converting Email body to PDF...")
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    
    pdf_key = f'invoices/{message_id}/email_body.pdf'
    obj =  s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    
    msg = email.message_from_string(email_content)
    
    text_content = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == 'text/plain':
                text_content = part.get_payload(decode=True).decode('utf-8')
                break
    else:
        text_content = msg.get_payload(decode=True).decode('utf-8')
    
    if text_content:
        buffer = io.BytesIO()
        pdf = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = [Paragraph(text_content, styles['Normal'])]
        pdf.build(elements)
        
        pdf_data = buffer.getvalue()
        
        try:
            print(f"Saving PDF to bucket [{bucket_name}], at location [{pdf_key}]...")
            pdf_binary = pdf_data.encode('utf-8')
            
            s3.put_object(
                Bucket=bucket_name,
                Key=pdf_key,
                Body=pdf_binary,
                ContentType='application/pdf'
            )
            print(f"Successfully saved PDF to bucket [{bucket_name}], at location [{pdf_key}]!")
            result = {
                'statusCode': 200,
                'status': 'success',
                'pdfKey': pdf_key
            }
        except Exception as e:
            print(f"Error saving PDF: {str(e)}")
            result = {
                'statusCode': 400,
                'status': 'error',
                'error': str(e),
                'pdfKey': pdf_key
            }
        return result
    else:
        return {
            'statusCode': 404,
            'status': 'error',
            'body': 'No text content found in the email'
        }