import boto3
import os
import base64
import io
import email
from docx import Document
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

s3 = boto3.client('s3')

def extract_text_from_docx(docx_data):
    doc = Document(io.BytesIO(docx_data))
    full_text = []
    for para in doc.paragraphs:
        full_text.append(para.text)
    return '\n'.join(full_text)

def create_pdf_from_text(text):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    flowables = []
    
    for paragraph in text.split('\n'):
        flowables.append(Paragraph(paragraph, styles['Normal']))
    
    doc.build(flowables)
    return buffer.getvalue()

def handler(event, context):
    print(f"Processing Word Document attachment...")
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    attachment_filename = event['filename']
    
    original_filename = os.path.splitext(attachment_filename)[0]
    pdf_key = f'invoices/{message_id}/{original_filename}.pdf'
    doc_data = None
    
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    msg = email.message_from_string(email_content)
    for part in msg.walk():
        if part.get_content_maintype() == 'application' and part.get_filename() == attachment_filename:
            doc_data = part.get_payload(decode=True)
            break
    
    if doc_data:
        try:
            # Extract text from Word document
            extracted_text = extract_text_from_docx(doc_data)
            
            # Create PDF from extracted text
            pdf_data = create_pdf_from_text(extracted_text)
            
            return {
                'statusCode': 200,
                'pdfKey': pdf_key,
                'pdfData': base64.b64encode(pdf_data).decode('utf-8'),
                'originalFilename': original_filename
            }
        except Exception as e:
            print(f"Error processing document: {str(e)}")
            return {
                'statusCode': 500,
                'body': f'Error processing document: {str(e)}'
            }
    else:
        return {
            'statusCode': 400,
            'body': f'Word document attachment {attachment_filename} not found'
        }