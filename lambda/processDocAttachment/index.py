import boto3
import os
import io
import docx2pdf
import email

s3 = boto3.client('s3')

def handler(event, context):
    print(f"Converting Word document to PDF...")
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    
    doc_data = None
    for part in email.message_from_string(email_content).walk():
        if part.get_content_maintype() == 'application' and part.get_filename().endswith(('.doc', '.docx')):
            doc_data = part.get_payload(decode=True)
            break
    
    if doc_data:
        input_io = io.BytesIO(doc_data)
        output_io = io.BytesIO()
        
        docx2pdf.convert(input_io, output_io)
        
        pdf_data = output_io.getvalue()
        pdf_key = f'invoices/{message_id}.pdf'
        
        return {
            'statusCode': 200,
            'pdfKey': pdf_key,
            'pdfData': pdf_data
        }
    else:
        return {
            'statusCode': 400,
            'body': 'No Word document attachment found'
        }