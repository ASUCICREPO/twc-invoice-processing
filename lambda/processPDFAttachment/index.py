import os
import base64
import boto3
import email

s3 = boto3.client('s3')

def handler(event, context):
    print(f"Extracting PDF attachment from the email...")
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    attachment_filename = event['filename']
    
    pdf_key = f'invoices/{message_id}/{attachment_filename}'
    pdf_data = None
    
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    msg = email.message_from_string(email_content)
    for part in msg.walk():
        if part.get_content_maintype() == 'application' and part.get_filename() == attachment_filename:
            pdf_data = part.get_payload(decode=True)
            break
    
    if pdf_data:
        return {
            'statusCode': 200,
            'pdfKey': pdf_key,
            'pdfData': base64.b64encode(pdf_data).decode('utf-8')
        }
    else:
        return {
            'statusCode': 400,
            'body': f'PDF attachment {attachment_filename} not found'
        }