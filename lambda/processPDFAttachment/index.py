import os
import boto3
import email

s3 = boto3.client('s3')

def handler(event, context):
    print(f"Extracting PDF attachment from the email...")
    bucket_namae = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    
    pdf_key = f'invoices/{message_id}.pdf'
    pdf_data = None
    
    obj = s3.get_object(Bucket=bucket_namae, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    msg = email.message_from_string(email_content)
    for part in msg.walk():
        if part.get_content_maintype() == 'application' and part.get_filename().endswith(('.pdf')):
            pdf_data = part.get_payload(decode=True)
            break
    
    if pdf_data:
        return {
            'statusCode': 200,
            'pdfKey': pdf_key,
            'pdfData': pdf_data
        }
    else:
        return {
            'statusCode': 400,
            'body': 'No PDF document attachment found'
        }