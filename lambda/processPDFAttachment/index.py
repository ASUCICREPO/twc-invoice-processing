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
        try:
            print(f"Saving PDF to bucket [{bucket_name}], at location [{pdf_key}]...")
            
            s3.put_object(
                Bucket=bucket_name,
                Key=pdf_key,
                Body=pdf_data,
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
            'body': f'PDF attachment {attachment_filename} not found'
        }