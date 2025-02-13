import boto3
import os
import email

s3 = boto3.client('s3')

def handler(event, context):
    print("Executing detectInvoice: Subject does NOT contain 'UPDATED ACCOUNT ASSIGNMENTS'")
    
    email_bucket_name = os.environ['EMAIL_BUCKET_NAME']
    message_id = event['messageId']
    
    print(f"Retrieving email with messageId [{message_id}] from S3 bucket [{email_bucket_name}]")
    obj = s3.get_object(Bucket=email_bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    
    print(f"Successfully retrieved email with messageId [{message_id}]! Parsing email...")
    msg = email.message_from_string(email_content)
    attachments = []
    
    print("Checking for attachments...")
    for part in msg.walk():
        if part.get_content_maintype() == 'application':
            filename = part.get_filename()
            if filename:
                if filename.endswith('.pdf'):
                    print(f"Found a PDF attachment with name [{filename}]!")
                    attachments.append({'type': 'pdf', 'filename': filename})
                elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                    print(f"Found an Excel attachment with name [{filename}]!")
                    attachments.append({'type': 'excel', 'filename': filename})
                elif filename.endswith('.docx') or filename.endswith('.doc'):
                    print(f"Found an Word Document attachment with name [{filename}]!")
                    attachments.append({'type': 'doc', 'filename': filename})
    if attachments == []:
        attachments.append({'type': 'body', 'filename': 'email_body'})
    return {
        'statusCode': 200,
        'messageId': message_id,
        'attachments': attachments,
        'bucketName': email_bucket_name
    }
    