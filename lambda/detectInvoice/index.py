import boto3
import os
import email

s3 = boto3.client('s3')

def handler(event, context):
    print("Executing detectInvoice: Subject does NOT contain 'UPDATED ACCOUNT ASSIGNMENTS'")
    
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    
    print(f"Retreiving email with messageId [{message_id}] from S3 bucket [{bucket_name}]")
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    
    print(f"Sucessfully retrieved email with messageId [{message_id}]! Parsing email...")
    msg = email.message_from_string(email_content)
    attachment_type = 'none'
    
    print("Checking for attachments...")
    for part in msg.walk():
        if part.get_content_maintype() == 'application':
            filename = part.get_filename()
            if filename:
                if filename.endswith('.pdf'):
                    print(f"Found a PDF attachment with name [{filename}]!")
                    attachment_type = 'pdf'
                elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                    print(f"Found an Excel attachment with name [{filename}]!")
                    attachment_type = 'excel'
                elif filename.endswith('.docx') or filename.endswith('.doc'):
                    print(f"Found an Word Document attachment with name [{filename}]!")
                    attachment_type = 'doc'
    if attachment_type =='none':
        print("No attachment found in the email")
        attachment_type = 'text'
       
    return {
        'statusCode': 200,
        'messageId': message_id,
        'attachmentType': attachment_type,
        'bucketName': bucket_name
    }
    