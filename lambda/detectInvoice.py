import json
import boto3
import os
import email

s3 = boto3.client('s3')

def handler(event, context):
    print("Executing detectInvoice: Subject does not contain 'UPDATE ACCOUNT ASSIGNMENTS'")
    
    # Retrieve the mail from S3
    bucket_name = os.environ['BUCKET_NAME']
    message_id = event['messageId']
    
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    
    msg = email.message_from_string(email_content)
    
    subject = msg['subject']
    sender = msg['from']
    body = msg.get_payload()
    
    print(f"Processing email from {sender} with subject: {subject}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('detectInvoice executed sucessfully')
    }
    