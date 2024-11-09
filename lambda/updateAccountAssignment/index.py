import boto3
import os
from email import parser

s3_client = boto3.client('s3')

def handler(event, context):
    message_id = event['messageId']
    email_bucket_name = os.environ['EMAIL_BUCKET_NAME']
    artefact_bucket_name = os.environ['ARTEFACT_BUCKET_NAME']
    
    # Get email content from S3
    response = s3_client.get_object(
        Bucket=email_bucket_name,
        Key=message_id
    )
    email_content = response['Body'].read().decode('utf-8')
    
    # Parse email to get body
    email_message = parser.Parser().parsestr(email_content)
    
    # Extract body based on content type
    if email_message.is_multipart():
        for part in email_message.walk():
            if part.get_content_type() == "text/plain":
                email_body = part.get_payload(decode=True).decode()
                break
    else:
        email_body = email_message.get_payload(decode=True).decode()
    
    # Upload the file to S3
    s3_key = 'account_assignment_rules.txt'
    s3_client.put_object(
        Bucket=artefact_bucket_name,
        Key=s3_key,
        Body=email_body.encode('utf-8'),
        ContentType='text/plain'
    )
    
    return {
        'statusCode': 200,
        'body': f'Successfully updated account assignment rules.'
    }