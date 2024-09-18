
import json
import boto3
import os
import email

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')

def handler(event, context):
    # Get the email details fromt he SES event
    ses_notification = event['Records'][0]['ses']
    message_id = ses_notification['mail']['messageId']
    bucket_name = os.environ['BUCKET_NAME']
    
    # Retrieve the meail from S3
    obj = s3.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8') 
    
    # Parse the email
    msg = email.message_from_string(email_content)
    subject = msg['subject']
    
    # Check if the subject contains "UPDATED ACCOUNT ASSIGNMENTS"
    subject_contains_account_assignment = "UPDATED ACCOUNT ASSIGNMENTS" in subject
    
    # Start the Step Function execution
    state_machine_arn = os.environ['STATE_MACHINE_ARN']
    stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({
            'subjectContainsAccountAssignment': subject_contains_account_assignment,
            'messageId': message_id,
            'bucketName': bucket_name
        })
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Email processed successfully')
    }