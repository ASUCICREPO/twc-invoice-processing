import boto3
import datetime
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import ClientError

def check_file_exists(s3_client, bucket, key):
    """Check if a file exists in S3 bucket"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise e

def get_s3_file(s3_client, bucket, key):
    """Get file from S3 if it exists"""
    try:
        return s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise e

def create_email_message(sender, recipients, date, invoice_data=None, log_data=None):
    """Create email message with available attachments"""
    msg = MIMEMultipart()
    msg['Subject'] = f'Daily Invoice Processing Report - {date}'
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    
    # Determine email body based on available files
    if not invoice_data and not log_data:
        body = f"No invoice processing reports are available for {date}."
    else:
        available_files = []
        if invoice_data:
            available_files.append("invoice report")
        if log_data:
            available_files.append("log report")
        body = f"Please find attached the available {' and '.join(available_files)} for {date}."
    
    msg.attach(MIMEText(body, 'plain'))
    
    # Attach available files
    if invoice_data:
        invoice_attachment = MIMEApplication(invoice_data)
        invoice_attachment.add_header('Content-Disposition', 'attachment', 
                                   filename=f'{date}_invoices.csv')
        msg.attach(invoice_attachment)
    
    if log_data:
        log_attachment = MIMEApplication(log_data)
        log_attachment.add_header('Content-Disposition', 'attachment', 
                                filename=f'{date}_logs.csv')
        msg.attach(log_attachment)
    
    return msg

def handler(event, context):
    # Initialize AWS clients
    s3 = boto3.client('s3')
    ses = boto3.client('ses')
    
    # Get current date in CST
    cst_tz = datetime.timezone(datetime.timedelta(hours=-6))
    current_time = datetime.datetime.now(cst_tz)
    current_date = current_time.strftime('%Y-%m-%d')
    
    # Check if it's a weekday (0 = Monday, 4 = Friday)
    if current_time.weekday() > 4:
        print(f"Skipping report as {current_date} is not a weekday")
        return {
            'statusCode': 200,
            'body': 'Skipped - not a weekday'
        }
    
    bucket_name = os.environ['RESULT_BUCKET_NAME']
    sender_email = os.environ['SENDER_EMAIL']
    recipient_emails = os.environ['RECIPIENT_EMAILS'].split(',')
    
    # Expected file paths for the day
    invoice_csv_key = f"{current_date}_invoices.csv"
    log_csv_key = f"{current_date}_logs.csv"
    
    try:
        # Check and get files if they exist
        invoice_data = get_s3_file(s3, bucket_name, invoice_csv_key)
        log_data = get_s3_file(s3, bucket_name, log_csv_key)
        
        # If both files are missing, still send an email but with a "no files" message
        if not invoice_data and not log_data:
            print(f"No files found for {current_date}")
        
        # Create email message with available attachments
        msg = create_email_message(
            sender_email,
            recipient_emails,
            current_date,
            invoice_data,
            log_data
        )
        
        # Send email using SES
        ses.send_raw_email(
            Source=sender_email,
            Destinations=recipient_emails,
            RawMessage={'Data': msg.as_string()}
        )
        
        return {
            'statusCode': 200,
            'body': f'Successfully sent daily report email for {current_date}'
        }
        
    except Exception as e:
        print(f"Error processing daily report: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error sending daily report: {str(e)}'
        }