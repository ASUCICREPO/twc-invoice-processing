import json
import boto3
import os
from datetime import datetime, timedelta
from email import parser
import pytz
import csv
import io
from email.utils import parsedate_to_datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

# Get environment variables
INPUT_BUCKET = os.environ['INPUT_BUCKET_NAME']
ARTEFACT_BUCKET = os.environ['ARTEFACT_BUCKET_NAME']
TIMEZONE = os.environ['TIMEZONE']

def get_next_business_day(date):
    # If it's Friday after 5pm, Saturday, or Sunday, next business day is Monday
    if (date.weekday() == 4 and date.hour >= 17) or \
       date.weekday() == 5 or date.weekday() == 6:
        days_ahead = 7 - date.weekday()
        next_business = date + timedelta(days=days_ahead)
        return next_business.replace(hour=8, minute=0, second=0, microsecond=0)
    
    # If it's after 5pm on a weekday, next business day is tomorrow
    if date.hour >= 17:
        next_day = date + timedelta(days=1)
        return next_day.replace(hour=8, minute=0, second=0, microsecond=0)
    
    return date

def extract_email_datetime(message_id, input_bucket):
    print(f"Fetching email with Message ID: [{message_id}] from S3 bucket with name: [{input_bucket}]")
    obj = s3_client.get_object(Bucket=input_bucket, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    email_message = parser.Parser().parsestr(email_content)
    date_str = email_message['Date']
    email_datetime = parsedate_to_datetime(date_str)
    
    print(f"Converting date-time to timezone: [{TIMEZONE}]")
    local_tz = pytz.timezone(TIMEZONE)
    local_datetime = email_datetime.astimezone(local_tz)
    
    return local_datetime

def analyze_pdf_with_textract(pdf_key, input_bucket):
    response = ""
    print(f"Sending PDF with Key [{pdf_key}] to Amazon Textract...")
    try:
        response = textract_client.start_expense_analysis(
            Document={
                'S3Object': {
                    'Bucket': input_bucket,
                    'Name': pdf_key
                }
            }
        )
    except textract_client.exceptions.UnsupportedDocumentException:
        print("PDF format not supported directly. Attempting alternative approaches...")
        try:
            # Option 2: Download and try with raw bytes
            print(f"Fetching the PDF with Key [{pdf_key}] from S3 bucket with name [{input_bucket}]")
            pdf_object = s3_client.get_object(Bucket=input_bucket, Key=pdf_key)
            pdf_bytes = pdf_object['Body'].read()
            
            response = textract_client.analyze_expense(
                Document={'Bytes': pdf_bytes}
            )
        except Exception as e:
            print(f"Failed to process PDF after multiple attempts: {str(e)}")
            raise Exception(f"Unable to process PDF. Please ensure the PDF is in a format supported by Textract: {str(e)}")
    
    # Extract required fields
    invoice_number = ""
    vendor_name = ""
    amount = 0.0
    
    print("Parsing Textract response to get required fields...")
    # TODO: This is simplified - you'll need to implement proper parsing
    for expense_doc in response['ExpenseDocuments']:
        for field in expense_doc['SummaryFields']:
            if field['Type']['Text'] == 'INVOICE_RECEIPT_ID':
                invoice_number = field['ValueDetection']['Text']
            elif field['Type']['Text'] == 'VENDOR_NAME':
                vendor_name = field['ValueDetection']['Text']
            elif field['Type']['Text'] == 'TOTAL':
                amount = field['ValueDetection']['Text']
    
    return invoice_number, vendor_name, amount

def get_or_create_csv(date, result_bucket):
    csv_filename = f"{date.strftime('%Y-%m-%d')}_invoices.csv"
    
    try:
        print(f"Try to get existing CSV with name [{csv_filename}] from S3 bucket [{result_bucket}]")
        csv_obj = s3_client.get_object(Bucket=result_bucket, Key=csv_filename)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        existing_rows = list(csv.reader(io.StringIO(csv_content)))
    except s3_client.exceptions.NoSuchKey:
        print(f"CSV with name [{csv_filename}] does not exist S3 bucket [{result_bucket}]. Creating new CSV file...")
        existing_rows = [['Date', 'Time', 'Invoice Number', 'Vendor Name', 'Amount']]
    
    return csv_filename, existing_rows

def handler(event, context):    
    for record in event:
        if record['statusCode'] != 200:
            continue
        
        message_id = record['pdfKey'].split('/')[1]
        pdf_key = record['pdfKey']
        print(f"Processing invoice from email with Message ID: [{message_id}] with PDF Key: [{pdf_key}]")
        
        # Get email datetime
        print(f"Extracting email date and time from email with Message ID [{message_id}]...")
        email_datetime = extract_email_datetime(message_id, INPUT_BUCKET)
        
        print(f"Determining which day's CSV this should go into...")
        target_date = get_next_business_day(email_datetime)
        print(f"Computed business day for the invoice is [{target_date}]")
        
        print("Extracting information from PDF using Textract...")
        invoice_number, vendor_name, amount = analyze_pdf_with_textract(pdf_key, INPUT_BUCKET)
        
        print("Saving Textract output to CSV...")
        # Get or create appropriate CSV
        csv_filename, existing_rows = get_or_create_csv(target_date, ARTEFACT_BUCKET)
        
        # Add new row
        new_row = [
            email_datetime.strftime('%Y-%m-%d'),
            email_datetime.strftime('%H:%M:%S'),
            invoice_number,
            vendor_name,
            amount
        ]
        existing_rows.append(new_row)
        
        # Write back to S3
        output = io.StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerows(existing_rows)
        
        print(f"Saving updated CSV file with name [{csv_filename}] to S3 Bucket [{ARTEFACT_BUCKET}]")
        s3_client.put_object(
            Bucket=ARTEFACT_BUCKET,
            Key=csv_filename,
            Body=output.getvalue()
        )
        print(f"Successfully processed invoice with PDF Key [{pdf_key}] and saved it to CSV with name [{csv_filename}]")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed invoices')
    }