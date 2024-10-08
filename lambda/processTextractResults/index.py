import json
import boto3
import os
from datetime import timedelta
import pytz
import csv
import io
from email import parser
from email.utils import parsedate_to_datetime

s3_client = boto3.client('s3')

def get_next_business_day(date):
    if (date.weekday() == 4 and date.hour >= 17) or \
       date.weekday() == 5 or date.weekday() == 6:
        days_ahead = 7 - date.weekday()
        next_business = date + timedelta(days=days_ahead)
        return next_business.replace(hour=8, minute=0, second=0, microsecond=0)
    
    if date.hour >= 17:
        next_day = date + timedelta(days=1)
        return next_day.replace(hour=8, minute=0, second=0, microsecond=0)
    
    return date

def extract_email_datetime(message_id):
    obj = s3_client.get_object(Bucket=os.environ['INPUT_BUCKET_NAME'], Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    email_message = parser.Parser().parsestr(email_content)
    date_str = email_message['Date']
    email_datetime = parsedate_to_datetime(date_str)
    
    local_tz = pytz.timezone(os.environ['TIMEZONE'])
    return email_datetime.astimezone(local_tz)

def get_or_create_csv(date, output_bucket):
    csv_filename = f"{date.strftime('%Y-%m-%d')}_invoices.csv"
    
    try:
        csv_obj = s3_client.get_object(Bucket=output_bucket, Key=csv_filename)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        existing_rows = list(csv.reader(io.StringIO(csv_content)))
    except s3_client.exceptions.NoSuchKey:
        existing_rows = [['Date', 'Time', 'Invoice Number', 'Vendor Name', 'Amount']]
    
    return csv_filename, existing_rows

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    try:
        textract_jobs = event['textractJobs']
        
        for job in textract_jobs:
            if job['jobStatus'] != 'SUCCEEDED' or 'resultsKey' not in job:
                continue
            
            # Get Textract results from S3
            try:
                results_obj = s3_client.get_object(
                    Bucket=os.environ['OUTPUT_BUCKET_NAME'],
                    Key=job['resultsKey']
                )
                results = json.loads(results_obj['Body'].read().decode('utf-8'))
            except Exception as e:
                print(f"Error reading Textract results from S3: {str(e)}")
                continue
            
            # Extract message ID from PDF key
            message_id = job['pdfKey'].split('/')[1]
            
            # Extract fields from results
            invoice_number = ""
            vendor_name = ""
            amount = 0.0
            
            for expense_doc in results.get('ExpenseDocuments', []):
                for field in expense_doc.get('SummaryFields', []):
                    field_type = field.get('Type', {}).get('Text', '')
                    field_value = field.get('ValueDetection', {}).get('Text', '')
                    
                    if field_type == 'INVOICE_RECEIPT_ID':
                        invoice_number = field_value
                    elif field_type == 'VENDOR_NAME':
                        vendor_name = field_value
                    elif field_type == 'TOTAL':
                        amount = field_value
            
            # Get email datetime and determine target date
            email_datetime = extract_email_datetime(message_id)
            target_date = get_next_business_day(email_datetime)
            
            # Get or create appropriate CSV
            csv_filename, existing_rows = get_or_create_csv(target_date, os.environ['OUTPUT_BUCKET_NAME'])
            
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
            
            s3_client.put_object(
                Bucket=os.environ['OUTPUT_BUCKET_NAME'],
                Key=csv_filename,
                Body=output.getvalue()
            )
            print(f"Successfully processed invoice from PDF: {job['pdfKey']}")
        
        return {
            'statusCode': 200,
            'message': 'Successfully processed Textract results'
        }
    
    except Exception as e:
        print(f"Error processing Textract results: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }