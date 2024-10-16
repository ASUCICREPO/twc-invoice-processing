import json
import boto3
import os
from datetime import timedelta
import pytz
import csv
import io
from email import parser
from email.utils import parsedate_to_datetime

bedrock_runtime = boto3.client('bedrock-runtime')
s3_client = boto3.client('s3')

def get_account_assignment_rules(bucket_name):
    print("Fetching the rule set...")
    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key='account_assignment_rules.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Error getting account assignment rules: {str(e)}")
        raise

def determine_account_assignment(vendor_name, invoice_number, bucket_name):
    print("Finding Accountant name...")
    rules = get_account_assignment_rules(bucket_name)
    if not rules:
        return None

    print("Construct prompt for Claude")
    prompt = f"""Given a vendor name: "{vendor_name}" and an invoice number: "{invoice_number}", determine the appropriate accountant assignment based on these rules:

{json.dumps(rules, indent=2)}

Rules explanation:
1. Each rule contains a rule (which can be a single letter, number, or specific vendor name) and an accountant name
2. Some rules are marked as exceptions (is_exception: true)
3. Some rules for have specific invoice_pattern requirements

Assignment logic:
1. First, check for any matching exception rules (where is_exception is true)
2. Use invoice number information present in the rule if applicable
3. If no exception matches, use the standard rule based on the the vendor name

Return your response as a JSON object with these fields only:
- accountant: the assigned accountant's name
- rule_matched: description of the rule that was matched
- confidence: "high" if there's a clear match, "medium" if it's a probable match, "low" if uncertain
Do not provide anything else in the response. Your response must strictly be in JSON format.

Response format:
{{
    "accountant": "accountant name",
    "rule_matched": "description of the matched rule",
    "confidence": "high/medium/low"
}}"""

    print("Sending request to Claude...")
    try:
        response = bedrock_runtime.invoke_model(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 200,
                "temperature": 0,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }).encode()
        )
        print("Received response from Claude!")
    except Exception as e:
        print(f"Error occurred while calling Claude: {str(e)}")
        raise
    
    try:
        response_body = json.loads(response['body'].read())
        result = response_body['content'][0]['text']
        return json.loads(result)
    except Exception as e:
        print(f"Error occurred while trying to extract the JSON from Claude response: {str(e)}")

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

def extract_email_datetime(message_id, bucket_name, timezone):
    obj = s3_client.get_object(Bucket=bucket_name, Key=message_id)
    email_content = obj['Body'].read().decode('utf-8')
    email_message = parser.Parser().parsestr(email_content)
    date_str = email_message['Date']
    email_datetime = parsedate_to_datetime(date_str)
    
    local_tz = pytz.timezone(timezone)
    return email_datetime.astimezone(local_tz)

def get_or_create_csv(date, result_bucket, suffix, headers):
    csv_filename = f"{date.strftime('%Y-%m-%d')}_{suffix}.csv"
    
    try:
        csv_obj = s3_client.get_object(Bucket=result_bucket, Key=csv_filename)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        existing_rows = list(csv.reader(io.StringIO(csv_content)))
    except s3_client.exceptions.NoSuchKey:
        existing_rows = [headers]
    
    return csv_filename, existing_rows

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    email_bucket_name = os.environ['INPUT_BUCKET_NAME']
    artefact_bucket_name = os.environ['ARTEFACT_BUCKET_NAME']
    result_bucket_name = os.environ['RESULT_BUCKET_NAME']
    timezone = os.environ['TIMEZONE']
    
    textract_jobs = event['textractJobs']
    
    invoice_headers = ['ReceiptDate', 'ReceiptTime', 'InvoiceNbr', 'VendorName', 'Amount', 'AcctAssigned']
    log_headers = ['MessageId', 'InvoiceNbr', 'Status', 'ErrorReason', 'LLMConfidence']
    
    for job in textract_jobs:
        # Extract message ID from PDF key
        message_id = job['pdfKey'].split('/')[1]
        
        print(f"Fetching email datetime and determine target date for email with Message ID: [{message_id}]")
        email_datetime = extract_email_datetime(message_id, email_bucket_name, timezone)
        target_date = get_next_business_day(email_datetime)
        
        # Prepare log data
        log_data = {
            'MessageId': message_id,
            'InvoiceNbr': '',
            'Status': 'Success',
            'ErrorReason': '',
            'LLMConfidence': ''
        }
        
        if job['jobStatus'] != 'SUCCEEDED' or 'resultsKey' not in job:
            log_data['Status'] = 'Error'
            log_data['ErrorReason'] = f"Textract Job status: {job['jobStatus']}"
            print(f"Error processing Textract Job with jobID: [{job['jobId']}]...")
        
        # Get Textract results from S3
        else:
            try:
                print(f"Fetching textract result for jobId: [{job['jobId']}]...")
                results_obj = s3_client.get_object(
                    Bucket=artefact_bucket_name,
                    Key=job['resultsKey']
                )
                results = json.loads(results_obj['Body'].read().decode('utf-8'))
                
                # Extract fields from results
                invoice_number = ""
                vendor_name = ""
                amount = 0.0
                
                print("Finding relevant values from Textract result...")
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
                if invoice_number == "":
                    raise Exception("Could not find invoice number, the document is not an invoice!")
                if vendor_name == "":
                    raise Exception("Could not find vendor name, the document is not an invoice!")
                
                print("Fetching Account Assignee value...")
                account_assignment = determine_account_assignment(
                    vendor_name, 
                    invoice_number,
                    artefact_bucket_name
                )
                assigned_accountant = account_assignment['accountant'] if account_assignment else ''
                
                log_data['InvoiceNbr'] = invoice_number
                log_data['LLMConfidence'] = account_assignment['confidence'] if account_assignment else ''
                
                print("Get or create CSV for business day")
                csv_filename, existing_rows = get_or_create_csv(target_date, result_bucket_name, "invoices", invoice_headers)
                
                # Add new row
                new_row = [
                    email_datetime.strftime('%Y-%m-%d'),
                    email_datetime.strftime('%H:%M:%S'),
                    invoice_number,
                    vendor_name,
                    amount,
                    assigned_accountant
                ]
                existing_rows.append(new_row)
                
                print("Write the CSV back to S3")
                output = io.StringIO()
                csv_writer = csv.writer(output)
                csv_writer.writerows(existing_rows)
                
                s3_client.put_object(
                    Bucket=result_bucket_name,
                    Key=csv_filename,
                    Body=output.getvalue()
                )
                print(f"Successfully processed invoice from PDF: {job['pdfKey']}")
            except Exception as e:
                log_data['Status'] = 'Error'
                log_data['ErrorReason'] = str(e)
                print(f"Error processing invoice: {str(e)}")
        print("Updating log CSV...")
        log_csv_filename, log_rows = get_or_create_csv(target_date, result_bucket_name, 'logs', log_headers)
        log_rows.append([log_data[header] for header in log_headers])
        
        print(f"Writing log CSV back to S3...")
        log_output = io.StringIO()
        csv.writer(log_output).writerows(log_rows)
        s3_client.put_object(
            Bucket=result_bucket_name,
            Key=log_csv_filename,
            Body=log_output.getvalue()
        )
    return {
        'statusCode': 200,
        'message': 'Successfully processed Textract results'
    }