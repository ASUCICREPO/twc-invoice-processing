import json
import boto3
import os
import datetime
from datetime import timedelta
import pytz
import csv
import io
from email import parser
from email.utils import parsedate_to_datetime
from typing import Dict, List, Tuple, Optional


class InvoiceProcessor:
    def __init__(self, email_bucket: str, artefact_bucket: str, result_bucket: str, timezone: str):
        print(f"Initializing InvoiceProcessor with buckets: email={email_bucket}, artefact={artefact_bucket}, result={result_bucket}")
        self.email_bucket = email_bucket
        self.artefact_bucket = artefact_bucket
        self.result_bucket = result_bucket
        self.timezone = timezone
        self.bedrock_runtime = boto3.client('bedrock-runtime')
        self.s3_client = boto3.client('s3')
        
        self.INVOICE_HEADERS = ['ReceiptDate', 'ReceiptTime', 'InvoiceNbr', 'VendorName', 'Amount', 'AcctAssigned']
        self.LOG_HEADERS = ['Timestamp', 'MessageId', 'InvoiceNbr', 'Status', 'ErrorReason', 'LLMConfidence']
    
    def _extract_email_datetime(self, message_id: str) -> datetime:
        """Extract datetime from email metadata."""
        print(f"Extracting datetime from email with message_id: {message_id}")
        obj = self.s3_client.get_object(Bucket=self.email_bucket, Key=message_id)
        email_content = obj['Body'].read().decode('utf-8')
        email_message = parser.Parser().parsestr(email_content)
        email_datetime = parsedate_to_datetime(email_message['Date'])
        local_tz = pytz.timezone(self.timezone)
        return email_datetime.astimezone(local_tz)
    
    def _get_next_business_day(self, date) -> datetime:
        """Calculate the next business day."""
        print(f"Calculating next business day from date: {date}")
        if (date.weekday() == 4 and date.hour >= 17) or date.weekday() in [5, 6]:
            days_ahead = 7 - date.weekday()
            next_business = date + timedelta(days=days_ahead)
            result = next_business.replace(hour=8, minute=0, second=0, microsecond=0)
            print(f"Weekend detected - Next business day: {result}")
            return result
        
        if date.hour >= 17:
            next_day = date + timedelta(days=1)
            result = next_day.replace(hour=8, minute=0, second=0, microsecond=0)
            print(f"After hours detected - Next business day: {result}")
            return result
        
        print(f"Using same business day: {date}")
        return date
    
    def _initialize_log_data(self, message_id: str, email_datetime: datetime) -> dict:
        """Initialize the log data structure with default values."""
        print(f"Initializing log data for message_id: {message_id}")
        return {
            'Timestamp': email_datetime,
            'MessageId': message_id,
            'InvoiceNbr': '',
            'Status': 'Success',
            'ErrorReason': '',
            'LLMConfidence': ''
        }
        
    def _is_valid_job(self, job: dict, log_data: dict) -> bool:
        """Check if the Textract job completed successfully."""
        print(f"Checking Textract job validity - JobId: {job.get('jobId')}, Status: {job.get('jobStatus')}")
        if job['jobStatus'] != 'SUCCEEDED' or 'resultsKey' not in job:
            log_data['Status'] = 'Error'
            log_data['ErrorReason'] = f"Textract Job status: {job['jobStatus']}"
            print(f"Error: Invalid Textract job - Status: {job['jobStatus']}, ResultsKey present: {'resultsKey' in job}")
            return False
        return True

    def _get_or_create_csv(self, date: datetime, suffix: str, headers: List[str]) -> Tuple[str, List[List[str]]]:
        """Get existing CSV or create new one with headers."""
        csv_filename = f"{date.strftime('%Y-%m-%d')}_{suffix}.csv"
        print(f"Accessing CSV file: {csv_filename}")
        
        try:
            csv_obj = self.s3_client.get_object(Bucket=self.result_bucket, Key=csv_filename)
            csv_content = csv_obj['Body'].read().decode('utf-8')
            print(f"Existing CSV file found: {csv_filename}")
            return csv_filename, list(csv.reader(io.StringIO(csv_content)))
        except self.s3_client.exceptions.NoSuchKey:
            print(f"Creating new CSV file: {csv_filename}")
            return csv_filename, [headers]
        
    def _write_csv(self, filename: str, rows: List[List[str]]) -> None:
        """Write CSV data to S3."""
        print(f"Writing {len(rows)} rows to CSV file: {filename}")
        output = io.StringIO()
        csv.writer(output).writerows(rows)
        self.s3_client.put_object(
            Bucket=self.result_bucket,
            Key=filename,
            Body=output.getvalue()
        )
        print(f"Successfully wrote data to {filename}")

    def _update_logs(self, target_date: datetime, log_data: dict) -> None:
        """Update the logs CSV file with new log data."""
        print(f"Updating logs for date: {target_date}, Status: {log_data['Status']}")
        log_csv_filename, log_rows = self._get_or_create_csv(
            target_date,
            'logs',
            self.LOG_HEADERS
        )
        
        log_rows.append([log_data[header] for header in self.LOG_HEADERS])
        self._write_csv(log_csv_filename, log_rows)
        
    def _is_invalid_document(self, expense_doc: dict, log_data: dict) -> bool:
        """Check if the document is invalid (e.g., statement, quote, etc.)."""
        print("Checking for invalid document types...")
        statement_keywords = ['statement', 'statements', 'statement as of', 'statement of']
        
        # Check for statements
        for block in expense_doc.get('Blocks', []):
            block_text = block.get('Text', '').lower()
            if any(keyword in block_text for keyword in statement_keywords):
                print(f"Invalid document detected - Statement keyword found: {block_text}")
                log_data['Status'] = 'Ignore'
                log_data['ErrorReason'] = 'Statement document detected'
                return True
        return False

    def _is_quote_or_estimate(self, field_label: str) -> bool:
        """Check if the document is a quote or estimate."""
        result = "quote" in field_label.lower() or "estimate" in field_label.lower()
        if result:
            print(f"Quote or estimate detected in field label: {field_label}")
        return result

    def _extract_invoice_fields(self, expense_doc: dict, invoice_data: dict) -> dict:
        """Extract invoice fields from Textract expense document."""
        print("Extracting invoice fields from expense document...")
        for field in expense_doc.get('SummaryFields', []):
            field_type = field.get('Type', {}).get('Text', '')
            field_value = field.get('ValueDetection', {}).get('Text', '')
            field_label = field.get('LabelDetection', {}).get('Text', '')
            
            if field_type == 'INVOICE_RECEIPT_ID':
                if self._is_quote_or_estimate(field_label):
                    raise ValueError("Quote or Estimate document detected")
                if invoice_data['invoice_number'] != '':
                    continue
                if not field_value:
                    raise ValueError("No Invoice Number found")
                invoice_data['invoice_number'] = field_value
            
            elif field_type == 'VENDOR_NAME':
                if not field_value:
                    raise ValueError("No Vendor Name found")
                if invoice_data['vendor_name'] != '':
                    continue
                invoice_data['vendor_name'] = field_value
            
            elif field_type == 'TOTAL':
                if invoice_data['amount'] != 0.0:
                    continue
                invoice_data['amount'] = field_value
           
        print(f"Extracted invoice data: {json.dumps(invoice_data)}")
        return invoice_data
    
    def _process_workquest_invoice(self, expense_doc: dict, invoice_data: dict) -> dict:
        """Special processing for Workquest invoices."""
        print("Processing Workquest invoice...")
        workquest_invoice_number_keywords = ['TINV']
        
        for block in expense_doc.get('Blocks', []):
            block_text = block.get('Text', '')
            if any(keyword in block_text for keyword in workquest_invoice_number_keywords):
                print(f"Found Workquest invoice number: {block_text}")
                invoice_data['invoice_number'] = block_text
                break
        
        return invoice_data   
    
    def _get_account_assignment_ruless(self) -> dict:
        """Fetch account assignment rules from S3."""
        print("Fetching account assignment rules from S3...")
        try:
            response = self.s3_client.get_object(
                Bucket=self.artefact_bucket,
                Key='account_assignment_rules.json'
            )
            rules = json.loads(response['Body'].read().decode('utf-8'))
            print(f"Successfully loaded {len(rules)} account assignment rules")
            return rules
        except Exception as e:
            print(f"Error getting account assignment rules: {str(e)}")
            raise 
        
    def _construct_claude_prompt(self, vendor_name: str, invoice_number: str, rules: dict) -> str:
        """Construct the prompt for Claude to determine account assignment."""
        print(f"Constructing Claude prompt for vendor: {vendor_name}, invoice: {invoice_number}")
        return f"""Given a vendor name: "{vendor_name}" and an invoice number: "{invoice_number}", determine the appropriate accountant assignment based on these rules:

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

    def determine_account_assignment(self, vendor_name: str, invoice_number: str) -> Optional[dict]:
        """Determine account assignment using Claude."""
        print(f"Determining account assignment for vendor: {vendor_name}, invoice: {invoice_number}")
        rules = self._get_account_assignment_ruless()
        if not rules:
            print("No account assignment rules found")
            return None

        prompt = self._construct_claude_prompt(vendor_name, invoice_number, rules)
        
        try:
            print("Invoking Claude model for account assignment...")
            response = self.bedrock_runtime.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 200,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": prompt}]
                }).encode()
            )
            response_body = json.loads(response['body'].read())
            result = json.loads(response_body['content'][0]['text'])
            print(f"Claude assignment result: {json.dumps(result)}")
            return result
        except Exception as e:
            print(f"Error in account assignment: {str(e)}")
            return None
    
    def _save_invoice_data(self, invoice_data: dict, email_datetime: datetime, target_date: datetime, log_data: dict) -> None:
        """Save processed invoice data to S3."""
        print(f"Saving invoice data for date: {target_date}")
        account_assignment = self.determine_account_assignment(
            invoice_data['vendor_name'],
            invoice_data['invoice_number']
        )
        
        log_data['InvoiceNbr'] = invoice_data['invoice_number']
        log_data['LLMConfidence'] = account_assignment['confidence'] if account_assignment else ''

        csv_filename, existing_rows = self._get_or_create_csv(
            target_date,
            "invoices",
            self.INVOICE_HEADERS
        )
        
        new_row = [
            email_datetime.strftime('%Y-%m-%d'),
            email_datetime.strftime('%H:%M:%S'),
            invoice_data['invoice_number'],
            invoice_data['vendor_name'],
            invoice_data['amount'],
            account_assignment['accountant'] if account_assignment else ''
        ]
        
        print(f"Adding new invoice row: {new_row}")
        self._write_csv(csv_filename, existing_rows + [new_row])

    def _process_textract_results(self, job: dict, log_data: dict) -> dict:
        """Process Textract results and extract invoice information."""
        print(f"Processing Textract results for job: {job.get('jobId')}")
        results_obj = self.s3_client.get_object(
            Bucket=self.artefact_bucket,
            Key=job['resultsKey']
        )
        results = json.loads(results_obj['Body'].read().decode('utf-8'))
        
        invoice_data = {
            'invoice_number': '',
            'vendor_name': '',
            'amount': 0.0
        }
        
        for expense_doc in results.get('ExpenseDocuments', []):
            print("Processing expense document...")
            if self._is_invalid_document(expense_doc, log_data):
                return invoice_data
            
            invoice_data = self._extract_invoice_fields(expense_doc, invoice_data)
            
            if invoice_data['vendor_name'].lower() == 'workquest':
                print("Workquest vendor detected - using special processing")
                invoice_data = self._process_workquest_invoice(expense_doc, invoice_data)
                return invoice_data
            
        return invoice_data
        
    def process_textract_job(self, job: dict) -> None:
        """Process a single Textract job."""
        message_id = job['pdfKey'].split('/')[1]
        print(f"\nProcessing Textract job for message_id: {message_id}")
        email_datetime = self._extract_email_datetime(message_id)
        target_date = self._get_next_business_day(email_datetime)
        
        log_data = self._initialize_log_data(message_id, email_datetime)
        
        if not self._is_valid_job(job, log_data):
            print(f"Invalid job detected for message_id: {message_id}")
            self._update_logs(target_date, log_data)
            return

        try:
            invoice_data = self._process_textract_results(job, log_data)
            if log_data['Status'] != 'Ignore':
                print(f"Processing valid invoice for message_id: {message_id}")
                self._save_invoice_data(invoice_data, email_datetime, target_date, log_data)
        except Exception as e:
            log_data['Status'] = 'Error'
            log_data['ErrorReason'] = str(e)
            print(f"Error processing invoice for message_id: {message_id}: {str(e)}")
        
        self._update_logs(target_date, log_data)
        print(f"Completed processing for message_id: {message_id}, Status: {log_data['Status']}\n")

def handler(event, context):
    """AWS Lambda handler function."""
    print(f"Received event: {json.dumps(event)}")
    
    processor = InvoiceProcessor(
        email_bucket=os.environ['INPUT_BUCKET_NAME'],
        artefact_bucket=os.environ['ARTEFACT_BUCKET_NAME'],
        result_bucket=os.environ['RESULT_BUCKET_NAME'],
        timezone=os.environ['TIMEZONE']
    )
    
    total_jobs = len(event['textractJobs'])
    print(f"Processing {total_jobs} Textract jobs")
    
    for i, job in enumerate(event['textractJobs'], 1):
        print(f"\nProcessing job {i} of {total_jobs}")
        processor.process_textract_job(job)
    
    print("Lambda handler execution completed successfully")

    return {
        'statusCode': 200,
        'message': 'Successfully processed Textract results'
    }