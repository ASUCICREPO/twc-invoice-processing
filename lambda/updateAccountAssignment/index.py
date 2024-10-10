import boto3
import json
import os
from email import parser

s3_client = boto3.client('s3')

def parse_account_assignment_rules(email_body):
    rules = []
    lines = email_body.split('\n')
    table_started = False
    header_read = False
    rule_item = {"rule": "", "accountant_name": "", "exceptis_exceptionion": False}
    
    for line in lines:
        # Detect the start of the table
        if "*Vendor Name begins with*" in line or "*Accountant Name*" in line:
            print(f"Table detected!")
            table_started = True
            continue
        if "*Exception*" in line:
            print("Header read done! Assuming rest of the text is inside the table")
            header_read = True
            continue        
        if table_started and header_read:
            # Skip empty lines
            if not line.strip():
                print("Empty Line, skipping...")
                continue
            if line.strip() == "*":
                print(f"Encountered exception '*', adding current item to rule set!")
                rule_item["is_exception"] = True
                rules.append(rule_item)
                rule_item = {"rule": "", "accountant_name": "", "is_exception": False}   
                continue
            if(rule_item["rule"] != "" and rule_item["accountant_name"] != ""):
                print(f"Rule item is complete, adding current item to rule set!")
                rules.append(rule_item)
                rule_item = {"rule": "", "accountant_name": "", "is_exception": False}   
            if rule_item["rule"] == "":
                rule_item["rule"] = line.strip()
            elif rule_item["accountant_name"] == "":
                rule_item["accountant_name"] = line.strip()
    
    return rules

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
                email_body = part.get_payload()
                break
    else:
        email_body = email_message.get_payload()
    
    # Parse rules from email body
    rules = parse_account_assignment_rules(email_body)
    print(f"Rules after IO: {rules}")
    
    if not rules:
        return {
            'statusCode': 400,
            'body': 'No account assignment rules found in email'
        }
    
    # Upload the file to S3
    s3_key = 'account_assignment_rules.json'
    s3_client.put_object(
        Bucket=artefact_bucket_name,
        Key=s3_key,
        Body=json.dumps(rules, indent=2)
    )
    
    return {
        'statusCode': 200,
        'body': f'Successfully updated account assignment rules.'
    }