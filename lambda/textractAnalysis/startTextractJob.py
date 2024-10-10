import json
import boto3
import os

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    artefact_bucket_name = os.environ['ARTEFACT_BUCKET_NAME']
    
    try:
        # Start a Textract job for each PDF in the processed attachments
        textract_jobs = []
        
        for item in event:
            if item['statusCode'] == 200 and 'pdfKey' in item:
                print(f"Starting Textract job for PDF: {item['pdfKey']}")
                
                response = textract_client.start_expense_analysis(
                    DocumentLocation={
                        'S3Object': {
                            'Bucket': artefact_bucket_name,
                            'Name': item['pdfKey']
                        }
                    }
                )
                
                textract_jobs.append({
                    'jobId': response['JobId'],
                    'pdfKey': item['pdfKey'],
                    'jobStatus': 'IN_PROGRESS'
                })
        
        return {
            'statusCode': 200,
            'textractJobs': textract_jobs
        }
    
    except Exception as e:
        print(f"Error starting Textract jobs: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }