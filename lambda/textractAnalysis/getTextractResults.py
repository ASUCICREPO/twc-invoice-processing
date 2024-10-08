import json
import boto3
import os

textract_client = boto3.client('textract')
s3_client = boto3.client('s3')

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    try:
        textract_jobs = event['textractJobs']
        all_jobs_completed = True
        updated_jobs = []
        
        for job in textract_jobs:
            if job['jobStatus'] != 'SUCCEEDED':
                response = textract_client.get_expense_analysis(
                    JobId=job['jobId']
                )
                
                job['jobStatus'] = response['JobStatus']
                if response['JobStatus'] == 'SUCCEEDED':
                    results_key = f"textract-results/{job['jobId']}.json"
                    s3_client.put_object(
                        Bucket=os.environ['OUTPUT_BUCKET_NAME'],
                        Key=results_key,
                        Body=json.dumps(response),
                        ContentType='application/json'
                    )
                    job['resultsKey'] = results_key
                elif response['JobStatus'] == 'IN_PROGRESS':
                    all_jobs_completed = False
                elif response['JobStatus'] == 'FAILED':
                    print(f"Textract job failed for PDF: {job['pdfKey']}")
                    job['error'] = response.get('StatusMessage', 'Unknown error')
                
            updated_jobs.append(job)
        
        return {
            'statusCode': 200,
            'jobStatus': 'SUCCEEDED' if all_jobs_completed else 'IN_PROGRESS',
            'textractJobs': updated_jobs
        }
    
    except Exception as e:
        print(f"Error getting Textract results: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }