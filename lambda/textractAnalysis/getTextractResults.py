import json
import boto3

textract_client = boto3.client('textract')

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
                    job['results'] = response
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