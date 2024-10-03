import boto3
import os
import base64
import json

s3 = boto3.client('s3')

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    bucket_name = os.environ['BUCKET_NAME']
    
    if not isinstance(event, list):
        print("Error: Expected a list of PDF data")
        return {
            'statusCode': 400,
            'body': 'Invalid input: expected a list of PDF data'
        }
    
    results = []
    for pdf_info in event:
        pdf_key = pdf_info.get('pdfKey')
        pdf_data = pdf_info.get('pdfData')
        
        if not pdf_key or not pdf_data:
            results.append({
                'status': 'error',
                'error': 'Missing PDF key or data',
                'pdfKey': pdf_key
            })
            continue
        
        try:
            print(f"Saving PDF to bucket [{bucket_name}], at location [{pdf_key}]...")
            pdf_binary = base64.b64decode(pdf_data.encode('utf-8'))
            
            s3.put_object(
                Bucket=bucket_name,
                Key=pdf_key,
                Body=pdf_binary,
                ContentType='application/pdf'
            )
            print(f"Successfully saved PDF to bucket [{bucket_name}], at location [{pdf_key}]!")
            results.append({
                'status': 'success',
                'pdfKey': pdf_key
            })
        except Exception as e:
            print(f"Error saving PDF: {str(e)}")
            results.append({
                'status': 'error',
                'error': str(e),
                'pdfKey': pdf_key
            })
    
    return {
        'statusCode': 200,
        'body': results
    }