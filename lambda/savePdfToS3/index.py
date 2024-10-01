import boto3
import os
import base64

s3 = boto3.client('s3')

def handler(event, context):
    bucket_name = os.environ['BUCKET_NAME']
    
    pdf_key = event.get('pdfKey')
    pdf_data = event.get('pdfData')
    
    if not pdf_key or not pdf_data:
        return {
            'statusCode': 400,
            'body': 'Missing PDF key or data'
        }
    
    try:
        print(f"Trying to save pdf to bucket [{bucket_name}], at location [{pdf_key}]...")
        pdf_binary = base64.b64decode(pdf_data.encode('latin-1'))
        
        s3.put_object(
            Bucket=bucket_name,
            Key=pdf_key,
            Body=pdf_binary,
            ContentType='application/pdf'
        )
        print(f"Successfully saved pdf to bucket [{bucket_name}], at location [{pdf_key}]!")
        return {
            'statusCode': 200,
            'body': f'PDF saved successfully to {pdf_key}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error saving PDF: {str(e)}'
        }