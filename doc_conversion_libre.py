import os
import subprocess
import time
import boto3
from pdf2image import convert_from_path
from fpdf import FPDF
from PIL import Image

# AWS Credentials must be configured via environment or AWS CLI
textract_client = boto3.client('textract', region_name='us-east-1')

input_file = "input.docx"   # Change as needed
converted_pdf = "converted.pdf"
output_pdf = "ocr_output.pdf"
s3_bucket = "your-s3-bucket-name"
s3_key = "uploads/converted.pdf"

# Step 1: Convert Office file to PDF
def convert_to_pdf(input_file, output_pdf):
    subprocess.run(['libreoffice', '--headless', '--convert-to', 'pdf', input_file, '--outdir', '.'])
    os.rename(input_file.replace(os.path.splitext(input_file)[1], '.pdf'), output_pdf)

# Step 2: Upload to S3
def upload_to_s3(file_path, bucket, key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket, key)
    print(f"Uploaded {file_path} to s3://{bucket}/{key}")

# Step 3: Start Textract Job
def start_textract_job(bucket, key):
    response = textract_client.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
    )
    return response['JobId']

# Step 4: Wait for Textract to finish
def wait_for_job(job_id):
    while True:
        response = textract_client.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']
        if status in ['SUCCEEDED', 'FAILED']:
            return response
        time.sleep(5)

# Step 5: Collect Textract text
def extract_text_from_textract(response):
    text = ""
    next_token = None
    while True:
        if next_token:
            response = textract_client.get_document_text_detection(JobId=response['JobId'], NextToken=next_token)
        blocks = response['Blocks']
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text += block['Text'] + '\n'
        next_token = response.get('NextToken')
        if not next_token:
            break
    return text

# Step 6: Generate searchable PDF
def generate_pdf_with_text(original_pdf, extracted_text, output_pdf):
    images = convert_from_path(original_pdf)
    pdf = FPDF()
    for img in images:
        img_path = "temp.jpg"
        img.save(img_path, "JPEG")
        pdf.add_page()
        pdf.image(img_path, x=0, y=0, w=210, h=297)  # A4 size
        os.remove(img_path)
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.multi_cell(0, 10, extracted_text)
    pdf.output(output_pdf)
    print(f"OCR Searchable PDF saved as {output_pdf}")

# Full Flow
convert_to_pdf(input_file, converted_pdf)
upload_to_s3(converted_pdf, s3_bucket, s3_key)
job_id = start_textract_job(s3_bucket, s3_key)
print("Textract job started, waiting for completion...")
response = wait_for_job(job_id)
print("Textract job completed.")
extracted_text = extract_text_from_textract(response)
generate_pdf_with_text(converted_pdf, extracted_text, output_pdf)