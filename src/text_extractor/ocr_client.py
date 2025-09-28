import time

from mistralai import Mistral
import json
import base64
import gzip
import requests
from PyPDF2 import PdfMerger
from io import BytesIO
from text_extractor.config import MISTRAL_API_KEY, GCP_REGION, GCP_PROJECT, GCP_OCR_MODEL
from text_extractor.logger import get_logger
import google.auth
import google.auth.transport.requests

logger = get_logger("text_extractor.ocr_clients")

api_key = "API_KEY"
client = Mistral(api_key=api_key)
ocr_model = "mistral-ocr-latest"

async def mistral_ocr(batch_file):
    async with Mistral(api_key=MISTRAL_API_KEY) as mistral_client:
        # upload the file to the API
        batch_data = await mistral_client.files.upload_async(
           file = {
                "file_name": batch_file,
                "content": open(batch_file, "rb")},
            purpose = "batch")
        created_job = mistral_client.batch.jobs.create(
                        input_files=[batch_data.id],
                        model=ocr_model,
                        endpoint="/v1/ocr",
                        metadata={"job_type": "testing"}
                    )
        retrieved_job = mistral_client.batch.jobs.get(job_id=created_job.id)

        while retrieved_job.status in ["QUEUED", "RUNNING"]:
            retrieved_job = client.batch.jobs.get(job_id=created_job.id)
            # print(f"Status: {retrieved_job.status}")
            # print(f"Total requests: {retrieved_job.total_requests}")
            # print(f"Failed requests: {retrieved_job.failed_requests}")
            # print(f"Successful requests: {retrieved_job.succeeded_requests}")
            # print(
            #     f"Percent done: {round((retrieved_job.succeeded_requests + retrieved_job.failed_requests) / retrieved_job.total_requests, 4) * 100}%")
            time.sleep(60)
        result_success = await mistral_client.files.download_async(file_id=retrieved_job.output_file)
        result_error = await mistral_client.files.download_async(file_id=retrieved_job.error_file)

        return result_success, result_error

def get_access_token():
    """Get an OAuth2 access token using Application Default Credentials."""
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    # Refresh if needed
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token

def gcp_ocr_provider(jsonl_file):
    merger = PdfMerger()

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            data = json.loads(line.strip())
            b64_str = data["body"]["document"]["image_url"]

            # Strip prefix if present
            if b64_str.startswith("data:image"):
                b64_str = b64_str.split(",")[1]

            # Decode and decompress
            raw_bytes = base64.b64decode(b64_str)
            if raw_bytes[:2] == b"\x1f\x8b":  # gzip magic bytes
                raw_bytes = gzip.decompress(raw_bytes)

            # Add to PDF merger
            try:
                merger.append(BytesIO(raw_bytes))
            except Exception as e:
                print(f"⚠️ Skipping line {line_no}, not a valid PDF page: {e}")

    # Output merged PDF to memory
    output_buffer = BytesIO()
    merger.write(output_buffer)
    merger.close()

    merged_pdf_bytes = output_buffer.getvalue()
    merged_pdf_b64 = base64.b64encode(merged_pdf_bytes).decode("utf-8")

    logger.info("Obtaining access token via google-auth …")
    GCP_ACCESS_TOKEN = get_access_token()

    endpoint = (
        f"https://{GCP_REGION}-aiplatform.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_REGION}/publishers/mistralai/models/{GCP_OCR_MODEL}:streamRawPredict"
    )

    headers = {
        "Authorization": f"Bearer {GCP_ACCESS_TOKEN}",
        "Content-Type": "application/json; charset=UTF-8",
    }

    body = {
        "model": GCP_OCR_MODEL,
        "document": {
            "type": "document_url",
            "document_url": f"data:application/pdf;base64,{merged_pdf_b64}",
        },
        "include_image_base64": True,
    }

    resp = requests.post(endpoint, headers=headers, json=body)
    resp.raise_for_status()

    return resp.json()




async def gcp_mistral_ocr_client(file):
    ocr_results = gcp_ocr_provider(file)
    return ocr_results



