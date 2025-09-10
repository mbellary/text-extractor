import time

from mistralai import Mistral

from text_extractor.config import MISTRAL_API_KEY

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