import asyncio
import base64
import io
import os
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import time
import tempfile
from datetime import datetime
import pyarrow as pa
import boto3
import pyarrow.parquet as pq
import aioboto3

from text_extractor.config import PROCESS_POOL_WORKERS, THREAD_POOL_WORKERS, PDF_S3_BUCKET, JSONL_MAX_CHUNK_SIZE_MB, \
    JSONL_MAX_NUM_PAGES, OCR_S3_BUCKET, OCR_JSONL_SQS_QUEUE_NAME, OCR_JSONL_STATE_NAME, OCR_PARQUET_STATE_NAME
from text_extractor.logger import get_logger
from text_extractor.ocr_client import mistral_ocr
from text_extractor.utils import download_s3_to_file, check_if_file_enqueued
from text_extractor.aws_clients import get_aboto3_client, get_boto3_client
from text_extractor.utils import get_queue_url

logger = get_logger("processors")


# Async wrappers that offload to process / thread pool
process_pool = ProcessPoolExecutor(max_workers=PROCESS_POOL_WORKERS)
thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

async def forward_to_dlq(sqs_client, dlq_url: str, body: str, attributes: dict):
    """Optionally forward message body or the failed S3 keys to DLQ manually """
    if not dlq_url:
        logger.warning("DLQ URL not configured; skipping forward", extra={"body": body})
        return
    await sqs_client.send_message(QueueUrl=dlq_url, MessageBody=body, MessageAttributes=attributes or {})

async def _get_dynamo_item(ddb, s3_key: str):
    resp = await ddb.get_item(TableName=OCR_PARQUET_STATE_NAME,
                              Key={"s3_key": {"S": s3_key}},
                              ConsistentRead=True)
    return resp.get("Item")

async def _init_or_mark_processing(ddb, s3_key: str):
    """
    Ensure an item exists and set status=processing if not done.
    Returns item (may be None if newly created).
    """
    item = await _get_dynamo_item(ddb, s3_key)
    if item:
        status = item.get("status", {}).get("S")
        if status == "done":
            return item  # done already
    # Put or update to set processing and ensure attempts/pages_processed exist
    now = _now_iso()
    # Use update_item to create if not exists and set status to processing
    await ddb.update_item(
        TableName=OCR_PARQUET_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="SET #st = :processing, #lu = :lu, attempts = if_not_exists(attempts, :zero_n), pages_processed = if_not_exists(pages_processed, :zero_n) ",
        ExpressionAttributeNames={"#st": "status", "#lu": "last_updated"},
        ExpressionAttributeValues={":processing": {"S": "processing"}, ":lu": {"S": now}, ":zero_n": {"N": "0"}},
    )
    return await _get_dynamo_item(ddb, s3_key)

async def _increment_pages_processed(ddb, s3_key: str, increment: int = 1):
    now = _now_iso()
    await ddb.update_item(
        TableName=OCR_PARQUET_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="ADD pages_processed :inc SET last_updated = :lu",
        ExpressionAttributeValues={":inc": {"N": str(increment)}, ":lu": {"S": now}},
    )

async def _set_done(ddb, s3_key: str, total_pages: int):
    now = _now_iso()
    await ddb.update_item(
        TableName=OCR_PARQUET_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="SET #st = :done, total_pages = :tp, last_updated = :lu",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":done": {"S": "done"}, ":tp": {"N": str(total_pages)}, ":lu": {"S": now}},
    )

async def _set_enqeue(ddb, s3_key: str, total_pages: int):
    now = _now_iso()
    await ddb.update_item(
        TableName=OCR_PARQUET_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="SET #st = :enqeued, total_pages = :tp, last_updated = :lu",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":done": {"S": "done"}, ":tp": {"N": str(total_pages)}, ":lu": {"S": now}},
    )

async def _mark_failure(ddb, s3_key: str, err_msg: str = ""):
    now = _now_iso()
    # increment attempts and set status=failed
    await ddb.update_item(
        TableName=OCR_PARQUET_STATE_NAME,
        Key={"s3_key": {"S": s3_key}},
        UpdateExpression="ADD attempts :one SET #st = :failed, last_error = :err, last_updated = :lu",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":one": {"N": "1"}, ":failed": {"S": "failed"}, ":err": {"S": err_msg[:1024]}, ":lu": {"S": now}},
    )


class JSONLBatchWriter:
        def __init__(self, s3_bucket, output_prefix, max_chunk_size_mb=JSONL_MAX_CHUNK_SIZE_MB, max_num_pages=JSONL_MAX_NUM_PAGES):
            # self.s3 = boto3.client("s3", region_name = AWS_REGION,
            #                                         aws_access_key_id=TEST_AWS_DDB_ACCESS_KEY_ID,
            #                                         aws_secret_access_key=TEST_AWS_DDB_SECRET_ACCESS_KEY_ID,
            #                                         endpoint_url=TEST_ENDPOINT_URL)
            # self.sqs = boto3.client("sqs" , region_name = AWS_REGION,
            #                                         aws_access_key_id=TEST_AWS_DDB_ACCESS_KEY_ID,
            #                                         aws_secret_access_key=TEST_AWS_DDB_SECRET_ACCESS_KEY_ID,
            #                                         endpoint_url=TEST_ENDPOINT_URL)
            # self.ddb = boto3.client("dynamodb", region_name = AWS_REGION,
            #                                         aws_access_key_id=TEST_AWS_DDB_ACCESS_KEY_ID,
            #                                         aws_secret_access_key=TEST_AWS_DDB_SECRET_ACCESS_KEY_ID,
            #                                         endpoint_url=TEST_ENDPOINT_URL)
            self.s3 = get_boto3_client("s3")
            self.sqs = get_boto3_client("sqs")
            self.ddb = get_boto3_client("dynamodb")
            self.bucket = s3_bucket
            self.prefix = output_prefix
            self.max_chunk = max_chunk_size_mb * 1024 * 1024
            self.max_pages = max_num_pages
            self.buffer = {"custom_id": [], "body": []} # keys as per Mistral batch_ocr api
            self.current_size = 0
            self.current_page_count = 0
            self.parts = []

        def _encode_image_data(self, page):
            return base64.b64encode(page).decode("utf-8")

        def add_page(self, doc_id, page_bytes):
            base64_image = self._encode_image_data(page_bytes)
            image_url = f"data:image/jpeg;base64,{base64_image}"
            body_content = {
                    "document": {
                        "type": "image_url",
                        "image_url": image_url
                    },
                    "include_image_base64": True
                }
            
            """Add one page to buffer and flush if needed."""
            self.buffer["custom_id"].append(doc_id)
            self.buffer["body"].append(body_content)
    
            # estimate incremental JSONL size.
            temp_json = {
                "custom_id": [doc_id],
                "body": body_content
            }            
            buf = io.BytesIO(json.dumps(temp_json).encode('utf-8'))
            self.current_size += buf.getbuffer().nbytes
            self.current_page_count += 1
    
            if self.current_size >= self.max_chunk or self.current_page_count >= self.max_pages:
                self.flush()

        def flush(self):
            """Write buffer to jsonl and upload to S3."""
            if not self.buffer["custom_id"]:
                return
            table = pa.Table.from_pydict({
                "custom_id": self.buffer["custom_id"],
                "body": self.buffer["body"]
            })
            df = table.to_pandas()
            buf = io.BytesIO()
            df.to_json(buf, orient='records', lines=True)
            buf.seek(0)
    
            key = f"{self.prefix}/{uuid.uuid4()}.jsonl"
            self.s3.upload_fileobj(buf, self.bucket, key)
            self.parts.append(key)
            logger.info(f"Successfully uploaded key {key} to S3")

            # add item to dynamodb
            try:
                response = check_if_file_enqueued(self.ddb, key, OCR_JSONL_STATE_NAME)
                if not response:
                    self.ddb.put_item(
                        TableName=OCR_JSONL_STATE_NAME,
                        Item={
                            's3_key': {'S': key},
                            'status': {"S": "enqueued"},
                            'timestamp': {'N': str(int(time.time()))}
                        },
                        ConditionExpression="attribute_not_exists(s3_key)"
                    )
                logger.info(f'Successfully updated key {key} to dynamo db.')
            except Exception as ddb_e:
                logger.warning(f'Failed to update key {key} to dynamo db: {ddb_e}')

            # publish to SQS
            try:
                queue_response = self.sqs.get_queue_url(QueueName=OCR_JSONL_SQS_QUEUE_NAME)
                queue_url = queue_response['QueueUrl']
                response= self.sqs.send_message(
                            QueueUrl=queue_url,
                            MessageBody=json.dumps({'s3_key': key})
                        )
                logger.info(f"Successfully enqueued S3 key {key} to SQS. Message ID: {response['MessageId']}")
            except Exception as sqs_e:
                logger.warning(f"Error sending message to SQS for {key}: {sqs_e}")

            # reset buffer
            self.buffer = {"custom_id": [], "body": []}
            self.current_size = 0
            self.current_page_count = 0

        def close(self):
            """Flush remaining pages before shutdown."""
            self.flush()
            return self.parts

async def process_parquet_file(s3_bucket_uri, s3_key, ddb_client, writer_json):

    # check if the s3_key is in dynamoDB
    # if it is not
        # add the s3_key in dynamodb and set it to "processing"
        # start downloading of parquet file from S3
        # process the parquet file
            # extract rows from the parquet file
            # flush to S3 and SQS if file_size or num_row exceeds the threshold
    if ddb_client is None:
        logger.info("DDB client not provided. creating an DDB client")
        #session = aioboto3.Session()
        #ddb_client = await session.client("dynamodb").__aenter__()
        ddb_client = await get_aboto3_client("dynamodb")

    # First, check DynamoDB: if status == done, skip
    try:
        existing = await _get_dynamo_item(ddb_client, s3_key)
        if existing and existing.get("status", {}).get("S") == "done":
            logger.info("Skipping %s — already done in DynamoDB", s3_key)
            return {"s3_key": s3_key, "status": "skipped", "reason": "already_done"}

        # mark processing (create or set)
        await _init_or_mark_processing(ddb_client, s3_key)

        tmp_parquet = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_parquet.close()
        try:
            logger.info(f"s3_key - {s3_key}")
            logger.info("Downloading s3://%s/%s to %s", s3_bucket_uri, s3_key, tmp_parquet.name)
            await download_s3_to_file(s3_bucket_uri, s3_key, tmp_parquet.name)

            # Use pyarrow to read rows
            doc_id = s3_key
            parquet_file = pq.ParquetFile(tmp_parquet.name)
            row_count = 0

            for batch in parquet_file.iter_batches(batch_size=500):
                for row in batch.to_pylist():
                    row_number = row_count + 1
                    # DynamoDB per-page increment (atomic)
                    try:
                        await _increment_pages_processed(ddb_client, s3_key, row_number)
                        logger.info("DynamoDB pages increment succeeded for %s page %s", s3_key, row_number)
                    except Exception as e:
                        logger.warning("DynamoDB pages increment failed for %s page %s: %s", s3_key, row_number, e)

                    # add page into shared writer
                    s3_bucket = s3_bucket_uri.replace("s3://","") if s3_bucket_uri.startswith("s3://") else s3_bucket_uri
                    writer_json.add_page(
                        doc_id=f'{doc_id}_{row_number}',
                        page_bytes=row['page_bytes'],
                    )
            # set done and total_pages in DynamoDB

            try:
                num_rows = parquet_file.metadata.num_rows
                await _set_done(ddb_client, s3_key, num_rows)
            except Exception as e:
                num_rows = 0
                logger.warning("Failed to set done in DynamoDB for %s: %s", s3_key, e)

            return {"s3_key": s3_key, "status": "success", "pages": num_rows}
        finally:
            try:
                os.unlink(tmp_parquet.name)
            except:
                pass
    except Exception as e:
        logger.exception("Unexpected failure processing %s: %s", s3_key, e)
        try:
            await _mark_failure(ddb_client, s3_key, str(e))
        except Exception as ex:
            logger.exception("Also failed to mark failure in DynamoDB: %s", ex)
        raise

async def process_jsonl_file(s3_bucket_uri, s3_key, s3_client, sqs_client, ddb_client, dlq_url, writer_json ):
    '''
    # check if the s3_key is in dynamoDB
    # if it is not
        # add the s3_key in dynamodb and set it to "processing"
        # start downloading of jsonl file from S3
        # process the jsonl file
            # OCR api call
            # save OCR response to S3
    '''
    session = aioboto3.Session()
    if ddb_client is None :
        ddb_client = await session.client("dynamodb").__aenter__()

    if s3_client is None:
        s3_client = await session.client("s3").__aenter__()

    # First, check DynamoDB: if status == done, skip
    try:
        existing = await _get_dynamo_item(ddb_client, s3_key)
        if existing and existing.get("status", {}).get("S") == "done":
            logger.info("Skipping %s — already done in DynamoDB", s3_key)
            return {"s3_key": s3_key, "status": "skipped", "reason": "already_done"}

        # mark processing (create or set)
        await _init_or_mark_processing(ddb_client, s3_key)

        tmp_jsonl = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        tmp_jsonl.close()

        try:
            logger.info("Downloading s3://%s/%s to %s", s3_bucket_uri.replace("s3://", ""), s3_key, tmp_jsonl.name)
            await download_s3_to_file(s3_bucket_uri, s3_key, tmp_jsonl.name)

            # send file to OCR provider
            response_succeeded, response_failed = await mistral_ocr(tmp_jsonl.name)

            if response_succeeded:
                # add it to S3 and publish to downstream SQS
                key = f'{s3_key.split("/")[1].split(".")[0]}_output.jsonl'
                buf = io.BytesIO()
                buf.write(response_succeeded)
                buf.seek(0)
                s3_client.upload_fileobj(buf, s3_bucket_uri, key)
            
            if response_failed:
                # publish to DLQ
                failed_keys = json.loads(response_failed)
                payload = {
                    "failed_pages": failed_keys,
                    "reason": "ocr_processing_failed"
                }
                body = json.dumps(payload)
                await forward_to_dlq(sqs_client, dlq_url, body)
                print(f"[dlq] published {len(failed_keys)} failed keys → DLQ")
        finally:
            try:
                os.unlink(tmp_jsonl.name)
            except:
                pass
    except Exception as e:
        logger.exception("Unexpected failure processing %s: %s", s3_key, e)
        try:
            await _mark_failure(ddb_client, s3_key, str(e))
        except Exception as ex:
            logger.exception("Also failed to mark failure in DynamoDB: %s", ex)
        raise

async def handle_parquet_message(body: str, message_meta: dict, sqs_client, ddb_client, dlq_url, writer_json):
    """
    body: typically S3 keys/metadata or a small JSON pointing to the parquet file.
    message_meta: metadata such as MessageId, ReceiptHandle etc.
    """
    loop = asyncio.get_running_loop()
    try:
        payload = json.loads(body)
    except:
        payload = {"s3_key": body}
    s3_bucket_uri = PDF_S3_BUCKET
    # SQS message contains a batch of s3_keys. defaults to 1000 keys per message.
    # s3_key value defines the parquet location on s3.
    s3_key = payload.get("s3_keys") or payload.get("s3_key") or payload.get("key") or payload.get("s3Uri") or payload.get("s3_uri") or body

    failed_keys = []
    try:
        logger.info(f'handling parquet message - bucket - {s3_bucket_uri}, s3_key - {s3_key}')
        result = await process_parquet_file(s3_bucket_uri, s3_key, ddb_client, writer_json)
    except Exception as e:
        logger.exception("Processing failed for %s: %s", s3_key, e)
        failed_keys.append(s3_key)
    
    if failed_keys:
        # explicitly publish failures to DLQ
        payload = {
            "failed_keys": failed_keys,
            "reason": "worker_processing_failed"
        }
        body = json.dumps(payload)
        await forward_to_dlq(sqs_client, dlq_url, body)
        print(f"[dlq] published {len(failed_keys)} failed keys → DLQ")
    else:
        # message deletion is handled by the worker
        pass
    return "parquet_processed"

async def handle_jsonl_message(body: str, message_meta: dict, sqs_client, ddb_client, dlq_url, writer_json):
    ''' downloads the JSONL file, 
        makes an call to OCR provider, 
        processes the response from OCR provider, 
        saves to S3, 
        publishes the key for downstream consumers.'''
    loop = asyncio.get_running_loop()
    try:
        payload = json.loads(body)
    except:
        payload = {"s3_keys": body}
    
    # SQS message contains a batch of s3_keys. defaults to 1000 keys per message.
    # s3_key value defines the parquet location on s3.
    s3_key = payload.get("s3_keys") or payload.get("s3_key") or payload.get("key") or payload.get("s3Uri") or payload.get("s3_uri") or body
    s3_bucket_uri = OCR_S3_BUCKET

    failed_keys = []
    try:
        result = await process_jsonl_file(s3_bucket_uri, s3_key,  None, sqs_client, ddb_client, dlq_url, writer_json)
    except Exception as e:
        logger.exception("Processing failed for %s: %s", s3_key, e)
        failed_keys.append(s3_key)
    
    if failed_keys:
        # explicitly publish failures to DLQ
        payload = {
            "failed_keys": failed_keys,
            "reason": "worker_processing_failed"
        }
        body = json.dumps(payload)
        await forward_to_dlq(sqs_client, dlq_url, body)
        print(f"[dlq] published {len(failed_keys)} failed keys → DLQ")
    else:
        # message deletion is handled by the worker
        pass
    return "jsonl_processed"

