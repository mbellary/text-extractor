# sqs_worker.py
import asyncio
import os
import signal
import sys

import aioboto3
import time
from typing import Callable, Dict, List

from text_extractor.config import (
    AWS_REGION, MAX_MESSAGES, WAIT_TIME_SECONDS, VISIBILITY_TIMEOUT,
    VISIBILITY_EXTENSION_MARGIN, MAX_CONCURRENT_TASKS, MAX_RECEIVE_COUNT, PARQUET_SQS_URL, PARQUET_SQS_DLQ_URL,
    JSONL_SQS_URL, JSONL_SQS_DLQ_URL, TEST_AWS_DDB_ACCESS_KEY_ID, TEST_AWS_DDB_SECRET_ACCESS_KEY_ID, OCR_S3_BUCKET,
    OCR_S3_JSONL_PART_KEY, TEST_AWS_SECRET_ACCESS_KEY_ID, TEST_AWS_ACCESS_KEY_ID, TEST_ENDPOINT_URL
)
from text_extractor.logger import get_logger
from text_extractor.metrics_server import start_metrics_server, MSG_PROCESSED, MSG_FAILED, MSG_RECEIVED, IN_FLIGHT, PROC_TIME
from text_extractor.processor import handle_parquet_message, handle_jsonl_message, JSONLBatchWriter

from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger("sqs_worker")

# global semaphore to limit concurrent message processing
processing_sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# health / shutdown flags
shutdown_event = asyncio.Event()

async def visibility_extender(sqs_client, queue_url, receipt_handle, stop_event: asyncio.Event, interval: int):
    """
    Periodically extend the visibility timeout while processing.
    This coroutine runs in background until stop_event is set.
    """
    logger.debug("starting visibility extender")
    try:
        while not stop_event.is_set():
            await asyncio.sleep(interval)
            if stop_event.is_set():
                break
            try:
                await sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
                logger.debug("extended visibility timeout")
            except Exception as e:
                logger.exception("failed to extend visibility timeout")
                # continue; we'll likely fail the processing if we can't extend repeatedly
    finally:
        logger.debug("stopped visibility extender")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=10), reraise=True)
async def delete_messages_batch(sqs_client, queue_url: str, entries: List[Dict]):
    """Delete messages in batch with retry."""
    if not entries:
        return
    resp = await sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = resp.get("Failed", [])
    if failed:
        logger.warning("some deletions failed, will retry", extra={"failed": failed})
        raise RuntimeError("delete_message_batch partial failure")

async def forward_to_dlq(sqs_client, dlq_url: str, body: str, attributes: dict):
    """Optionally forward message body or the failed S3 keys to DLQ manually """
    if not dlq_url:
        logger.warning("DLQ URL not configured; skipping forward", extra={"body": body})
        return
    await sqs_client.send_message(QueueUrl=dlq_url, MessageBody=body, MessageAttributes=attributes or {})

async def process_single_message(sqs_client, queue_url: str, msg: dict, handler: Callable, dlq_url: str, ddb_client, writer_json):
    """
    Process a single message with:
      - visibility extension loop
      - retry logic
      - send to DLQ if receive count exceed threshold
    """
    receipt = msg["ReceiptHandle"]
    mid = msg["MessageId"]
    body = msg.get("Body", "")
    attributes = msg.get("MessageAttributes", {})
    # SQS returns message attribute ApproximateReceiveCount as a string in msg['Attributes'] if requested
    receive_count = int(msg.get("Attributes", {}).get("ApproximateReceiveCount", "1"))

    logger.info(f"start processing message with {body}", extra={"MessageId": mid, "ReceiveCount": receive_count})
    MSG_RECEIVED.labels(queue=queue_url).inc()
    IN_FLIGHT.inc()
    start_ts = time.time()

    stop_extender = asyncio.Event()
    extender_task = asyncio.create_task(
        visibility_extender(sqs_client, queue_url, receipt, stop_extender, VISIBILITY_TIMEOUT - VISIBILITY_EXTENSION_MARGIN)
    )

    # process each message. if successful, return "processed" else forward to DLQ
    try:
        # If receive_count > MAX_RECEIVE_COUNT, forward to DLQ immediately (prevent cycles)
        if receive_count >= MAX_RECEIVE_COUNT:
            logger.warning("receive_count exceeded threshold, forwarding to DLQ", extra={"MessageId": mid, "ReceiveCount": receive_count})
            await forward_to_dlq(sqs_client, dlq_url, body, attributes)
            MSG_FAILED.labels(queue=queue_url).inc()
            return "forwarded_to_dlq"

        # Use tenacity-style retry for the processing step (transient failures)
        @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=0.5, max=10), reraise=True)
        async def _try_process():
            return await handler(body, {"MessageId": mid}, sqs_client, ddb_client, dlq_url, writer_json)

        result = await _try_process()
        elapsed = time.time() - start_ts
        PROC_TIME.labels(queue=queue_url).observe(elapsed)
        MSG_PROCESSED.labels(queue=queue_url).inc()
        logger.info("message processed successfully", extra={"MessageId": mid, "elapsed": elapsed, "result": result})
        return "processed"
    except Exception as e:
        logger.exception("processing failed", extra={"MessageId": mid})
        MSG_FAILED.labels(queue=queue_url).inc()
        # forward to DLQ if configured
        try:
            await forward_to_dlq(sqs_client, dlq_url, body, attributes)
            return "forwarded_to_dlq"
        except Exception:
            logger.exception("failed to forward to DLQ")
        raise
    finally:
        stop_extender.set()
        try:
            await extender_task
        except Exception:
            pass
        IN_FLIGHT.dec()

async def poller_task(queue_url: str, handler: Callable, dlq_url: str, session, writer_json):
    """
    Continuously polls a queue and submits processing coroutines to worker pool.
    """
    logger.info("starting poller", extra={"queue_url": queue_url})
    async with (session.client("sqs", region_name=AWS_REGION, aws_access_key_id=TEST_AWS_ACCESS_KEY_ID, aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY_ID, endpoint_url=TEST_ENDPOINT_URL) as sqs_client,
                session.client("dynamodb", region_name=AWS_REGION, aws_access_key_id=TEST_AWS_DDB_ACCESS_KEY_ID, aws_secret_access_key=TEST_AWS_DDB_SECRET_ACCESS_KEY_ID, endpoint_url=TEST_ENDPOINT_URL) as ddb_client):
        while not shutdown_event.is_set():
            try:
                resp = await sqs_client.receive_message(
                    QueueUrl=queue_url,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=MAX_MESSAGES,
                    WaitTimeSeconds=WAIT_TIME_SECONDS,
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                )
                messages = resp.get("Messages", [])
                if not messages:
                    # no messages this poll
                    continue

                # process messages concurrently with concurrency limit
                tasks = []
                entries_to_delete = []
                for msg in messages:
                    # Acquire a slot for processing
                    await processing_sem.acquire()

                    async def _process_and_release(m=msg):
                        try:
                            outcome = await process_single_message(sqs_client, queue_url, m, handler, dlq_url, ddb_client, writer_json)
                            # if processed or forwarded, schedule delete
                            if outcome in ("processed", "forwarded_to_dlq"):
                                entries_to_delete.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})
                        except Exception:
                            # failed; we do not delete => message returns to queue / redrive policy / manual DLQ already attempted
                            pass
                        finally:
                            processing_sem.release()

                    tasks.append(asyncio.create_task(_process_and_release()))

                # Wait for tasks to finish or let them run? To keep steady throughput, we wait for at least all tasks created this batch
                # but don't let exceptions propagate out to crash the poller
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                # Delete processed messages in batch
                if entries_to_delete:
                    # batch deletes may be >10 entries; handle in chunks
                    chunk_size = 10
                    for i in range(0, len(entries_to_delete), chunk_size):
                        chunk = entries_to_delete[i:i + chunk_size]
                        try:
                            await delete_messages_batch(sqs_client, queue_url, chunk)
                        except Exception:
                            logger.exception("delete batch failed, entries will become visible again; monitor for duplicates")
            except Exception as e:
                logger.exception("poller loop exception, backing off")
                # exponential backoff
                await asyncio.sleep(1)

    logger.info("poller stopping", extra={"queue_url": queue_url})

def _install_signal_handlers(loop):
    for sig in (signal.SIGTERM, signal.SIGINT):
        # add signal handler to the event loop
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

async def shutdown():
    logger.info("shutdown requested")
    shutdown_event.set()
    # Wait a little for inflight to drain
    for _ in range(30):
        if IN_FLIGHT._value == 0:
            break
        logger.info("waiting for in-flight messages to finish", extra={"in_flight": IN_FLIGHT._value})
        await asyncio.sleep(1)
    logger.info("shutdown complete")

async def run():
    # start metrics server
    start_metrics_server(host=os.environ.get("METRICS_HOST", "0.0.0.0"), port=int(os.environ.get("METRICS_PORT", "8000")))
    logger.info("metrics server started", extra={"host": os.environ.get("METRICS_HOST"), "port": os.environ.get("METRICS_PORT")})

    # create aioboto3 session
    session = aioboto3.Session()
    loop = asyncio.get_running_loop()
    #_install_signal_handlers(loop)

    # JSON writer
    writer_json = JSONLBatchWriter(OCR_S3_BUCKET, OCR_S3_JSONL_PART_KEY)

    pollers = []
    # poll parquet_sqs_queue and publish failures to parquet_dqs_queue
    pollers.append(asyncio.create_task(poller_task(PARQUET_SQS_URL, handle_parquet_message, PARQUET_SQS_DLQ_URL, session, writer_json)))

    #poll json_sqs_queue and publish failures to json dqs
    pollers.append(asyncio.create_task(poller_task(JSONL_SQS_URL, handle_jsonl_message, JSONL_SQS_DLQ_URL, session, writer_json)))

    # Wait until shutdown is requested
    await shutdown_event.wait()

    # Cancel pollers and wait
    for p in pollers:
        p.cancel()
    await asyncio.gather(*pollers, return_exceptions=True)
    logger.info("worker exiting")


def main():
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(run())
    except KeyboardInterrupt:
        pass

# if __name__ == "__main__":
#     try:
#         if sys.platform == 'win32':
#             asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         pass
