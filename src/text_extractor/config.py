# config.py
import os
from dotenv import load_dotenv
from typing import Optional


load_dotenv()

def _env(name, default=None):
    v = os.getenv(name)
    return v if v is not None else default

# Data Files
DATA_FILES = _env("DATA_FILES", "")

# AWS CREDENTIALS
TEST_AWS_ACCESS_KEY_ID=_env("TEST_AWS_ACCESS_KEY_ID", 'test')
TEST_AWS_SECRET_ACCESS_KEY_ID=_env("TEST_AWS_SECRET_ACCESS_KEY_ID", 'test')
TEST_AWS_DDB_ACCESS_KEY_ID=_env("TEST_AWS_DDB_ACCESS_KEY_ID", 'fake')
TEST_AWS_DDB_SECRET_ACCESS_KEY_ID=_env("TEST_AWS_DDB_SECRET_ACCESS_KEY_ID",'fake')
TEST_ENDPOINT_URL=_env("TEST_ENDPOINT_URL", 'http://localhost:4566')

# Mistral Credentials
MISTRAL_API_KEY=_env("MISTRAL_API_KEY", "")

# S3 Buckets
PDF_S3_BUCKET=_env("PDF_S3_BUCKET", "pdf-bucket")
PDF_S3_PARQUET_PART_KEY=_env("PDF_S3_PARQUET_PART_KEY", "parquet")
OCR_S3_BUCKET=_env("OCR_S3_BUCKET", "ocr-bucket")
OCR_S3_JSONL_PART_KEY=_env("OCR_S3_JSONL_PART_KEY", "jsonl")

# SQS URLs
PDF_OCR_PARQUET_SQS_URL = _env("PDF_OCR_PARQUET_SQS_URL")  # parquet messages
OCR_JSONL_SQS_URL = _env("OCR_JSONL_SQS_URL")  # jsonl messages

# Optional manual DLQ urls (if you want worker to forward)
PDF_OCR_PARQUET_DLQ_URL: Optional[str] = _env("PDF_OCR_PARQUET_DLQ_URL")
OCR_JSONL_DLQ_URL: Optional[str] = _env("OCR_JSONL_DLQ_URL")

# Dynamo DB table
# tracks status of PDF files processed by pdf processor module
PDF_FILE_STATE = _env("PDF_FILE_STATE", "pdf_file_state")

# tracks status of parquet files processed by ocr module
OCR_PARQUET_STATE = _env("OCR_PARQUET_STATE", "ocr_parquet_state")

# tracks status of JSONL files processed by OCR module
OCR_JSONL_STATE = _env("OCR_JSONL_STATE", "ocr_jsonl_state")

# tracks status of pages processed by OCR service provider
OCR_PAGE_STATE = _env("OCR_PAGE_STATE", "ocr_page_state")


# Worker tuning
AWS_REGION = _env("AWS_REGION", "us-east-1")
MAX_MESSAGES = int(_env("MAX_MESSAGES", "10"))
WAIT_TIME_SECONDS = int(_env("WAIT_TIME_SECONDS", "20"))  # long poll
VISIBILITY_TIMEOUT = int(_env("VISIBILITY_TIMEOUT", "60"))  # default per-message
VISIBILITY_EXTENSION_MARGIN = int(_env("VISIBILITY_EXTENSION_MARGIN", "10"))
MAX_CONCURRENT_TASKS = int(_env("MAX_CONCURRENT_TASKS", "200"))  # global
MAX_WORKERS_PER_QUEUE = int(_env("MAX_WORKERS_PER_QUEUE", "100"))

# Retry / DLQ policy
MAX_RECEIVE_COUNT = int(_env("MAX_RECEIVE_COUNT", "3"))  # if exceeded -> manual DLQ
RETRY_BACKOFF_BASE = float(_env("RETRY_BACKOFF_BASE", "0.5"))
RETRY_BACKOFF_MAX = float(_env("RETRY_BACKOFF_MAX", "30"))

# Metrics / HTTP health server
METRICS_HOST = _env("METRICS_HOST", "0.0.0.0")
METRICS_PORT = int(_env("METRICS_PORT", "8000"))

# Executor settings
PROCESS_POOL_WORKERS = int(_env("PROCESS_POOL_WORKERS", "4"))
THREAD_POOL_WORKERS = int(_env("THREAD_POOL_WORKERS", "32"))

#OCR JSON Batch settings
JSONL_MAX_CHUNK_SIZE_MB = int(_env("JSONL_MAX_CHUNK_SIZE_MB", "40"))
JSONL_MAX_NUM_PAGES = int(_env("JSONL_MAX_NUM_PAGES", "950"))
