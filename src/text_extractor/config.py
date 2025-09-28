import os
from dotenv import load_dotenv, dotenv_values
from text_extractor.logger import get_logger
from pathlib import Path

logger = get_logger("enqueue_worker.config")

PACKAGE_DIR = Path(__file__).resolve().parent


APP_ENV = os.getenv("APP_ENV", "production").lower()


if APP_ENV == "localstack":
    load_dotenv(PACKAGE_DIR / ".env.localstack")
    logger.info('Loaded localstack environment variables')
else:
    load_dotenv(PACKAGE_DIR / ".env.production")
    logger.info('Loaded production environment variables')


def _env(name, default=None):
    v = os.getenv(name)
    return v if v is not None else default

#aws region
AWS_REGION = _env("AWS_REGION", "ap-south-1")

# AWS CREDENTIALS
AWS_ACCESS_KEY_ID=_env("AWS_ACCESS_KEY_ID", None) #if use_localstack else None
AWS_SECRET_ACCESS_KEY=_env("AWS_SECRET_ACCESS_KEY", None) #if use_localstack else None
LOCALSTACK_URL=_env("LOCALSTACK_URL", None) #if use_localstack else None


# Data Files
#DATA_FILES = _env("DATA_FILES", "")

# Mistral Credentials
MISTRAL_API_KEY=_env("MISTRAL_API_KEY", "")

# S3 Buckets
PDF_S3_BUCKET=_env("PDF_S3_BUCKET", "rag-pdf-s3-bucket")
PDF_S3_PARQUET_PART_KEY=_env("PDF_S3_PARQUET_PART_KEY", "parquet")
OCR_S3_BUCKET=_env("OCR_S3_BUCKET", "rag-ocr-s3-bucket")
OCR_S3_JSONL_PART_KEY=_env("OCR_S3_JSONL_PART_KEY", "jsonl")

# SQS URLs
# PDF_OCR_PARQUET_SQS_URL = _env("PDF_OCR_PARQUET_SQS_URL")  # parquet messages
# OCR_JSONL_SQS_URL = _env("OCR_JSONL_SQS_URL")  # jsonl messages
#
# # Optional manual DLQ urls (if you want worker to forward)
# PDF_OCR_PARQUET_DLQ_URL: Optional[str] = _env("PDF_OCR_PARQUET_DLQ_URL")
# OCR_JSONL_DLQ_URL: Optional[str] = _env("OCR_JSONL_DLQ_URL")

PDF_OCR_PARQUET_SQS_QUEUE_NAME = _env("PDF_OCR_PARQUET_SQS_QUEUE_NAME", "parquet-queue")
PDF_OCR_PARQUET_DLQ_QUEUE_NAME = _env("PDF_OCR_PARQUET_DLQ_QUEUE_NAME", "parquet-dlq")
OCR_JSONL_SQS_QUEUE_NAME = _env("OCR_JSONL_SQS_QUEUE_NAME", "jsonl-queue")
OCR_JSONL_DLQ_QUEUE_NAME = _env("OCR_JSONL_DLQ_QUEUE_NAME", "jsonl-dlq")
OCR_OUTPUT_JSONL_SQS_QUEUE_NAME= _env("OCR_OUTPUT_JSONL_SQS_QUEUE_NAME", "ocr-jsonl-queue")  #output of OCR provider is published here
OCR_OUTPUT_JSONL_DLQ_QUEUE_NAME= _env("OCR_OUTPUT_JSONL_DLQ_QUEUE_NAME", "ocr-jsonl-dlq")

# Dynamo DB table
PDF_FILE_STATE_NAME = _env("PDF_FILE_STATE_NAME", "pdf-processing-state")

# tracks status of parquet files processed by ocr module
OCR_PARQUET_STATE_NAME = _env("OCR_PARQUET_STATE_NAME", "ocr_parquet_state")

# tracks status of JSONL files processed by OCR module
OCR_JSONL_STATE_NAME = _env("OCR_JSONL_STATE_NAME", "ocr_jsonl_state")

# tracks status of pages processed by OCR service provider
OCR_PAGE_STATE_NAME = _env("OCR_PAGE_STATE_NAME", "ocr_page_state")

# tracks status of pages processed by embedding service provider
EMBEDDER_PAGE_STATE_NAME = _env("EMBEDDER_PAGE_STATE_NAME", "embedder_page_state")

# Worker tuning
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

# GCP
GCP_REGION=_env("GCP_REGION" , "us-central1")
GCP_PROJECT=_env("GCP_PROJECT", "mistral-ocr-gcp")
GCP_OCR_MODEL=_env("GCP_OCR_MODEL", "mistral-ocr-2505")