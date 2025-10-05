# text-extractor

The **text-extractor** is a pivotal component in the *Unstruct AI Modular Data Pipeline*, responsible for converting raw extracted text (from PDFs, DOCX, OCR output, etc.) into cleaned, tokenized, and enriched text suitable for embedding and semantic search.

This repository is designed for modular, containerized deployment, includes **AWS service integrations (S3, SQS, DynamoDB)**, and comes with **observability** via Prometheus + Grafana support.

---

## 🧩 System Architecture Overview

The `text-extractor` sits between **pdf-processor** / **document ingestion** and the **embeddings/search** systems:

| Component         | Repository                    | Responsibility                                        |
|-------------------|-------------------------------|--------------------------------------------------------|
| File Loader        | `file-loader`                | Ingests raw files and queues tasks                    |
| PDF Processor      | `pdf-processor`              | Converts PDFs into raw text + metadata                |
| **Text Extractor** | `text-extractor`             | Cleans and enriches raw text, outputs text artifacts   |
| Embeddings         | `embeddings`                  | Generates vector embeddings                           |
| Search             | `search`                       | Indexes embeddings + metadata into OpenSearch          |
| Infra (Terraform)  | `infra`                        | Manages shared AWS infra (VPC, ECS, Redis, etc.)      |

The **text-extractor** receives messages via **SQS**, fetches raw text artifacts from **S3**, processes (cleanup, normalization, tokenization, filtering, etc.), and writes the enriched text and metadata back to **S3** or forwards events via **SQS**.

---

## ⚙️ Core Responsibilities

- Poll an **input SQS queue** for new text extraction jobs  
- Fetch raw text or OCR output from **S3**  
- Perform cleanup, normalization, tokenization, filtering, entity expansion, etc.  
- Compile structured metadata and enriched text JSON  
- Upload output artifacts to **S3**  
- Update **DynamoDB** (or metadata store) with job status and statistics  
- Emit downstream **SQS** events for further pipeline stages  
- Expose Prometheus metrics at `/metrics`

---

## 🏗️ Repository Structure

```
text-extractor/
├─ src/text_extractor/         # Core Python package
│  ├─ main.py                  # Worker entry point
│  ├─ extractor.py             # Core text processing logic
│  ├─ aws_client.py            # Helpers for S3, SQS, DynamoDB
│  ├─ metrics.py               # Prometheus metrics definitions
│  └─ __init__.py
├─ Dockerfile.dev              # Dev Docker image
├─ Dockerfile.prod             # Prod Docker image
├─ docker-compose.yml          # LocalStack + service + monitoring stack
├─ prometheus.yml              # Prometheus scrape config
├─ requirements.txt            # Python dependencies
├─ pyproject.toml              # Build / metadata config
├─ localstack_data/            # Persisted LocalStack data
├─ grafana_data/               # Grafana dashboard storage
├─ LICENSE                     # Apache License 2.0
└─ README.md                   # Project documentation
```

---

## 🚀 Quickstart

### 1️⃣ Prerequisites

- Python 3.10+  
- Docker & Docker Compose  
- (Optional) AWS CLI / LocalStack CLI for local testing

### 2️⃣ Clone repository

```bash
git clone https://github.com/mbellary/text-extractor.git
cd text-extractor
```

### 3️⃣ Start via Docker Compose

```bash
docker compose up --build
```

This spins up:

- `text-extractor` worker  
- `localstack` (mock AWS services)  
- `prometheus` (monitoring)  
- `grafana` (dashboards)  

> Prometheus → http://localhost:9090  
> Grafana → http://localhost:3000  
> LocalStack → http://localhost:4566

---

## 🧠 Local Development

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m text_extractor
```

---

## ⚙️ Configuration

Create a `.env` with:

```env
# AWS / LocalStack
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-1
LOCALSTACK_ENDPOINT=http://localstack:4566

# S3 / SQS / DynamoDB
S3_INPUT_BUCKET=unstruct-processed-bucket
S3_OUTPUT_BUCKET=unstruct-extracted-bucket
SQS_INPUT_QUEUE=unstruct-processed-events
SQS_OUTPUT_QUEUE=unstruct-extraction-events
DYNAMODB_TABLE=unstruct-extraction-metadata

# Processing
CLEANUP_ENABLED=True
ENTITY_EXPANSION=True
BATCH_SIZE=20

# Monitoring
PROMETHEUS_PORT=9093
LOG_LEVEL=INFO
```

To create required resources in LocalStack:

```bash
docker exec -it localstack awslocal s3 mb s3://unstruct-processed-bucket
docker exec -it localstack awslocal s3 mb s3://unstruct-extracted-bucket
docker exec -it localstack awslocal sqs create-queue --queue-name unstruct-processed-events
docker exec -it localstack awslocal sqs create-queue --queue-name unstruct-extraction-events
```

---

## 📦 Example Flow

1️⃣ Upstream sends message after `pdf-processor` completion:

```json
{
  "bucket": "unstruct-processed-bucket",
  "key": "text/sample.txt",
  "job_id": "abc123"
}
```

2️⃣ `text-extractor` fetches and processes this input, producing:

```
s3://unstruct-extracted-bucket/cleaned/sample_clean.txt
s3://unstruct-extracted-bucket/meta/sample_meta.json
```

3️⃣ It publishes a downstream SQS message:

```json
{
  "bucket": "unstruct-extracted-bucket",
  "key": "cleaned/sample_clean.txt",
  "status": "extracted"
}
```

---

## 📊 Observability and Metrics

- Prometheus scrapes `/metrics` endpoint of the service  
- Grafana dashboards (persisted in `grafana_data/`) visualize key metrics such as:  
  - `text_extraction_jobs_total`  
  - `text_extraction_duration_seconds`  
  - `sqs_messages_consumed_total`  
  - `s3_upload_failures_total`

---

## 🧪 Testing

```bash
pytest -q
ruff check src
black src
```

---

## 🚀 Deployment

In production, `text-extractor` runs on **AWS ECS Fargate** via your `infra` Terraform stack.  
It is configured with:

- IAM permissions (S3, SQS, DynamoDB)  
- Metrics scraped by Prometheus via ECS service discovery  
- Deployment automated by CI/CD pipelines (e.g. GitHub Actions)

---

## 🧭 Roadmap

- [ ] Add support for multilingual cleanup and normalization  
- [ ] Integrate named entity recognition (NER) during extraction  
- [ ] Add retry and dead-letter handling for failed jobs  
- [ ] Provide example Grafana dashboard JSONs  
- [ ] Scale horizontally with batch processing support

---


## 📜 License

Licensed under the [Apache License 2.0](./LICENSE).

---

## 🧾 Author

**Mohammed Ali**  
🌐 [https://github.com/mbellary](https://github.com/mbellary)

---

> _Part of the **Unstruct Modular Data Pipeline** — connecting ingestion, processing, extraction, embedding, and search._
