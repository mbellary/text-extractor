import json

import boto3
import time
from botocore.exceptions import ClientError
from config import (
    PDF_S3_BUCKET,
    AWS_REGION,
    OCR_PARQUET_STATE_NAME,
    PDF_OCR_PARQUET_SQS_QUEUE_NAME,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    LOCALSTACK_URL)

'''
This script simulates the pdf_processor modules "processor" function.
 After the file has been successfully processed, the following ops are performed:
    - file is uploaded to S3
    - file is added to OCR_PARQUET_STATE table with status set to "enqueued".
    - file is published to PARQUET_SQS_URL
'''


S3_ENDPOINT = "http://localhost:4566"

LOCAL_FILES = [
    ('C:\\Users\\bmoha\\Work\\projects\\AWS\\text-extractor\\data\\invoice_1.parquet', "parquets/invoice_1.parquet"),
    ('C:\\Users\\bmoha\\Work\\projects\\AWS\\text-extractor\\data\\oracle_10k_2014_q1_small.parquet',
     "parquets/oracle_10k_2014_q1_small.parquet")]



def check_if_file_enqueued(ddb_client, s3_key):
    """
    check if Parquet file is enqueued or being processed
    """
    try:
        response = ddb_client.get_item(
            TableName=OCR_PARQUET_STATE_NAME,
            Key={'s3_key': {'S': s3_key}}
        )
        item = response.get('Item')
        if item:
            status = item.get('status', {}).get('S', 'not_found')
            if status in ['enqueued', 'processing']:
                print(f"File {s3_key} is already enqueued or being processed. Skipping.")
                return True
        return False
    except ClientError as e:
        print(f"Error checking item in DynamoDB: {e}")
        return False

def main():
    s3_client = boto3.client("s3",
        region_name = AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=LOCALSTACK_URL)

    ddb_client = boto3.client("dynamodb",
        region_name = AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=LOCALSTACK_URL)

    sqs_client = boto3.client("sqs",
        region_name = AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=LOCALSTACK_URL)

    s3_bucket = PDF_S3_BUCKET
    for local_file, key in LOCAL_FILES:
        s3_key = key

        try: # upload to S3
            s3_client.upload_file(local_file, s3_bucket, s3_key)
            print(f'Successfully uploaded key {s3_key} to S3.')
        except Exception as e:
            print(f'Failed to upload file {local_file} to s3')

        try: # update Dynamo DB
            response = check_if_file_enqueued(ddb_client, s3_key)
            if not response:
                ddb_client.put_item(
                    TableName=OCR_PARQUET_STATE_NAME,
                    Item={
                        's3_key': {'S': s3_key},
                        'status': {"S" : "enqueued"},
                        'timestamp': {'N': str(int(time.time()))}
                    },
                    ConditionExpression="attribute_not_exists(s3_key)"
                )
            print(f'Successfully updated key {s3_key} to dynamo db.')
        except Exception as ddb_e:
            print(f'Failed to update key {s3_key} to dynamo db: {ddb_e}')

        try:
            queue_response = sqs_client.get_queue_url(QueueName=PDF_OCR_PARQUET_SQS_QUEUE_NAME)
            queue_url = queue_response['QueueUrl']
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({'s3_key': s3_key})
            )
            print(f"Successfully enqueued S3 key {s3_key} to SQS. Message ID: {response['MessageId']}")

        except Exception as sqs_e:
            print(f"Error sending message to SQS for {s3_key}: {sqs_e}")


if __name__ == "__main__":
    main()