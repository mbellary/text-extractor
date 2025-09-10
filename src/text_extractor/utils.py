import aioboto3
from botocore.exceptions import ClientError

from text_extractor.config import AWS_REGION, TEST_AWS_SECRET_ACCESS_KEY_ID, TEST_AWS_ACCESS_KEY_ID, TEST_ENDPOINT_URL


async def download_s3_to_file(s3_bucket_uri: str, s3_key: str, local_path: str):
    sess = aioboto3.Session(region_name=AWS_REGION)
    async with sess.client('s3', region_name=AWS_REGION, aws_access_key_id=TEST_AWS_ACCESS_KEY_ID, aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY_ID, endpoint_url=TEST_ENDPOINT_URL) as s3:
        await s3.download_file(Bucket=s3_bucket_uri.replace("s3://","") if s3_bucket_uri.startswith("s3://") else s3_bucket_uri, Key=s3_key, Filename=local_path)


def check_if_file_enqueued(ddb_client, s3_key, table_name):
    """
    check if Parquet file is enqueued or being processed
    """
    try:
        response = ddb_client.get_item(
            TableName=table_name,
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

