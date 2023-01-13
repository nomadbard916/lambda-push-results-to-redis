import csv
import os
import redis

# environment variables
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")


def lambda_handler(event, context):
    # extract the S3 bucket and object key from the event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # TODO: pipeline
    # Connect to redis
    redis_client = redis.Redis(
        host="your_redis_host", port=6379, password=REDIS_PASSWORD
    )

    # download the CSV file from S3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_file = obj["Body"].read()

    # parse the CSV file
    rows = csv.DictReader(csv_file.decode().splitlines())
    for row in rows:
        # set the Redis key-value
        redis_client.set(row["key"], row["value"])
