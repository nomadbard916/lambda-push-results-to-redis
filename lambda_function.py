# lambda includes it. we install it only to allow for importing at local execution.
import boto3
import csv
import os
from redis import Redis
import logging

# environment variables
REDIS_HOST = os.environ.get("REDIS_HOST")

# global setup
IS_DEV = False
logging.basicConfig(level=logging.INFO)
TTL = 86400


def lambda_handler(event, context):
    # TODO: sanity check for data contents

    # extract the S3 bucket and object key from the event
    s3_event_data = event["Records"][0]["s3"]
    bucket = s3_event_data["bucket"]["name"]
    key = s3_event_data["object"]["key"]

    # download the CSV file from S3 and parse to iterator of lists
    if IS_DEV:
        # make bytes ourselves, by reading local file directly,
        # skipping event data altogether.
        with open("example_data.csv", "rb") as example_csv_file:
            csv_file = example_csv_file.read()
    else:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        csv_file = obj["Body"].read()

    # TODO: change to redis cluster
    # Connect to redis and set pipeline
    client = Redis(host=REDIS_HOST)
    pipeline = client.pipeline()

    # parse the csv file
    rows = csv.reader(csv_file.decode().splitlines(), delimiter=":")
    for row in rows:
        key, value = row
        # TODO: TTL by different models. it's temp value here.
        pipeline.set(key.strip(), value.strip(), TTL)

    pipeline.execute()

    # TODO: helper function to get the value from redis, remove when production.
    print(client.keys())
    print("done setting key-values with pipeline")


if __name__ == "__main__":
    import json

    IS_DEV = True

    REDIS_HOST = "127.0.0.1"

    with open("example_request.json") as example_event_file:
        data_dict = json.load(example_event_file)

        lambda_handler(data_dict, None)
