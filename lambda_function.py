# lambda includes it. we install it only to allow for importing at local execution.
import boto3
import csv
import os
import redis

# environment variables
IS_DEV = False

REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")


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

    # TODO: redis cluster
    # Connect to redis and set pipeline
    client = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD)
    pipeline = client.pipeline()

    # parse the csv file
    rows = csv.reader(csv_file.decode().splitlines(), delimiter=":")
    for row in rows:
        key, value = row
        # TODO: TTL by different models. it's temp here.
        pipeline.set(key.strip(), value.strip(), 3600)

    pipeline.execute()

    print("done setting key-values with pipeline")


if __name__ == "__main__":
    import json

    IS_DEV = True

    REDIS_HOST = "127.0.0.1"
    REDIS_PASSWORD = ""

    with open("example_request.json") as example_event_file:
        data_dict = json.load(example_event_file)

        lambda_handler(data_dict, None)
