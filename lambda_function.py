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


def get_models() -> dict:
    """ get models data. prod. environment should pull from DB, while dev. just freely fill in fake data """
    # TODO: fetch this data from DB in production environment
    return {
        '0001': {
            "TTL": 172800,  # 2 days
            'name': "sinica_tag",
            'description': '中研院模型',
            'created_at': '2023-02-14 17:46:03',
            'updated_at': '2023-02-14 17:46:03',
            'disabled_at': None  # TODO: consider should we really use null in DB?
        },
        'st': {
            "TTL": 172800,  # 2 days
            'name': "sinica_tag_alt",
            'description': '中研院模型 alternative',
            'created_at': '2023-02-14 17:46:03',
            'updated_at': '2023-02-14 17:46:03',
            'disabled_at': None  # TODO: consider should we really use null in DB?
        }
        #     ......other models
    }


def lambda_handler(event, context):
    # TODO: sanity check for data contents

    bucket, key = extract_s3_params(event)

    csv_file = get_csv_file_binary(bucket, key)

    client, pipeline = set_redis()

    save_to_redis(csv_file, pipeline)

    print_result_info(client)


def print_result_info(client):
    # TODO: helper function to get all the keys from redis. remove when production.
    print(client.keys())
    print("done setting key-values with pipeline")


def save_to_redis(csv_file, pipeline):
    # parse the csv file
    rows = csv.reader(csv_file.decode().splitlines(), delimiter=":")
    for row in rows:
        key, value = row
        # TODO: TTL by different models. it's temp value here.
        pipeline.set(key.strip(), value.strip(), TTL)
    pipeline.execute()


def set_redis():
    """     Connect to redis and set pipeline """
    # TODO: change to redis cluster

    client = Redis(host=REDIS_HOST)
    pipeline = client.pipeline()

    return client, pipeline


def get_csv_file_binary(bucket, key) -> bytes:
    """   download the CSV file from S3 and parse to iterator of lists """
    if IS_DEV:
        # make bytes ourselves, by reading local file directly,
        # skipping event data altogether.
        with open("example_data.csv", "rb") as example_csv_file:
            return example_csv_file.read()

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def extract_s3_params(event):
    """ extract the S3 bucket and object key from the event """
    s3_event_data = event["Records"][0]["s3"]
    bucket = s3_event_data["bucket"]["name"]
    key = s3_event_data["object"]["key"]

    return bucket, key


if __name__ == "__main__":
    import json

    IS_DEV = True

    REDIS_HOST = "127.0.0.1"

    with open("example_request.json") as example_event_file:
        data_dict = json.load(example_event_file)

        lambda_handler(data_dict, None)
