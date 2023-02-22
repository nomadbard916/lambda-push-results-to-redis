# lambda environment already includes boto3. we install it only to allow for importing at local execution.
import boto3
import csv
import os
from redis import Redis
import logging
import argparse
from datetime import datetime
import re

# environment variables
REDIS_HOST = os.environ.get("REDIS_HOST")

# global setup
IS_DEV = False
logging.basicConfig(level=logging.INFO)

TTL = 86400
DATE_HOUR_FORMAT = "%Y-%m-%d-%H"
NOW = datetime.now()
CURRENT_DATE_HOUR = NOW.strftime(DATE_HOUR_FORMAT)


def lambda_handler(event, context):
    parse_cli_args()

    bucket, file_key = extract_s3_params(event)

    uploaded_time = get_uploaded_date_hour(file_key)

    client, pipeline = set_redis()

    csv_file_binary = get_csv_file_binary(bucket, file_key)

    save_to_redis(csv_file_binary, uploaded_time, get_models(), pipeline)

    print_result_info(client)


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--upload_time",
        "-u",
        help="set upload time manually by developer, for testing purpose",
    )

    return parser.parse_args()


def extract_s3_params(event):
    """extract the S3 bucket and object key from the event"""

    s3_event_data = event["Records"][0]["s3"]
    bucket = s3_event_data["bucket"]["name"]
    key = s3_event_data["object"]["key"]

    return bucket, key


def get_uploaded_date_hour(s3_file_key) -> str:
    """ get date_hour by environment and/or argument. format: %Y-%m-%d-%H """
    regex_date_hour_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}$")

    # TODO: sanity check for date_hour format
    if IS_DEV:
        cli_args = parse_cli_args()
        upload_time_param = cli_args.upload_time
        if upload_time_param and regex_date_hour_pattern.match(upload_time_param):
            return upload_time_param
        return CURRENT_DATE_HOUR

    date_hour = s3_file_key.split("/")[0]

    return date_hour if regex_date_hour_pattern.match(date_hour) else CURRENT_DATE_HOUR


def set_redis():
    """Connect to redis and set pipeline"""
    # TODO: change to redis cluster

    client = Redis(host=REDIS_HOST)
    pipeline = client.pipeline()

    return client, pipeline


def get_csv_file_binary(bucket, file_key) -> bytes:
    """download the CSV file from S3 and parse to iterator of lists"""
    if IS_DEV:
        # make bytes ourselves, by reading local file directly,
        # skipping event data altogether.
        with open("example_data.csv", "rb") as example_csv_file:
            return example_csv_file.read()

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=file_key)
    return obj["Body"].read()


def get_models() -> dict:
    """get models data. prod. environment should pull from DB, while dev. just freely fill in fake data"""
    # TODO: fetch this data from DB in production environment
    two_days = 172800

    return {
        "0001": {
            "TTL": two_days,  # 2 days
            "name": "sinica_tag",
            "description": "中研院模型",
            "created_at": "2023-02-14 17:46:03",
            "updated_at": "2023-02-14 17:46:03",
            "disabled_at": None,  # TODO: consider should we really use null in DB?
        },
        "st": {
            "TTL": two_days,  # 2 days
            "name": "sinica_tag_alt",
            "description": "中研院模型 alternative",
            "created_at": "2023-02-14 17:46:03",
            "updated_at": "2023-02-14 17:46:03",
            "disabled_at": None,  # TODO: consider should we really use null in DB?
        }
        #     ......other models
    }


def get_model_ttl(src_models, model_id) -> int:
    return src_models[model_id]["TTL"]


def get_date_hour_diff(later_time: str, earlier_time: str) -> int:
    time_delta = datetime.strptime(later_time, DATE_HOUR_FORMAT) - datetime.strptime(
        earlier_time, DATE_HOUR_FORMAT
    )

    seconds_float = time_delta.total_seconds()

    return int(seconds_float)


def get_real_ttl(model_ttl: int, uploaded_time: str) -> int:
    date_hour_diff = get_date_hour_diff(CURRENT_DATE_HOUR, uploaded_time)

    possible_ttl = model_ttl - date_hour_diff

    return possible_ttl if possible_ttl > 0 else model_ttl


def save_to_redis(csv_file_binary, uploaded_time, src_models, pipeline):
    # TODO: sanity check for data contents

    # parse the csv file from binary
    rows = csv.reader(
        csv_file_binary.decode(encoding="utf-8-sig").splitlines(), delimiter=":"
    )
    for row in rows:
        raw_key, raw_value = row
        key, value = raw_key.strip(), raw_value.strip()

        model_id = key.split("-")[-1]

        model_ttl = get_model_ttl(src_models, model_id)

        real_ttl = get_real_ttl(model_ttl, uploaded_time)

        pipeline.set(key.strip(), value.strip(), real_ttl)

    pipeline.execute()


def print_result_info(client):
    # TODO: helper function to get all the keys from redis. remove when production.
    print(client.keys())
    print("done setting key-values with pipeline")


if __name__ == "__main__":
    import json

    IS_DEV = True

    REDIS_HOST = "127.0.0.1"

    with open("example_request.json") as example_event_file:
        data_dict = json.load(example_event_file)

        lambda_handler(data_dict, None)
