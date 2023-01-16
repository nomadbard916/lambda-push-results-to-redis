def lambda_handler(event, context):
    # * put setup here so they won't interfere with local functions
    # there's no need to install manually. lambda includes it.
    import boto3

    import csv
    import os
    import redis

    # environment variables
    REDIS_HOST = os.environ.get("REDIS_HOST")
    REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

    # extract the S3 bucket and object key from the event
    s3_event_data = event["Records"][0]["s3"]
    bucket = s3_event_data["bucket"]["name"]
    key = s3_event_data["object"]["key"]

    # TODO: sanity check for contents with "if" and "return"

    # TODO: cluster
    # Connect to redis and set pipeline
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD)
    redis_pipeline = redis_client.pipeline()

    # download the CSV file from S3 and parse to iterator of lists
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    csv_file = obj["Body"].read()

    rows = csv.reader(csv_file.decode(), delimiter=":")
    for row in rows:
        key, value = row
        redis_pipeline.set(key, value, 3600)

    # old example
    # parse the CSV file
    # rows = csv.reader(csv_file.decode())
    # for row in rows:
    #     # set the Redis key-value
    #     # TODO: TTL by different models. it's temp here.

    #     redis_pipeline.set(row["key"], row["value"], 3600)

    redis_pipeline.execute()


if __name__ == "__main__":
    print("FQ")
