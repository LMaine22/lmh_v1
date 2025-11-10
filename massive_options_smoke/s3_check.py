import os
from dotenv import load_dotenv
import boto3
from botocore.config import Config

load_dotenv()

ak = os.environ["MASSIVE_S3_ACCESS_KEY"].strip()
sk = os.environ["MASSIVE_S3_SECRET_KEY"].strip()

s3 = boto3.client(
    "s3",
    endpoint_url="https://files.massive.com",
    aws_access_key_id=ak,
    aws_secret_access_key=sk,
    config=Config(signature_version="s3v4"),
)

# Choose a prefix that exists for your plan; here are common ones:
#  - 'us_options_opra'     (options)
#  - 'us_stocks_sip'       (stocks)
prefix = "us_options_opra"

resp = s3.list_objects_v2(Bucket="flatfiles", Prefix=prefix, MaxKeys=20)

if "Contents" not in resp:
    raise RuntimeError(f"No objects returned under prefix '{prefix}'. Check plan and S3 keys.")

print(f"First {len(resp['Contents'])} objects under {prefix}:")
for obj in resp["Contents"]:
    print(" -", obj["Key"])

print("S3 Flat Files connectivity OK.")

