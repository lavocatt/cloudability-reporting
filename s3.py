from datetime import datetime as dt
import boto3
import argparse
import os


def add_s3_necesseary_args(parser: argparse.ArgumentParser):
    parser.add_argument("--bucket-name",
                        type=str,
                        help="S3 bucket name to upload to, implies passing credentials"
                        )
    parser.add_argument("--aws-access-key-id",
                        type=str,
                        help="aws access key id, required to upload"
                        )
    parser.add_argument("--aws-secret-access-key",
                        type=str,
                        help="aws secret access key, required to upload"
                        )
    parser.add_argument("--region-name",
                        type=str,
                        default="us-east-1",
                        help="aws secret access key, required to upload"
                        )


def upload_file(file_name, args: argparse.Namespace):
    if args.bucket_name:
        if not args.aws_access_key_id or not args.aws_secret_access_key:
            raise RuntimeError("Please give aws credentials to upload")
        boto3.Session(
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.region_name
        ).client(
            "s3"
        ).upload_file(
            file_name,
            args.bucket_name,
            f"{dt.now().strftime('%Y-%m-%d-%h-%M-%s')}-{os.path.basename(file_name)}"
        )
