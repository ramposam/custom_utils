import os
import boto3


def download_s3_folder(bucket_name, s3_folder, local_dir):
    """
    Download an entire folder from an S3 bucket to a local directory.

    :param bucket_name: Name of the S3 bucket
    :param s3_folder: Folder path in the S3 bucket
    :param local_dir: Local directory to save the files
    """
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key)

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_folder)

    for page in pages:
        print(page)
        if 'Contents' in page:
            for obj in page['Contents']:
                s3_key = obj['Key']
                local_file_path = os.path.join(local_dir, os.path.relpath(s3_key, s3_folder))

                # Create directories if they don't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # Download the file
                print(f"Downloading {s3_key} to {local_file_path}")
                s3.download_file(bucket_name, s3_key, local_file_path)
