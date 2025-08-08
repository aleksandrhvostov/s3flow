import os
from botocore.exceptions import ClientError
from .core import list_objects

def download_file(s3_client, bucket, key, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        s3_client.download_file(bucket, key, local_path)
    except ClientError as e:
        raise RuntimeError(f"Download failed for {bucket}/{key}: {e}")

def download_files_by_keys(s3_client, bucket, keys, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    for key in keys:
        filename = os.path.basename(key)
        local_path = os.path.join(local_dir, filename)
        download_file(s3_client, bucket, key, local_path)

def download_by_mask(s3_client, bucket, prefix='', suffix='', local_dir='.'):
    keys = list(list_objects(s3_client, bucket, prefix=prefix, suffix=suffix))
    download_files_by_keys(s3_client, bucket, keys, local_dir)
    return keys

def download_prefix_recursive(s3_client, bucket, prefix, local_root):
    for key in list_objects(s3_client, bucket, prefix=prefix, suffix=''):
        local_path = os.path.join(local_root, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        download_file(s3_client, bucket, key, local_path)
