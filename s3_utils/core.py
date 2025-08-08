import boto3

def get_s3_client(aws_profile=None, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    else:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
    return session.client("s3")

def list_objects(s3_client, bucket, prefix='', suffix=''):
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(suffix):
                yield key


def list_prefixes(s3_client, bucket, prefix, delimiter="/"):
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
    return [p["Prefix"].rstrip("/") for p in resp.get("CommonPrefixes", [])]


def list_prefix_names(s3_client, bucket, root_prefix):
    names = set()
    for p in list_prefixes(s3_client, bucket, root_prefix):
        # p looks like 'root_prefix/<name>'
        name = p[len(root_prefix):].strip("/")
        if name:
            names.add(name)
    return names


def object_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception:
        return False
