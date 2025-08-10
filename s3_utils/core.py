from __future__ import annotations
from typing import Optional, Iterator, Set
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

def get_s3_client(
    aws_profile: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: Optional[str] = None,
    retries_max_attefrom __future__ import annotations
from typing import Iterable, Iterator, Optional, Set, Tuple, Dict, Any
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def get_s3_client(
    aws_profile: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: Optional[str] = None,
    retries_max_attempts: int = 8,
    retries_mode: str = "standard",
    connect_timeout: int = 10,
    read_timeout: int = 60,
):
    """
    Create a boto3 S3 client with retries and timeouts applied.
    """
    cfg = Config(
        retries={"max_attempts": retries_max_attempts, "mode": retries_mode},
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    else:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
    return session.client("s3", config=cfg)


def list_objects(s3_client, bucket: str, prefix: str = "", suffix: str = "") -> Iterator[str]:
    """
    Yield object keys in a bucket filtered by prefix/suffix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            if not key:
                continue
            if key.startswith(prefix) and key.endswith(suffix):
                yield key


def list_prefixes(s3_client, bucket: str, root_prefix: str = "", depth: int = 1) -> Set[str]:
    """
    Return unique logical prefixes (first `depth` path components) under root_prefix.
    Implementation scans object keys to avoid delimiter limitations.
    """
    names: Set[str] = set()
    for key in list_objects(s3_client, bucket, prefix=root_prefix):
        rel = key[len(root_prefix) :].lstrip("/")
        if not rel:
            continue
        parts = rel.split("/")
        take = min(depth, len(parts))
        if take > 0:
            names.add("/".join(parts[:take]))
    return names


def list_prefix_names(s3_client, bucket: str, root_prefix: str = "") -> Set[str]:
    """
    Shorthand for first-level folder names under root_prefix.
    """
    return list_prefixes(s3_client, bucket, root_prefix=root_prefix, depth=1)


def head_object_meta(s3_client, bucket: str, key: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Return (etag, size) for an object, or (None, None) if not accessible.
    """
    try:
        h = s3_client.head_object(Bucket=bucket, Key=key)
        etag = (h.get("ETag") or "").strip('"')
        size = h.get("ContentLength")
        return etag, size
    except ClientError:
        return None, None
    except Exception:
        return None, None


def object_exists(s3_client, bucket: str, key: str) -> bool:
    """
    Return True if the object exists (HEAD succeeds), False otherwise.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        return False
    except Exception:
        return False
mpts: int = 8,
    retries_mode: str = "standard",
    connect_timeout: int = 10,
    read_timeout: int = 60,
):
    """Create a boto3 S3 client with sensible retries and timeouts."""
    cfg = Config(
        retries={"max_attempts": retries_max_attempts, "mode": retries_mode},
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    else:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
    return session.client("s3", config=cfg)

def list_objects(s3_client, bucket: str, prefix: str = "", suffix: str = "") -> Iterator[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

def list_prefixes(s3_client, bucket: str, root_prefix: str = "", depth: int = 1) -> Set[str]:
    """Return all *prefix strings* at given depth under root_prefix.
    E.g. with depth=1 from 'a/b/c.txt' -> collect 'a', with depth=2 -> 'a/b'.
    """
    names: Set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            p = cp.get("Prefix", "")
            if not p:
                continue
            rel = p[len(root_prefix):].strip("/")
            if not rel:
                continue
            parts = rel.split("/")
            if len(parts) >= depth:
                name = "/".join(parts[:depth])
                names.add(name)
            else:
                names.add(rel)
    return names

def list_prefix_names(s3_client, bucket: str, root_prefix: str = "") -> Set[str]:
    """Compatibility helper that returns unique first-level folder names under root_prefix."""
    return list_prefixes(s3_client, bucket, root_prefix=root_prefix, depth=1)

def object_exists(s3_client, bucket: str, key: str) -> bool:
    """HEAD the object; return False on 404/NoSuchKey/NotFound, be conservative otherwise."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        return False
    except Exception:
        return False
