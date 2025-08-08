from botocore.exceptions import ClientError
from .copy import copy_object
from .core import list_objects
from concurrent.futures import ThreadPoolExecutor, as_completed

def move_object(s3_client, source_bucket, source_key, target_bucket, target_key):
    copy_object(s3_client, source_bucket, source_key, target_bucket, target_key)
    try:
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    except ClientError as e:
        raise RuntimeError(f"Failed to delete original {source_bucket}/{source_key}: {e}")

def move_files_by_keys(s3_client, source_bucket, target_bucket, keys, prefix_src='', prefix_dst=''):
    for key in keys:
        src_key = key[len(prefix_src):] if prefix_src and key.startswith(prefix_src) else key
        dst_key = f"{prefix_dst}{src_key}" if prefix_dst else src_key
        move_object(s3_client, source_bucket, key, target_bucket, dst_key)

def move_by_mask(s3_client, source_bucket, target_bucket, prefix='', suffix='', prefix_dst=''):
    """
    Move all objects that match prefix/suffix from source_bucket to target_bucket.
    Destination key keeps relative path after `prefix`, optionally with `prefix_dst` prepended.
    """
    keys = list(list_objects(s3_client, source_bucket, prefix=prefix, suffix=suffix))
    move_files_by_keys(s3_client, source_bucket, target_bucket, keys, prefix_src=prefix, prefix_dst=prefix_dst)
    return keys

# Optional: high-level movers for whole prefixes (parallel), mirroring copy helpers

def _move_prefix(s3_client, source_bucket, target_bucket, src_prefix, dst_prefix, max_workers=8):
    """
    Move (copy+delete) all objects under src_prefix -> dst_prefix (relative paths preserved).
    """
    def dst_key_for(key: str) -> str:
        suffix = key[len(src_prefix):].lstrip("/")
        return f"{dst_prefix}{suffix}"

    oks, errs = 0, 0
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for key in list_objects(s3_client, source_bucket, prefix=src_prefix):
            dst_key = dst_key_for(key)
            futures.append(ex.submit(move_object, s3_client, source_bucket, key, target_bucket, dst_key))
        for f in as_completed(futures):
            try:
                f.result()
                oks += 1
            except Exception:
                errs += 1
    return {"moved": oks, "errors": errs}

def move_multiple_prefixes(
    s3_client,
    source_bucket,
    target_bucket,
    src_prefixes,
    src_root_prefix,
    dst_root_prefix,
    max_workers=8
):
    """
    For each name N in src_prefixes:
      move src_root_prefix/N/ -> dst_root_prefix/N/
    """
    summary = {}
    for name in src_prefixes:
        src_pref = f"{src_root_prefix}{name}/"
        dst_pref = f"{dst_root_prefix}{name}/"
        summary[name] = _move_prefix(s3_client, source_bucket, target_bucket, src_pref, dst_pref, max_workers=max_workers)
    return summary
