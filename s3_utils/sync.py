from __future__ import annotations
from typing import List, Optional, Tuple, Dict
from botocore.exceptions import ClientError
from tqdm import tqdm

from .core import list_objects
from .copy import copy_object
from .utils import relativize_keys


def _chunked(iterable, size: int):
    """Yield successive chunks from iterable of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def sync_prefix(
    s3_client,
    source_bucket: str,
    target_bucket: str,
    prefix_src: str = "",
    prefix_dst: str = "",
    delete_extra: bool = False,
    compare_mode: str = "key",  # future: etag/size
    dry_run: bool = False,
    max_workers: int = 8,       # reserved for parallel copy
    progress: bool = False,
) -> Dict[str, List]:
    """
    Sync all objects from source_bucket/prefix_src to target_bucket/prefix_dst.
    Optionally delete extra objects in target that are not in source.
    """
    src_keys = list(list_objects(s3_client, source_bucket, prefix=prefix_src))
    dst_keys = list(list_objects(s3_client, target_bucket, prefix=prefix_dst))

    src_rel = relativize_keys(src_keys, prefix_src)
    dst_rel = relativize_keys(dst_keys, prefix_dst)

    to_copy_rel = sorted(src_rel - dst_rel)
    to_delete_rel = sorted(dst_rel - src_rel) if delete_extra else []

    copied: List[Tuple[str, str]] = []
    errors_copy: List[str] = []
    deleted: List[str] = []
    errors_delete: List[str] = []

    # Copy phase
    copy_bar = tqdm(total=len(to_copy_rel), desc="Copy", unit="obj") if progress and to_copy_rel else None

    for rel in to_copy_rel:
        src_key = f"{prefix_src}{rel}" if prefix_src else rel
        dst_key = f"{prefix_dst}{rel}" if prefix_dst else rel
        if dry_run:
            copied.append((src_key, dst_key))
        else:
            try:
                copy_object(s3_client, source_bucket, src_key, target_bucket, dst_key)
                copied.append((src_key, dst_key))
            except Exception as e:
                errors_copy.append(f"{src_key} -> {dst_key}: {e}")
        if copy_bar:
            copy_bar.update(1)

    if copy_bar:
        copy_bar.close()

    # Delete phase
    delete_bar = tqdm(total=len(to_delete_rel), desc="Delete", unit="obj") if progress and to_delete_rel else None

    if to_delete_rel:
        if dry_run:
            deleted.extend(to_delete_rel)
            if delete_bar:
                delete_bar.update(len(to_delete_rel))
        else:
            for chunk in _chunked(to_delete_rel, 1000):
                objects = [{"Key": f"{prefix_dst}{rel}" if prefix_dst else rel} for rel in chunk]
                try:
                    resp = s3_client.delete_objects(Bucket=target_bucket, Delete={"Objects": objects})
                    deleted.extend([obj["Key"] for obj in resp.get("Deleted", [])])
                    for err in resp.get("Errors", []):
                        errors_delete.append(f"{err['Key']}: {err['Code']} {err['Message']}")
                except ClientError as e:
                    for obj in objects:
                        errors_delete.append(f"{obj['Key']}: {e}")
                if delete_bar:
                    delete_bar.update(len(chunk))

    if delete_bar:
        delete_bar.close()

    return {
        "copied": copied,
        "errors_copy": errors_copy,
        "deleted": deleted,
        "errors_delete": errors_delete,
        "stats": {
            "source_bucket": source_bucket,
            "target_bucket": target_bucket,
            "prefix_src": prefix_src,
            "prefix_dst": prefix_dst,
            "delete_extra": delete_extra,
            "dry_run": dry_run,
            "total_src": len(src_keys),
            "total_dst": len(dst_keys),
            "to_copy": len(to_copy_rel),
            "to_delete": len(to_delete_rel),
        },
    }
