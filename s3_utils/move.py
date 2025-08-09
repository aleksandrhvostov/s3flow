from __future__ import annotations
from typing import Iterable, List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .core import list_objects
from .copy import copy_object
from .utils import relativize_keys


def _chunked(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _parallel_copy(
    s3_client,
    source_bucket: str,
    target_bucket: str,
    pairs: Iterable[Tuple[str, str]],
    max_workers: int = 8,
    progress: bool = False,
    extra_args: Optional[Dict] = None,
) -> Tuple[List[Tuple[str, str]], List[str]]:
    copied: List[Tuple[str, str]] = []
    errors: List[str] = []
    pairs_list = list(pairs)

    bar = tqdm(total=len(pairs_list), desc="Copy", unit="obj") if progress and pairs_list else None

    def _do(pair: Tuple[str, str]) -> Tuple[str, str]:
        sk, dk = pair
        copy_object(s3_client, source_bucket, sk, target_bucket, dk, extra_args=extra_args)
        return pair

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_do, p) for p in pairs_list]
        for f in as_completed(futs):
            try:
                copied.append(f.result())
            except Exception as e:
                errors.append(str(e))
            finally:
                if bar:
                    bar.update(1)

    if bar:
        bar.close()

    copied.sort()
    return copied, errors


def move_by_mask(
    s3_client,
    source_bucket: str,
    target_bucket: str,
    prefix: str = "",
    suffix: str = "",
    prefix_dst: str = "",
    max_workers: int = 8,
    progress: bool = False,
    extra_args: Optional[Dict] = None,
    dry_run: bool = False,
    delete_batch_size: int = 1000,
) -> Dict[str, List]:
    """
    Move objects matching (prefix, suffix) from source_bucket to target_bucket/prefix_dst.
    Steps: copy all → delete successfully copied sources in batches.
    """
    # Collect source keys and form (src_key, dst_key) pairs
    src_keys = [k for k in list_objects(s3_client, source_bucket, prefix=prefix, suffix=suffix)]
    rel = relativize_keys(src_keys, prefix)

    pairs: List[Tuple[str, str]] = []
    for r in rel:
        sk = f"{prefix}{r}" if prefix else r
        dk = f"{prefix_dst}{r}" if prefix_dst else r
        pairs.append((sk, dk))

    copied_pairs: List[Tuple[str, str]] = []
    copy_errors: List[str] = []
    deleted: List[str] = []
    delete_errors: List[str] = []

    if dry_run:
        # In dry-run, report what would be copied and deleted
        copied_pairs = pairs.copy()
        deleted = [sk for (sk, _) in pairs]
        return {
            "moved": copied_pairs,
            "errors_copy": copy_errors,
            "deleted_source": deleted,
            "errors_delete": delete_errors,
            "stats": {
                "source_bucket": source_bucket,
                "target_bucket": target_bucket,
                "prefix": prefix,
                "prefix_dst": prefix_dst,
                "suffix": suffix,
                "total": len(pairs),
                "dry_run": True,
            },
        }

    # Copy phase
    copied_pairs, copy_errors = _parallel_copy(
        s3_client,
        source_bucket,
        target_bucket,
        pairs=pairs,
        max_workers=max_workers,
        progress=progress,
        extra_args=extra_args,
    )

    # Delete only sources that were successfully copied
    to_delete = [sk for (sk, dk) in copied_pairs]

    bar = tqdm(total=len(to_delete), desc="Delete", unit="obj") if progress and to_delete else None
    for chunk in _chunked(to_delete, min(max(int(delete_batch_size), 1), 1000)):
        try:
            resp = s3_client.delete_objects(
                Bucket=source_bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
            for d in resp.get("Deleted", []) or []:
                if (k := d.get("Key")):
                    deleted.append(k)
                    if bar:
                        bar.update(1)
            for err in resp.get("Errors", []) or []:
                k = err.get("Key")
                code = err.get("Code")
                msg = err.get("Message")
                delete_errors.append(f"{k}: {code} {msg}")
                if bar:
                    bar.update(1)
        except Exception as e:
            for k in chunk:
                delete_errors.append(f"{k}: {e}")
                if bar:
                    bar.update(1)
    if bar:
        bar.close()

    return {
        "moved": copied_pairs,
        "errors_copy": copy_errors,
        "deleted_source": deleted,
        "errors_delete": delete_errors,
        "stats": {
            "source_bucket": source_bucket,
            "target_bucket": target_bucket,
            "prefix": prefix,
            "prefix_dst": prefix_dst,
            "suffix": suffix,
            "total": len(pairs),
            "dry_run": False,
        },
    }
