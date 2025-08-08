from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from .core import list_objects
from .copy import copy_object
from .utils import relativize_keys

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

def _head_map(s3, bucket, keys):
    out = {}
    for k in keys:
        try:
            h = s3.head_object(Bucket=bucket, Key=k)
            out[k] = (h.get("ETag", "").strip('"'), h.get("ContentLength", None))
        except ClientError:
            out[k] = (None, None)
    return out

def sync_prefix(
    s3_client,
    source_bucket,
    target_bucket,
    prefix_src="",
    prefix_dst="",
    delete_extra=False,
    compare_mode="name",
    max_workers=8,
    dry_run=False,
    delete_batch_size=1000,
    progress=False,
):
    if source_bucket == target_bucket:
        s = prefix_src.rstrip("/")
        d = prefix_dst.rstrip("/")
        if s == d or s.startswith(d) or d.startswith(s):
            raise ValueError("Refusing to sync: src/dst prefixes overlap in the same bucket")

    src_keys = set(list_objects(s3_client, source_bucket, prefix=prefix_src))
    dst_keys = set(list_objects(s3_client, target_bucket, prefix=prefix_dst))

    src_rel = relativize_keys(src_keys, prefix_src)
    dst_rel = relativize_keys(dst_keys, prefix_dst)

    to_copy_rel = src_rel - dst_rel

    if compare_mode in ("etag", "size"):
        common_rel = src_rel & dst_rel
        if common_rel:
            src_full = { (prefix_src + r if prefix_src else r): r for r in common_rel }
            dst_full = { (prefix_dst + r if prefix_dst else r): r for r in common_rel }

            src_head = _head_map(s3_client, source_bucket, list(src_full.keys()))
            dst_head = _head_map(s3_client, target_bucket, list(dst_full.keys()))

            for full_src, rel in src_full.items():
                full_dst = (prefix_dst + rel) if prefix_dst else rel
                s_meta = src_head.get(full_src, (None, None))
                d_meta = dst_head.get(full_dst, (None, None))
                if compare_mode == "etag" and s_meta[0] != d_meta[0]:
                    to_copy_rel.add(rel)
                elif compare_mode == "size" and s_meta[1] != d_meta[1]:
                    to_copy_rel.add(rel)

    copied = []
    errors_copy = []
    if to_copy_rel:
        if dry_run:
            copied = [ (prefix_src + r if prefix_src else r, prefix_dst + r if prefix_dst else r) for r in sorted(to_copy_rel) ]
        else:
            bar = None
            if progress and tqdm:
                bar = tqdm(total=len(to_copy_rel), desc="Copy", unit="obj")
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = []
                for rel in to_copy_rel:
                    src_key = (prefix_src + rel) if prefix_src else rel
                    dst_key = (prefix_dst + rel) if prefix_dst else rel
                    futs.append(ex.submit(copy_object, s3_client, source_bucket, src_key, target_bucket, dst_key))
                for f in as_completed(futs):
                    try:
                        f.result()
                    except Exception as e:
                        errors_copy.append(str(e))
                    finally:
                        if bar:
                            bar.update(1)
            if bar:
                bar.close()
            copied = sorted(list(to_copy_rel))

    deleted = []
    errors_delete = []
    if delete_extra:
        extras_rel = dst_rel - src_rel
        if extras_rel:
            if dry_run:
                deleted = [ (prefix_dst + r if prefix_dst else r) for r in extras_rel ]
            else:
                pass

    return {
        "copied": copied,
        "deleted": deleted,
        "errors_copy": errors_copy,
        "errors_delete": errors_delete,
        "stats": {
            "compare_mode": compare_mode,
            "dry_run": dry_run,
            "delete_extra": delete_extra,
        },
    }
