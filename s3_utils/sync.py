from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from .core import list_objects
from .copy import copy_object

def _relativize(keys, prefix):
    rel = set()
    for k in keys:
        if prefix and k.startswith(prefix):
            rel.add(k[len(prefix):])
        else:
            rel.add(k)
    return rel

def _head_map(s3, bucket, keys):
    """Return {key: (etag, size)} for quick comparisons."""
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
    compare_mode="name",   # 'name' | 'etag' | 'size'
    max_workers=8,
    dry_run=False,
    delete_batch_size=1000
):
    """
    Sync keys under prefix_src -> prefix_dst.
    compare_mode:
      - 'name' : copy if name missing on dst
      - 'etag' : also compare ETag; copy if missing or ETag differs
      - 'size' : compare ContentLength
    """

    # safety guard: prevent obvious self-sync mistakes
    if source_bucket == target_bucket:
        s = prefix_src.rstrip("/")
        d = prefix_dst.rstrip("/")
        if s == d or s.startswith(d) or d.startswith(s):
            raise ValueError("Refusing to sync: src/dst prefixes overlap in the same bucket")

    src_keys = set(list_objects(s3_client, source_bucket, prefix=prefix_src))
    dst_keys = set(list_objects(s3_client, target_bucket, prefix=prefix_dst))

    src_rel = _relativize(src_keys, prefix_src)
    dst_rel = _relativize(dst_keys, prefix_dst)

    # Determine what to copy
    to_copy_rel = src_rel - dst_rel

    # Optional deep compare
    if compare_mode in ("etag", "size"):
        # keys present on both sides; check content
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
                if compare_mode == "etag":
                    if s_meta[0] != d_meta[0]:
                        to_copy_rel.add(rel)
                elif compare_mode == "size":
                    if s_meta[1] != d_meta[1]:
                        to_copy_rel.add(rel)

    # Copy
    copied = []
    errors_copy = []
    if to_copy_rel:
        if dry_run:
            copied = [ (prefix_src + r if prefix_src else r, prefix_dst + r if prefix_dst else r) for r in sorted(to_copy_rel) ]
        else:
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
            copied = sorted(list(to_copy_rel))

    # Delete extras
    deleted = []
    errors_delete = []
    if delete_extra:
        extras_rel = dst_rel - src_rel
        if extras_rel:
            if dry_run:
                deleted = [ (prefix_dst + r if prefix_dst else r) for r in sorted(extras_rel) ]
            else:
                # batch with DeleteObjects (up to 1000 keys per call)
                keys_list = [ {"Key": (prefix_dst + r) if prefix_dst else r } for r in extras_rel ]
                for i in range(0, len(keys_list), delete_batch_size):
                    chunk = keys_list[i:i+delete_batch_size]
                    try:
                        resp = s3_client.delete_objects(Bucket=target_bucket, Delete={"Objects": chunk, "Quiet": True})
                        # collect actually deleted keys if needed:
                        for d in resp.get("Deleted", []):
                            deleted.append(d.get("Key"))
                        for e in resp.get("Errors", []):
                            errors_delete.append(f"{e.get('Key')}: {e.get('Message')}")
                    except ClientError as e:
                        errors_delete.append(str(e))

    return {
        "copied": copied,                 # list of rel paths (or (src,dst) in dry-run)
        "deleted": deleted,               # list of dst keys (or planned in dry-run)
        "errors_copy": errors_copy,
        "errors_delete": errors_delete,
        "stats": {
            "src_total": len(src_keys),
            "dst_total": len(dst_keys),
            "to_copy": len(to_copy_rel),
            "to_delete": len(deleted) if not dry_run else len(dst_rel - src_rel),
            "compare_mode": compare_mode,
            "dry_run": dry_run,
        }
    }
