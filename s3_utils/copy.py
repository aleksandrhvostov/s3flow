from concurrent.futures import ThreadPoolExecutor, as_completed
from .core import list_objects, list_prefix_names

def copy_object(s3_client, source_bucket, source_key, target_bucket, target_key):
    s3_client.copy({'Bucket': source_bucket, 'Key': source_key}, target_bucket, target_key)

def _copy_prefix(s3_client, source_bucket, target_bucket, src_prefix, dst_prefix, max_workers=8):
    if source_bucket == target_bucket and src_prefix.rstrip("/") == dst_prefix.rstrip("/"):
        raise ValueError("src_prefix and dst_prefix must differ")

    def dst_key_for(key: str) -> str:
        suffix = key[len(src_prefix):].lstrip("/")
        return f"{dst_prefix}{suffix}"

    oks = 0
    errs = 0
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for key in list_objects(s3_client, source_bucket, prefix=src_prefix):
            futures.append(ex.submit(
                copy_object, s3_client, source_bucket, key, target_bucket, dst_key_for(key)
            ))
        for f in as_completed(futures):
            try:
                f.result()
                oks += 1
            except Exception:
                errs += 1
    return {"copied": oks, "errors": errs}

def copy_common_and_addon_from_roots(
    s3_client,
    bucket,
    src_root_prefix,
    ref_root_prefix,
    common_dst_root_prefix,
    addon_dst_root_prefix,
    max_workers=8
):
    src_names = list_prefix_names(s3_client, bucket, src_root_prefix)
    ref_names = list_prefix_names(s3_client, bucket, ref_root_prefix)

    common = sorted(src_names & ref_names)
    addon  = sorted(src_names - ref_names)

    summary = {"common": {}, "addon": {}, "common_count": len(common), "addon_count": len(addon)}

    for name in common:
        res = _copy_prefix(
            s3_client,
            bucket, bucket,
            f"{src_root_prefix}{name}/",
            f"{common_dst_root_prefix}{name}/",
            max_workers=max_workers
        )
        summary["common"][name] = res

    for name in addon:
        res = _copy_prefix(
            s3_client,
            bucket, bucket,
            f"{src_root_prefix}{name}/",
            f"{addon_dst_root_prefix}{name}/",
            max_workers=max_workers
        )
        summary["addon"][name] = res

    return summary

def copy_files_by_keys(s3_client, source_bucket, target_bucket, keys, prefix_src='', prefix_dst=''):
    for key in keys:
        src_key = key[len(prefix_src):] if prefix_src and key.startswith(prefix_src) else key
        dst_key = f"{prefix_dst}{src_key}" if prefix_dst else src_key
        copy_object(s3_client, source_bucket, key, target_bucket, dst_key)

def copy_by_mask(s3_client, source_bucket, target_bucket, prefix='', suffix='', prefix_dst=''):
    keys = list(list_objects(s3_client, source_bucket, prefix=prefix, suffix=suffix))
    copy_files_by_keys(s3_client, source_bucket, target_bucket, keys, prefix_src=prefix, prefix_dst=prefix_dst)
    return keys

def copy_multiple_prefixes(
    s3_client,
    source_bucket,
    target_bucket,
    src_prefixes,
    src_root_prefix,
    dst_root_prefix,
    max_workers=8
):
    summary = {}
    for folder in src_prefixes:
        src_pref = f"{src_root_prefix}{folder}/"
        dst_pref = f"{dst_root_prefix}{folder}/"
        res = _copy_prefix(s3_client, source_bucket, target_bucket, src_pref, dst_pref, max_workers=max_workers)
        summary[folder] = res
    return summary
