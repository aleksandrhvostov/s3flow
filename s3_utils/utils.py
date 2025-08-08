import re
import yaml

def filter_keys_by_mask(keys, prefix='', suffix=''):
    return [k for k in keys if k.startswith(prefix) and k.endswith(suffix)]

def is_s3_uri(uri):
    return bool(re.match(r'^s3://[a-zA-Z0-9.\-_]+/.+', uri))

def parse_s3_uri(uri):
    if not is_s3_uri(uri):
        raise ValueError(f"Invalid S3 URI: {uri}")
    parts = uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]

def group_keys_by_prefix(keys, depth=1):
    groups = {}
    for key in keys:
        parts = key.split("/")
        group = "/".join(parts[:depth])
        groups.setdefault(group, []).append(key)
    return groups

def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}
