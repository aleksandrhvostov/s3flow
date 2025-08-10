# s3-toolkit

Config-driven utilities for common Amazon S3 workflows:
- list objects and pseudo-folders (prefixes)
- download / copy / move / sync
- higher-level helpers (copy “common vs addon” folders)
- optional PID/SO extraction helpers via patterns

The examples are **config-driven** (YAML) and use the package modules end-to-end—no raw boto3 in example code.

---

## Requirements

- Python 3.10+ (works on macOS/Linux/Windows)
- AWS credentials configured (e.g., `~/.aws/credentials` or env vars)
- Install deps:
  ```bash
  pip install -r requirements.txt

## requirements.txt
```
boto3
botocore
pyyaml
pandas
```

## Project layout
```bash
s3-toolkit/
├─ s3_utils/
│  ├─ __init__.py
│  ├─ core.py              # auth, list_objects, list_prefixes, list_prefix_names
│  ├─ download.py          # download_file, download_by_mask, etc.
│  ├─ copy.py              # copy_by_mask, copy_files_by_keys, copy_common_and_addon_from_roots, ...
│  ├─ move.py              # move helpers
│  ├─ sync.py              # sync prefixes
│  ├─ errors.py            # optional logging/decorators
│  └─ utils.py             # read_yaml and misc helpers
│
├─ s3_case_helpers/
│  ├─ __init__.py
│  └─ pid_mapping.py       # pattern-based PID/SO extraction (optional)
│
├─ config/
│  ├─ config.yaml          # all runtime settings for examples
│  └─ patterns.yaml        # regex patterns for PID/SO (if you use case helpers)
│
├─ examples/
│  ├─ download_example.py
│  ├─ copy_example.py
│  ├─ move_example.py
│  ├─ sync_example.py
│  ├─ download_from_csv_example.py
│  └─ copy_common_addon_example.py   # <— new, tiny, high-level
│
├─ requirements.txt
├─ .gitignore
└─ README.md
```

## Configuration 
All examples read from config/config.yaml. Template:
```yaml
aws:
  profile: "aws_profile"     # or null to use env/default
  region: null               # e.g. "us-east-1"

s3:
  bucket: "your-s3-bucket"
  root_prefix: "your/root/prefix"   # used by some examples

paths:
  output_dir: "./outputs"
  csv_1: "path/to/first.csv"
  csv_2: "path/to/second.csv"
  cases_folder: "path/to/cases_folder"
  storage_manager_dir: "path/to/storage_manager_dir"

examples:
  mask:
    prefix: "some/prefix/"
    suffix: ".ext"

  copy:
    source_bucket: "source-bucket"
    target_bucket: "target-bucket"
    prefix: "source/prefix/"
    suffix: ".ext"
    prefix_dst: "dest/prefix/"

  move:
    source_bucket: "source-bucket"
    target_bucket: "target-bucket"
    prefix: "to/move/"
    suffix: ".ext"
    prefix_dst: "moved/"

  sync:
    source_bucket: "source-bucket"
    target_bucket: "target-bucket"
    prefix_src: "sync/src/"
    prefix_dst: "sync/dst/"
    delete_extra: false

  csv_download:
    csv_file: "path/to/input.csv"
    column: "your_column"
    s3_prefix: "files/prefix/"
    max_workers: 8

  copy_common_addon:
    bucket: "your-s3-bucket"
    src_root: "path/source_root/"          # e.g. "cases_src/"
    ref_root: "path/reference_root/"       # e.g. "cases_ref/"
    common_dst_root: "path/common_dst/"    # e.g. "cases_original/"
    addon_dst_root: "path/addon_dst/"      # e.g. "cases_addon/"
    max_workers: 10
```
If you also use PID/SO helpers, maintain config/patterns.yaml with your regexes.

## Usage
### s3flow

Small S3 toolkit: sync/copy/move/download with parallelism and optional progress bars.

### Install
```bash
pip install -r requirements.txt
```

### CLI
#### Sync
```bash
python -m s3_utils.cli sync \
  --source-bucket my-source \
  --target-bucket my-target \
  --prefix-src data/v1/ \
  --prefix-dst data/v1/ \
  --delete-extra \
  --progress \
  --verbose
```
--compare-mode name|etag|size

--dry-run to preview actions

--delete-extra removes keys in target missing in source (batched, ≤1000 per request)
#### Download 
```bash
python -m s3_utils.cli download \
  --bucket my-bucket \
  --prefix images/ \
  --suffix .jpg \
  --dst-root ./downloads \
  --keep-structure \
  --progress
```
#### Move
```bash
python -m s3_utils.cli move \
  --source-bucket my-source \
  --target-bucket my-target \
  --prefix tmp/ \
  --prefix-dst archive/tmp/ \
  --progress \
  --delete-batch-size 1000
```
#### Python API
```python 
from s3_utils.core import get_s3_client
from s3_utils.sync import sync_prefix

s3 = get_s3_client()
res = sync_prefix(s3, "src-bucket", "dst-bucket", "data/", "data/", delete_extra=True, progress=True)
print(res["stats"])
```
#### Config 
CLI reads config/config.yaml by default. Use --config to pass a different file.
