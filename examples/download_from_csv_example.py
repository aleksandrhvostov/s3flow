import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from s3_utils.core import get_s3_client
from s3_utils.download import download_file
from s3_utils.utils import read_yaml

CONFIG_PATH = "config/config.yaml"

def main():
    cfg = read_yaml(CONFIG_PATH)
    s3 = get_s3_client(aws_profile=cfg["aws"]["profile"], region_name=cfg["aws"]["region"])

    bucket = cfg["s3"]["bucket"]
    out_dir = cfg["paths"]["output_dir"]
    os.makedirs(out_dir, exist_ok=True)

    ex = cfg["examples"]["csv_download"]
    df = pd.read_csv(ex["csv_file"])
    values = df[ex["column"]].dropna().unique().tolist()
    file_names = [os.path.basename(str(p)) for p in values]
    s3_prefix = ex["s3_prefix"]
    max_workers = int(ex.get("max_workers", 8))

    def task(name):
        key = f"{s3_prefix}{name}"
        local_path = os.path.join(out_dir, name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            download_file(s3, bucket, key, local_path)
            print(f"[OK]  {name}")
        except Exception as e:
            print(f"[ERR] {name}: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        pool.map(task, file_names)

if __name__ == "__main__":
    main()
