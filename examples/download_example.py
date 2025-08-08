import os
from s3_utils.core import get_s3_client
from s3_utils.download import download_by_mask
from s3_utils.utils import read_yaml

CONFIG_PATH = "config/config.yaml"

if __name__ == "__main__":
    cfg = read_yaml(CONFIG_PATH)
    s3 = get_s3_client(aws_profile=cfg["aws"]["profile"], region_name=cfg["aws"]["region"])
    out_dir = cfg["paths"]["output_dir"]
    ex = cfg["examples"]["mask"]

    os.makedirs(out_dir, exist_ok=True)
    keys = download_by_mask(s3, cfg["s3"]["bucket"], prefix=ex["prefix"], suffix=ex["suffix"], local_dir=out_dir)
    print(f"Downloaded {len(keys)} objects to {out_dir}")
