from s3_utils.core import get_s3_client
from s3_utils.copy import copy_by_mask
from s3_utils.utils import read_yaml

CONFIG_PATH = "config/config.yaml"

if __name__ == "__main__":
    cfg = read_yaml(CONFIG_PATH)
    s3 = get_s3_client(aws_profile=cfg["aws"]["profile"], region_name=cfg["aws"]["region"])
    ex = cfg["examples"]["copy"]
    keys = copy_by_mask(
        s3, ex["source_bucket"], ex["target_bucket"],
        prefix=ex["prefix"], suffix=ex["suffix"], prefix_dst=ex["prefix_dst"]
    )
    print(f"Copied {len(keys)} objects.")
