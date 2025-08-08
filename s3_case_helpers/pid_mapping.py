import re
import yaml
import os

def load_patterns_from_yaml(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    return [(p['regex'], p.get('type', 'unknown')) for p in data.get('patterns', [])]

def extract_pid_so_by_patterns(s, patterns):
    for regex, pattern_type in patterns:
        m = re.match(regex, s)
        if m:
            return pattern_type, m.group('pid'), m.group('so')
    return None, None, None

def extract_pid_so_from_folder(folder_path, patterns):
    result = []
    for name in os.listdir(folder_path):
        ptype, pid, so = extract_pid_so_by_patterns(name, patterns)
        if pid and so:
            result.append({'name': name, 'pattern_type': ptype, 'pid': pid, 'so': so})
    return result
