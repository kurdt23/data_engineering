import json
import os

def create_dir(output_dir='stat'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def save_in_json(data, file_name, output_dir='stat'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(os.path.join(output_dir, f"{file_name}.json"), mode='w', encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=4, ensure_ascii=False)
