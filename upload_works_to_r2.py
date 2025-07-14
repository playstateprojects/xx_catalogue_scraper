import os
import re
import csv
import json
import boto3
import unicodedata
import datetime
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

# Load .env credentials
load_dotenv()

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_BUCKET = os.getenv("R2_BUCKET", "composer-data")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# S3 client setup
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
)

# Paths for logs
log_path = Path("upload_log.jsonl")
unmatched_log_path = Path("unmatched_composers.log")
composer_csv_path = Path("composer_names.csv")

def slugify(text: str) -> str:
    text = re.sub(r'[^\w\s-]', '', text)
    return re.sub(r'[-\s]+', '-', text.strip().lower())

def normalize_ascii(value: str) -> str:
    return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')

def load_composer_name_map(csv_path: Path) -> Dict[str, str]:
    name_to_id = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip()
            composer_id = row["composer_id"].strip()
            if name and composer_id:
                name_to_id[name] = composer_id
    return name_to_id

def make_markdown(data: Dict) -> str:
    lines = [f"# {data.get('Name', 'Untitled')}\n"]

    for label, key in [
        ("Composer", "Composer"),
        ("Genre", "Genre"),
        ("SubGenre", "SubGenre"),
        ("Instrumentation", "Instrumentation"),
        ("Publisher", "publisher"),
        ("ISMN", "ISMN"),
        ("Catalog Number", "Catalog Number"),
    ]:
        value = data.get(key)
        if value:
            if isinstance(value, list):
                value = ", ".join(value)
            lines.append(f"**{label}:** {value}  ")

    lines.append("\n## Description")
    if data.get("Long Description"):
        lines.append(data["Long Description"])
    elif data.get("Short Description"):
        lines.append(data["Short Description"])

    if data.get("Notes"):
        lines.append("\n## Notes\n" + data["Notes"])

    if data.get("Link to Score") or data.get("links"):
        lines.append("\n## Links")
        if data.get("Link to Score"):
            lines.append(f"- [Link to score]({data['Link to Score']})")
        for link in data.get("links", []):
            lines.append(f"- {link}")

    if data.get("tags"):
        lines.append("\n## Tags")
        lines.append(", ".join(data["tags"]))

    return "\n".join(lines)

def extract_metadata(data: Dict, name_to_id: Dict[str, str]) -> Dict:
    fields = [
        "Composer", "Catalog Number", "ISMN", "publisher",
        "period", "availability", "Genre", "SubGenre"
    ]
    meta = {}
    for field in fields:
        value = data.get(field)
        if value:
            meta_key = field.lower().replace(" ", "_")
            meta[meta_key] = normalize_ascii(str(value))

    composer_name = data.get("Composer", "").strip()
    if composer_name:
        composer_id = name_to_id.get(composer_name)
        if composer_id:
            meta["composer_id"] = composer_id
        else:
            with open(unmatched_log_path, "a", encoding="utf-8") as logf:
                logf.write(composer_name + "\n")

    return meta

def has_already_uploaded(key: str) -> bool:
    if not log_path.exists():
        return False
    with open(log_path, 'r', encoding='utf-8') as f:
        return any(key in line for line in f)

def log_upload(key: str):
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps({
            "key": key,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }) + "\n")

def upload_markdown(content: str, metadata: Dict, genre: str, subgenre: str, title: str):
    key = f"{genre}/{subgenre}/{slugify(title)}.md"

    if has_already_uploaded(key):
        print(f"⏭️  Skipping (already uploaded): {key}")
        return

    s3.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=content.encode('utf-8'),
        ContentType='text/markdown',
        Metadata=metadata
    )
    log_upload(key)
    print(f"✅ Uploaded: {key}")

def process_json_file(json_path: Path, name_to_id: Dict[str, str]):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    genre = slugify(data.get("Genre", "Unknown"))
    subgenre = slugify(data.get("SubGenre", "Unknown"))
    title = data.get("Name", "untitled")

    markdown = make_markdown(data)
    metadata = extract_metadata(data, name_to_id)

    # Only upload if composer_id is present
    if "composer_id" not in metadata:
        print(f"❌ Skipping (no composer_id): {title}")
        return

    upload_markdown(markdown, metadata, genre, subgenre, title)

if __name__ == "__main__":
    composer_map = load_composer_name_map(composer_csv_path)
    input_folder = Path("works_json")  # adjust this path as needed
    for json_file in input_folder.glob("*.json"):
        process_json_file(json_file, composer_map)
