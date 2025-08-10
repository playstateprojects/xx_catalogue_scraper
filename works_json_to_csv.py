#!/usr/bin/env python3
# pip install pandas
import os, json, pandas as pd, logging

# ---------- logging ----------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

WORKS_FIELDS = [
    "name", "composer", "source", "publication_year", "first_performance",
    "duration", "availability", "link_to_score", "links", "status", "notes",
    "genre", "period", "instrumentation", "related_works", "long_description",
    "short_description", "tags", "catalog_number", "ismn", "publisher"
]

# 1. Build composer_id lookup: name -> composer_id
composer_df = pd.read_csv("composer_names.csv")
name_to_id = dict(zip(composer_df["name"].str.strip(), composer_df["composer_id"]))
logging.info("Loaded %d composer name -> id mappings", len(name_to_id))

folder = "works_json"
json_files = [f for f in os.listdir(folder) if f.lower().endswith('.json')]
logging.info("Found %d JSON files in %s", len(json_files), folder)

rows = []
for fname in json_files:
    path = os.path.join(folder, fname)
    with open(path, encoding='utf-8') as fh:
        data = json.load(fh)

    original_id = data.get("composer_id")
    if original_id:
        logging.debug("%s already has composer_id=%s", fname, original_id)
    else:
        composer_name = str(data.get("Composer", "")).strip()
        logging.debug("%s -> Composer='%s'", fname, composer_name)
        data["composer_id"] = name_to_id.get(composer_name)
        if data["composer_id"]:
            logging.info("Mapped %s -> composer_id=%s", composer_name, data["composer_id"])
        else:
            logging.warning("NO composer_id found for Composer='%s' in %s", composer_name, fname)

    # Skip if still no composer_id
    if not data.get("composer_id"):
        logging.warning("SKIPPING %s (no composer_id)", fname)
        continue

    # Save updated JSON
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

    rows.append({
        "name"             : data.get("Name"),
        "composer"         : data.get("composer_id"),
        "source"           : data.get("Source/Collection"),
        "publication_year" : data.get("Publication Year"),
        "first_performance": data.get("First Performance"),
        "duration"         : data.get("Duration"),
        "availability"     : data.get("Availability"),
        "link_to_score"    : data.get("Link to Score"),
        "links"            : ";".join(data.get("links", [])),
        "status"           : data.get("Status"),
        "notes"            : data.get("Notes"),
        "genre"            : data.get("Genre"),
        "period"           : data.get("Period"),
        "instrumentation"  : ",".join(data.get("Instrumentation", [])),
        "related_works"    : ",".join(data.get("Related Works", [])),
        "long_description" : data.get("Long Description"),
        "short_description": data.get("Short Description"),
        "tags"             : ",".join(data.get("tags", [])),
        "catalog_number"   : data.get("Catalog Number"),
        "ismn"             : data.get("ISMN"),
        "publisher"        : data.get("publisher")
    })

# ---------- final result ----------
df = pd.DataFrame(rows, columns=WORKS_FIELDS)
df.to_csv("output.csv", index=False, encoding='utf-8')
logging.info("CSV created with %d rows (after composer_id filtering)", len(df))