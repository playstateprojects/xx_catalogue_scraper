import json
import os
from typing import List, Dict, Any

MAX_CHARS = 3500
OUTPUT_DIR = "markdown_chunks"
INPUT_PATH = "doc_json/vokal.json"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def deep_extract_text(obj: Dict[str, Any], depth=0) -> str:
    """Recursively walk nested blocks to extract text as Markdown."""
    result = ""

    if "textBlock" in obj:
        tb = obj["textBlock"]
        text = tb.get("text", "")
        block_type = tb.get("type", "").lower()
        subblocks = tb.get("blocks", [])

        # Headings
        if block_type.startswith("heading") and text:
            level = int(block_type[-1]) if block_type[-1].isdigit() else 2
            result += f"{'#' * level} {text}\n\n"
        elif text:
            result += f"{text}\n\n"

        for sub in subblocks:
            result += deep_extract_text(sub, depth + 1)

    elif "tableBlock" in obj:
        result += extract_table(obj["tableBlock"])

    elif "blocks" in obj:
        for sub in obj["blocks"]:
            result += deep_extract_text(sub, depth + 1)

    return result


def extract_table(table_block: Dict[str, Any]) -> str:
    rows = table_block.get("bodyRows", [])
    lines = []
    for i, row in enumerate(rows):
        cells = []
        for cell in row.get("cells", []):
            cell_text = " ".join(deep_extract_text(b).strip() for b in cell.get("blocks", []))
            cells.append(cell_text.strip().replace("\n", " "))
        if not any(cells):
            continue
        lines.append(" | ".join(cells))
        if i == 0:
            lines.append(" | ".join(["---"] * len(cells)))
    return "\n".join(lines) + "\n\n"


def chunk_blocks(blocks: List[Dict[str, Any]]) -> List[str]:
    chunks = []
    current = ""
    for block in blocks:
        md = deep_extract_text(block)
        if len(current) + len(md) > MAX_CHARS:
            chunks.append(current.strip())
            current = ""
        current += md
    if current.strip():
        chunks.append(current.strip())
    return chunks


# Load JSON
with open(INPUT_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

blocks = data.get("documentLayout", {}).get("blocks", [])

chunks = chunk_blocks(blocks)

# Save
for i, chunk in enumerate(chunks):
    with open(os.path.join(OUTPUT_DIR, f"chunk_{i+1:02}.md"), "w", encoding="utf-8") as f:
        f.write(chunk)

print(f"✅ {len(chunks)} markdown chunks saved to {OUTPUT_DIR}")
