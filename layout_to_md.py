import json
from typing import Any, Dict, List
import re

def parse_text_block(block: Dict[str, Any], indent_level: int = 0) -> str:
    if 'text' in block:
        prefix = ""
        if block.get("type") == "heading-1":
            prefix = "# "
        elif block.get("type") == "heading-2":
            prefix = "## "
        elif block.get("type") == "heading-3":
            prefix = "### "
        return f"{prefix}{block['text']}".strip()
    return ""

def parse_table(table: Dict[str, Any]) -> str:
    # Parse simple 2-column tables as Markdown tables
    rows = table.get("bodyRows", [])
    if not rows:
        return ""

    headers = []
    md_lines = []

    for row_index, row in enumerate(rows):
        line = []
        for cell in row["cells"]:
            texts = []
            for block in cell["blocks"]:
                texts.append(parse_text_block(block["textBlock"]))
            line.append(" ".join(texts).strip())

        if row_index == 0:
            headers = line
            md_lines.append("| " + " | ".join(headers) + " |")
            md_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        else:
            md_lines.append("| " + " | ".join(line) + " |")

    return "\n".join(md_lines)

def is_heading(text: str) -> int:
    clean = text.strip().upper()
    if clean in {
        "CONTENTS", "ORCHESTRAL WORKS", "CHAMBER MUSIC", "VOCAL MUSIC", "DISCOGRAPHY",
        "OCCASIONAL WORKS", "AWARDS AND PRIZES", "ALPHABETICAL INDEX", "PREFACE", "VORWORT"
    }:
        return 2  # h2
    if re.fullmatch(r"[A-Z\s\-]+", clean) and len(clean.split()) <= 5:
        return 2  # heuristic: short, all-caps = heading
    return 0

def parse_blocks(blocks: List[Dict[str, Any]]) -> List[str]:
    result = []

    for block in blocks:
        tb = block.get("textBlock")

        if not tb:
            continue

        # If it's a nested heading with child blocks
        if "blocks" in tb and tb.get("type", "").startswith("heading"):
            heading_level = int(tb["type"].split("-")[1])
            result.append(f"{'#' * heading_level} {tb.get('text', '').strip()}")

            # Process child blocks (these are typically paragraphs)
            for child in tb["blocks"]:
                child_tb = child.get("textBlock", {})
                if "text" in child_tb:
                    text = child_tb["text"].strip()
                    if text:
                        result.append(text)

        # Normal flat textBlock (no children)
        elif "text" in tb:
            text = tb["text"].strip()
            heading_level = is_heading(text)
            if heading_level > 0:
                result.append(f"{'#' * heading_level} {text}")
            else:
                result.append(text)

        # If block has tableBlock
        elif "tableBlock" in block:
            table_md = parse_table(block["tableBlock"])
            if table_md:
                result.append(table_md)

    return result


def convert_document_ai_json_to_markdown(doc: Dict[str, Any]) -> str:
    blocks = doc.get("documentLayout", {}).get("blocks", [])
    markdown_output = parse_blocks(blocks)
    return "\n\n".join([line for line in markdown_output if line.strip()])

# Load your JSON file
with open("outputs_annotated-catalog_3242456571404390253_10_gubaidulina_werkverzeichnis-0.json", "r", encoding="utf-8") as f:
    doc_data = json.load(f)

markdown_result = convert_document_ai_json_to_markdown(doc_data)

# Write to markdown file
with open("gubaidulina_werkverzeichnis-0.json.md", "w", encoding="utf-8") as f:
    f.write(markdown_result)

print("Markdown export complete.")
