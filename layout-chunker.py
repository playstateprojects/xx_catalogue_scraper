import json
import os
from typing import List, Dict, Any

INPUT_DIR = "doc_json"
OUTPUT_DIR = "chunks_structured"
MAX_TOKENS = 15000
MIN_TOKENS = 2500

os.makedirs(OUTPUT_DIR, exist_ok=True)


def estimate_tokens(text: str) -> int:
    return len(text) // 4


def flatten_entry(entry: Dict) -> str:
    texts = []

    if "textBlock" in entry:
        tb = entry["textBlock"]
        text = tb.get("text", "")
        if text:
            texts.append(text)
        for sub in tb.get("blocks", []):
            texts.append(flatten_entry(sub))

    elif "tableBlock" in entry:
        for row in entry["tableBlock"].get("bodyRows", []):
            for cell in row.get("cells", []):
                for block in cell.get("blocks", []):
                    texts.append(flatten_entry(block))

    elif "blocks" in entry:
        for block in entry["blocks"]:
            texts.append(flatten_entry(block))

    return "\n".join(filter(None, texts))


def chunk_by_token_limit(texts: List[str], max_tokens: int) -> List[str]:
    chunks = []
    current_chunk = ""
    current_tokens = 0

    for text in texts:
        tokens = estimate_tokens(text)
        if current_tokens + tokens > max_tokens and current_chunk:
            chunks.append(current_chunk.strip())
            current_chunk = text + "\n"
            current_tokens = tokens
        else:
            current_chunk += text + "\n"
            current_tokens += tokens

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks


def merge_tiny_chunks(chunks: List[str], min_tokens: int, max_tokens: int) -> List[str]:
    """Merge tiny chunks with neighbors intelligently until they meet min_tokens or exceed max."""
    merged = []
    buffer = ""
    buffer_tokens = 0

    for chunk in chunks:
        chunk = chunk.strip()
        tokens = estimate_tokens(chunk)

        # If it's a large chunk, flush buffer first
        if tokens >= min_tokens:
            if buffer:
                merged.append(buffer.strip())
                buffer = ""
                buffer_tokens = 0
            merged.append(chunk)
            continue

        # Otherwise, accumulate into buffer
        if buffer_tokens + tokens <= max_tokens:
            buffer += ("\n" + chunk) if buffer else chunk
            buffer_tokens += tokens
        else:
            # If buffer is full, flush it and start new buffer
            if buffer:
                merged.append(buffer.strip())
            buffer = chunk
            buffer_tokens = tokens

    # Final flush
    if buffer.strip():
        merged.append(buffer.strip())

    return merged

def split_into_sections(blocks: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    sections = []
    current_section = []

    for block in blocks:
        block_type = block.get("textBlock", {}).get("type", "").lower()
        if block_type.startswith("heading") and current_section:
            sections.append(current_section)
            current_section = [block]
        else:
            current_section.append(block)

    if current_section:
        sections.append(current_section)

    return sections


def process_file(json_path: str) -> int:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    blocks = data.get("documentLayout", {}).get("blocks", data.get("blocks", []))
    if not isinstance(blocks, list):
        print(f"⚠️ Skipping {json_path}: no 'blocks' array found.")
        return 0

    sections = split_into_sections(blocks)
    print(f"📘 {os.path.basename(json_path)} → {len(sections)} sections")

    all_chunks = []
    for section in sections:
        grouped_texts = [flatten_entry(b) for b in section if flatten_entry(b).strip()]
        if not grouped_texts:
            continue
        section_chunks = chunk_by_token_limit(grouped_texts, MAX_TOKENS)
        section_chunks = merge_tiny_chunks(section_chunks, MIN_TOKENS, MAX_TOKENS)
        all_chunks.extend(section_chunks)

    base_name = os.path.splitext(os.path.basename(json_path))[0]
    for i, chunk in enumerate(all_chunks):
        out_path = os.path.join(OUTPUT_DIR, f"{base_name}_chunk_{i:03}.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(chunk)

    return len(all_chunks)


def main():
    json_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".json")]
    total_chunks = 0

    for file_name in json_files:
        full_path = os.path.join(INPUT_DIR, file_name)
        chunks_created = process_file(full_path)
        print(f"✅ {file_name}: {chunks_created} chunks saved\n")
        total_chunks += chunks_created

    print(f"🏁 All done! {total_chunks} chunks saved from {len(json_files)} files.")


if __name__ == "__main__":
    main()
