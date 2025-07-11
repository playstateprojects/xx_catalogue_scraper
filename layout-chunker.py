import json
import os
from typing import List, Dict, Union

INPUT_FILE = "layout_parser_output.json"
OUTPUT_DIR = "chunks_structured"
MAX_TOKENS = 1500

os.makedirs(OUTPUT_DIR, exist_ok=True)

def estimate_tokens(text: str) -> int:
    return len(text) // 4  # rough estimate: 1 token ≈ 4 chars

def flatten_entry(entry: Dict) -> str:
    """Flatten a Layout Parser entry including nested blocks."""
    texts = []
    if "textBlock" in entry:
        texts.append(entry["textBlock"].get("text", ""))

    if "blocks" in entry:
        for block in entry["blocks"]:
            texts.append(flatten_entry(block))

    return "\n".join(texts)

def flatten_all(blocks: List[Dict]) -> List[str]:
    """Convert structured LayoutParser blocks into logically grouped strings."""
    grouped_texts = []
    for entry in blocks:
        grouped_texts.append(flatten_entry(entry))
    return grouped_texts

def chunk_by_token_limit(grouped_texts: List[str], max_tokens: int) -> List[str]:
    """Chunk the logically grouped texts into token-limited LLM input segments."""
    chunks = []
    current_chunk = ""
    current_tokens = 0

    for text in grouped_texts:
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

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    blocks = data if isinstance(data, list) else data.get("blocks", [])

    grouped_texts = flatten_all(blocks)
    chunks = chunk_by_token_limit(grouped_texts, MAX_TOKENS)

    for i, chunk in enumerate(chunks):
        with open(os.path.join(OUTPUT_DIR, f"chunk_{i:03}.txt"), "w", encoding="utf-8") as f:
            f.write(chunk)

    print(f"✅ Saved {len(chunks)} structured chunks to '{OUTPUT_DIR}'.")

if __name__ == "__main__":
    main()
