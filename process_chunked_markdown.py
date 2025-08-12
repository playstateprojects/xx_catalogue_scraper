#!/usr/bin/env python3
"""
Process Chunked Markdown to JSON using Kimi API.

This script processes the chunked markdown files created by chunk_markdown_with_context.py
and extracts structured JSON data for musical works.
"""

import json
import os
import glob
import time
import logging
import re
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('process_chunks.log')
    ]
)
logger = logging.getLogger(__name__)

# Set up Kimi client
client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),
    base_url="https://api.moonshot.ai/v1",
    timeout=600,  # 10 minutes for large requests
)

# Directories
INPUT_DIR = "markdown_chunks"
OUTPUT_DIR = "chunked_works_json"
FINAL_OUTPUT = "consolidated_works.json"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# System prompt optimized for chunked content
system_prompt = """
You are an expert musicologist analyzing chunks of classical music catalogues.

You will receive a markdown chunk that may contain:
- Context summary from the previous chunk
- Current chunk content with musical work information
- The chunk may contain partial information about works

Your task is to extract ALL complete musical works mentioned in the current chunk content. 

IMPORTANT RULES:
1. Only extract works that have enough information to be meaningful (at minimum: Name and Composer)
2. If a work is only partially described (e.g., just mentioned in passing), do not extract it
3. Focus on complete work entries with substantial information
4. Use the context from previous sections to help fill in missing details where logical

Output format: JSON object with "works" array containing complete work objects:

{
  "works": [
    {
      "Name": "Work title",
      "Composer": "Composer name",
      "Source/Collection": "",
      "Publication Year": "",
      "First Performance": "",
      "Duration": "",
      "Availability": "",
      "Link to Score": "",
      "links": [],
      "Status": "",
      "Notes": "",
      "Genre": "Chamber Music|Choral|Opera|Orchestral|Solo|Vocal",
      "SubGenre": "Anthem|Aria|Bagatelle|Ballet|Canon|Cantata|etc",
      "Period": "Medieval|Renaissance|Baroque|Classical|Romantic|20th Century|Contemporary|unknown",
      "Instrumentation": [],
      "Scoring": "",
      "Related Works": [],
      "Long Description": "",
      "Short Description": "",
      "tags": [],
      "Catalog Number": "",
      "ISMN": "International Standard Music Number",
      "OCLC": "WorldCat identifier",
      "ISWC": "International Standard Musical Work Code",
      "publisher": "",
      "name of source": ""
    }
  ],
  "chunk_info": {
    "chunk_id": "filename",
    "works_count": 0,
    "processing_notes": "any relevant notes about this chunk"
  }
}

Rules:
- Use empty string "" for unknown text fields
- Use empty array [] for unknown array fields  
- Only extract works with sufficient detail
- Output ONLY the JSON object, no explanations
"""

def read_chunk_file(file_path):
    """Read chunk file and extract metadata."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract metadata from front matter
        metadata = {}
        content_lines = content.split('\n')
        
        if content_lines[0].strip() == '---':
            # Find end of front matter
            end_idx = -1
            for i, line in enumerate(content_lines[1:], 1):
                if line.strip() == '---':
                    end_idx = i
                    break
            
            if end_idx > 0:
                # Parse metadata
                for line in content_lines[1:end_idx]:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        metadata[key.strip()] = value.strip()
                
                # Get content after metadata
                content = '\n'.join(content_lines[end_idx + 1:])
        
        return content, metadata
        
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return None, {}

def process_chunk_with_kimi(chunk_content, chunk_filename, metadata):
    """Process a single chunk using Kimi API."""
    
    user_prompt = f"""
    Process this chunk from a musical catalogue document:
    
    Chunk ID: {chunk_filename}
    Source File: {metadata.get('source_file', 'unknown')}
    Chunk: {metadata.get('chunk_number', 'unknown')} of {metadata.get('total_chunks', 'unknown')}
    
    Content:
    {chunk_content}
    
    Extract all complete musical works from this chunk. Focus on works that have sufficient detail (at minimum Name and Composer).
    """
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"    Processing with Kimi API (attempt {attempt + 1})...")
            
            response = client.chat.completions.create(
                model="kimi-k2-0711-preview",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3,
                max_tokens=64000,  # Maximum tokens for comprehensive extraction
                timeout=480  # 8 minutes per request
            )
            
            # Parse response
            response_text = response.choices[0].message.content.strip()
            parsed = json.loads(response_text)
            
            # Extract works and chunk info
            works = parsed.get("works", [])
            chunk_info = parsed.get("chunk_info", {})
            
            # Validate works
            valid_works = []
            for work in works:
                if (isinstance(work, dict) and 
                    work.get("Name") and 
                    work.get("Composer") and
                    work.get("Name").strip() and 
                    work.get("Composer").strip()):
                    
                    # Add chunk metadata to work
                    work["source_chunk"] = chunk_filename
                    work["source_file"] = metadata.get('source_file', '')
                    valid_works.append(work)
                else:
                    logger.debug(f"    Skipping incomplete work: {work.get('Name', 'Unnamed')}")
            
            if valid_works or attempt == max_retries - 1:
                logger.info(f"    ✅ Extracted {len(valid_works)} works from chunk")
                return valid_works, chunk_info
                
        except json.JSONDecodeError as e:
            logger.error(f"    JSON decode error: {e}")
            logger.debug(f"    Response: {response_text[:200]}...")
        except Exception as e:
            logger.error(f"    API error: {e}")
        
        if attempt < max_retries - 1:
            time.sleep((attempt + 1) * 2)
    
    return [], {}

def check_if_chunk_already_processed(chunk_filename):
    """Check if a chunk has already been processed by looking for its output file."""
    chunk_stem = Path(chunk_filename).stem
    output_path = os.path.join(OUTPUT_DIR, f"{chunk_stem}.json")
    
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            # Check if it has works or is a valid processed file
            works = existing_data.get("works", [])
            chunk_info = existing_data.get("chunk_info", {})
            
            logger.info(f"    ✅ Found existing results: {len(works)} works")
            return works, chunk_info, existing_data
            
        except Exception as e:
            logger.warning(f"    ⚠️ Error reading existing file, will reprocess: {e}")
            return None, None, None
    
    return None, None, None

def process_all_chunks():
    """Process all chunk files."""
    chunk_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.md")))
    
    if not chunk_files:
        logger.error(f"No .md files found in '{INPUT_DIR}'")
        return
    
    logger.info(f"Found {len(chunk_files)} chunk files to process")
    
    all_works = []
    chunk_results = {}
    processed_count = 0
    
    for i, chunk_file in enumerate(chunk_files):
        chunk_filename = os.path.basename(chunk_file)
        logger.info(f"[{i+1}/{len(chunk_files)}] Processing: {chunk_filename}")
        
        # Read chunk
        content, metadata = read_chunk_file(chunk_file)
        if not content:
            logger.error(f"  Failed to read chunk: {chunk_filename}")
            continue
        
        logger.info(f"  Chunk size: {len(content):,} characters")
        logger.info(f"  Source: {metadata.get('source_file', 'unknown')}")
        
        # Check if chunk already processed (resume capability)
        existing_works, existing_chunk_info, existing_data = check_if_chunk_already_processed(chunk_filename)
        
        if existing_works is not None:
            logger.info(f"  🔄 Resuming: Using existing results (skipping LLM call)")
            works = existing_works
            chunk_info = existing_chunk_info
        else:
            # Process with Kimi
            logger.info(f"  🆕 Processing new chunk with Kimi API...")
            try:
                works, chunk_info = process_chunk_with_kimi(content, chunk_filename, metadata)
            
            if works:
                all_works.extend(works)
                logger.info(f"  ✅ Added {len(works)} works")
                
                # Save individual chunk results
                chunk_output = {
                    "chunk_filename": chunk_filename,
                    "source_file": metadata.get('source_file', ''),
                    "chunk_metadata": metadata,
                    "works": works,
                    "chunk_info": chunk_info
                }
                
                output_path = os.path.join(OUTPUT_DIR, f"{Path(chunk_filename).stem}.json")
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(chunk_output, f, indent=2, ensure_ascii=False)
                
            else:
                logger.info(f"  No works found in chunk")
            
            chunk_results[chunk_filename] = {
                "works_count": len(works),
                "chunk_info": chunk_info
            }
            
            processed_count += 1
            
        except Exception as e:
            logger.error(f"  Error processing {chunk_filename}: {e}")
        
        # Small delay between chunks
        time.sleep(1)
    
    # Save consolidated results
    if all_works:
        # Group works by source file
        works_by_source = {}
        for work in all_works:
            source = work.get('source_file', 'unknown')
            if source not in works_by_source:
                works_by_source[source] = []
            works_by_source[source].append(work)
        
        # Create final consolidated output
        final_output = {
            "processing_summary": {
                "total_chunks_processed": processed_count,
                "total_works_extracted": len(all_works),
                "source_files": list(works_by_source.keys()),
                "processing_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            "works_by_source": works_by_source,
            "all_works": all_works,
            "chunk_results": chunk_results
        }
        
        # Save final consolidated file
        final_path = os.path.join(OUTPUT_DIR, FINAL_OUTPUT)
        with open(final_path, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info(f"✅ Processing completed!")
        logger.info(f"   Chunks processed: {processed_count}/{len(chunk_files)}")
        logger.info(f"   Total works extracted: {len(all_works)}")
        logger.info(f"   Source files covered: {len(works_by_source)}")
        logger.info(f"   Individual chunk results: {OUTPUT_DIR}")
        logger.info(f"   Consolidated output: {final_path}")
        
        # Show works per source file
        for source, works in works_by_source.items():
            logger.info(f"     {source}: {len(works)} works")
        
        logger.info("=" * 60)
        
    else:
        logger.error("❌ No works extracted from any chunks")

def test_kimi_connection():
    """Test Kimi API connection."""
    logger.info("Testing Kimi API connection...")
    
    try:
        response = client.chat.completions.create(
            model="kimi-k2-0711-preview",
            messages=[
                {"role": "system", "content": "Output valid JSON."},
                {"role": "user", "content": "Return JSON: {'status': 'connected'}"}
            ],
            response_format={"type": "json_object"},
            max_tokens=100,
            timeout=60  # 1 minute for connection test
        )
        
        result = json.loads(response.choices[0].message.content)
        if result.get("status") == "connected":
            logger.info("✅ Kimi API connection successful!")
            return True
        else:
            logger.error(f"✗ Unexpected response: {result}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Kimi API connection failed: {e}")
        return False

def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("CHUNKED MARKDOWN TO JSON PROCESSOR")
    logger.info("=" * 60)
    
    # Check API key
    if not os.environ.get("MOONSHOT_API_KEY"):
        logger.error("MOONSHOT_API_KEY not found in environment variables.")
        return
    
    # Check input directory
    if not os.path.exists(INPUT_DIR):
        logger.error(f"Input directory '{INPUT_DIR}' does not exist.")
        logger.error("Run chunk_markdown_with_context.py first to create chunks.")
        return
    
    # Test API connection
    if not test_kimi_connection():
        logger.error("API connection test failed.")
        return
    
    # Process chunks
    process_all_chunks()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
