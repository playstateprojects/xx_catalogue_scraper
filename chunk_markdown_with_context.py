#!/usr/bin/env python3
"""
Intelligent Markdown Chunker with Context Summaries.

This script splits large markdown documents into overlapping chunks and uses
Kimi API to generate context summaries for better continuity.
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
        logging.FileHandler('chunk_markdown.log')
    ]
)
logger = logging.getLogger(__name__)

# Set up Kimi client
client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),
    base_url="https://api.moonshot.ai/v1",
    timeout=600,  # 10 minutes for context extraction
)

# Configuration
INPUT_DIR = "markdown_documents"
OUTPUT_DIR = "markdown_chunks"
CHUNK_SIZE = 8000  # Characters per chunk (Kimi can handle ~200k context)
OVERLAP_SIZE = 1000  # Character overlap between chunks
MIN_CHUNK_SIZE = 2000  # Don't create chunks smaller than this

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# System prompt for comprehensive context extraction
context_system_prompt = """
You are an expert at extracting and maintaining comprehensive contextual information from musical catalogue documents.

Your task is to analyze document chunks and extract/update a comprehensive context summary that will be valuable for processing ALL subsequent chunks.

Extract and maintain information about:

**COMPOSERS:**
- Names of all composers mentioned
- Their periods/styles (Baroque, Classical, Romantic, etc.)
- Birth/death years if mentioned
- Nationality/origin
- Notable characteristics or specializations

**PUBLICATIONS & SOURCES:**
- Publisher names and information
- Publication series or collections
- Catalogue systems used (BWV, K, Op., etc.)
- Publication years or periods
- Editor names
- Source libraries or archives

**MUSICAL CONTEXT:**
- Genres being covered (chamber music, opera, etc.)
- Instrumentation patterns
- Time periods of compositions
- Geographic regions
- Performance traditions

**ORGANIZATIONAL STRUCTURE:**
- How the catalogue is organized
- Section headings or categories
- Numbering systems
- Cross-references

**TECHNICAL INFORMATION:**
- ISMN patterns or publisher codes
- Availability information (in print, manuscript, etc.)
- Duration patterns
- Difficulty levels if mentioned

Format your response as a structured context summary that can be used for all future chunks:

COMPOSERS: [list key composers and their info]
PUBLISHERS: [publisher info, series, etc.]
CATALOGUE_SYSTEM: [numbering/organization system]
GENRES: [main musical genres covered]
PERIODS: [time periods/eras covered]
TECHNICAL: [ISMNs, availability patterns, etc.]
NOTES: [any other important context]

Keep each section concise but comprehensive. This context will be used for ALL remaining chunks.
"""

def read_markdown_file(file_path):
    """Read markdown file content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return None

def find_good_split_point(text, target_pos, search_range=200):
    """
    Find a good place to split text near target_pos.
    Look for paragraph breaks, section breaks, or other natural boundaries.
    """
    start_search = max(0, target_pos - search_range)
    end_search = min(len(text), target_pos + search_range)
    search_text = text[start_search:end_search]
    
    # Look for good split points in order of preference
    split_patterns = [
        r'\n\n## ',      # New major section
        r'\n\n### ',     # New subsection  
        r'\n\n#### ',    # New sub-subsection
        r'\n\n\| ',      # Table start
        r'\n\n---',      # Horizontal rule
        r'\n\n\*\*',     # Bold text (often titles)
        r'\n\n',         # Paragraph break
        r'\. ',          # Sentence end
        r', ',           # Comma
    ]
    
    for pattern in split_patterns:
        matches = list(re.finditer(pattern, search_text))
        if matches:
            # Find the match closest to our target
            target_in_search = target_pos - start_search
            best_match = min(matches, key=lambda m: abs(m.start() - target_in_search))
            return start_search + best_match.start()
    
    # Fallback to target position if no good split found
    return target_pos

def extract_comprehensive_context(chunk_text, filename, existing_context=""):
    """Extract and update comprehensive context information using Kimi API."""
    try:
        if existing_context:
            user_prompt = f"""
            Document: {filename}
            
            EXISTING CONTEXT (from previous chunks):
            {existing_context}
            
            NEW CHUNK CONTENT:
            {chunk_text}
            
            Update and enhance the existing context with any new information from this chunk. 
            Merge information intelligently - add new composers, update publisher info, 
            expand genre coverage, etc. Keep the same structured format.
            
            If no significant new context is found, return the existing context unchanged.
            """
        else:
            user_prompt = f"""
            Document: {filename}
            
            This is the first chunk of a musical catalogue document.
            
            CHUNK CONTENT:
            {chunk_text}
            
            Extract comprehensive contextual information that will be useful for 
            understanding and processing ALL subsequent chunks of this document.
            """
        
        response = client.chat.completions.create(
            model="kimi-k2-0711-preview",
            messages=[
                {"role": "system", "content": context_system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=2000,  # More tokens for comprehensive context
            timeout=300  # 5 minutes for context extraction
        )
        
        context = response.choices[0].message.content.strip()
        logger.info(f"  Updated comprehensive context ({len(context)} chars)")
        logger.debug(f"  Context preview: {context[:200]}...")
        return context
        
    except Exception as e:
        logger.error(f"  Error generating context: {e}")
        fallback = existing_context if existing_context else "Musical catalogue document - context extraction failed."
        return fallback

def create_smart_chunks(content, filename):
    """
    Create overlapping chunks with comprehensive contextual information.
    """
    if len(content) <= CHUNK_SIZE:
        logger.info(f"  Document fits in single chunk ({len(content)} chars)")
        return [content]
    
    chunks = []
    current_pos = 0
    chunk_num = 0
    comprehensive_context = ""
    
    while current_pos < len(content):
        chunk_num += 1
        
        # Calculate chunk boundaries
        chunk_end = min(current_pos + CHUNK_SIZE, len(content))
        
        # Find a good place to split (unless we're at the very end)
        if chunk_end < len(content):
            chunk_end = find_good_split_point(content, chunk_end)
        
        # Extract the chunk
        chunk_text = content[current_pos:chunk_end]
        
        # For the first chunk, extract initial context
        if chunk_num == 1:
            logger.info(f"  Extracting comprehensive context from first chunk...")
            comprehensive_context = extract_comprehensive_context(chunk_text, filename)
            chunk_with_context = chunk_text
            time.sleep(2)  # Brief pause after context extraction
        else:
            # Update comprehensive context with new information
            logger.info(f"  Updating comprehensive context with chunk {chunk_num}...")
            comprehensive_context = extract_comprehensive_context(
                chunk_text, filename, comprehensive_context
            )
            
            # Add the comprehensive context to this chunk
            chunk_with_context = f"""**COMPREHENSIVE DOCUMENT CONTEXT:**

{comprehensive_context}

**CURRENT SECTION:**

{chunk_text}"""
            time.sleep(2)  # Brief pause between API calls
        
        chunks.append(chunk_with_context)
        
        logger.info(f"  Chunk {chunk_num}: chars {current_pos}-{chunk_end}")
        logger.info(f"    With context: {len(chunk_with_context)} total characters")
        
        # Move to next chunk with overlap
        if chunk_end >= len(content):
            break
            
        # Calculate next starting position with overlap
        overlap_start = max(current_pos, chunk_end - OVERLAP_SIZE)
        overlap_start = find_good_split_point(content, overlap_start, 100)
        current_pos = overlap_start
        
        # Ensure we're making progress
        if current_pos >= chunk_end:
            current_pos = chunk_end
    
    logger.info(f"  Created {len(chunks)} chunks with comprehensive context")
    logger.info(f"  Final context size: {len(comprehensive_context)} characters")
    return chunks, comprehensive_context

def save_chunks(chunks, original_filename, comprehensive_context=""):
    """Save chunks to individual files with enhanced metadata."""
    base_name = Path(original_filename).stem
    saved_files = []
    
    for i, chunk in enumerate(chunks):
        chunk_filename = f"{base_name}_chunk_{i+1:03d}.md"
        chunk_path = os.path.join(OUTPUT_DIR, chunk_filename)
        
        # Add metadata header to chunk
        chunk_with_metadata = f"""---
source_file: {original_filename}
chunk_number: {i+1}
total_chunks: {len(chunks)}
chunk_size: {len(chunk)}
context_included: {'yes' if 'COMPREHENSIVE DOCUMENT CONTEXT' in chunk else 'no'}
context_size: {len(comprehensive_context)}
created_by: Enhanced Markdown Chunker with Comprehensive Context
processing_timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
---

{chunk}
"""
        
        try:
            with open(chunk_path, 'w', encoding='utf-8') as f:
                f.write(chunk_with_metadata)
            
            saved_files.append(chunk_filename)
            logger.info(f"  Saved: {chunk_filename} ({len(chunk_with_metadata):,} chars)")
            
        except Exception as e:
            logger.error(f"  Error saving {chunk_filename}: {e}")
    
    # Also save the comprehensive context as a separate file for reference
    if comprehensive_context:
        context_filename = f"{base_name}_CONTEXT.md"
        context_path = os.path.join(OUTPUT_DIR, context_filename)
        
        context_file_content = f"""---
source_file: {original_filename}
context_type: comprehensive_document_context
context_size: {len(comprehensive_context)}
created_by: Enhanced Markdown Chunker
processing_timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
---

# Comprehensive Document Context

This file contains the comprehensive contextual information extracted from the entire document: **{original_filename}**

This context is included in all chunks (except the first) to provide complete background information for processing.

---

{comprehensive_context}
"""
        
        try:
            with open(context_path, 'w', encoding='utf-8') as f:
                f.write(context_file_content)
            logger.info(f"  Saved context: {context_filename}")
        except Exception as e:
            logger.error(f"  Error saving context file: {e}")
    
    return saved_files

def test_kimi_connection():
    """Test Kimi API connection."""
    logger.info("Testing Kimi API connection...")
    
    try:
        response = client.chat.completions.create(
            model="kimi-k2-0711-preview",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Reply with 'connected' if you receive this."}
            ],
            max_tokens=50,
            timeout=60  # 1 minute for connection test
        )
        
        if "connect" in response.choices[0].message.content.lower():
            logger.info("✅ Kimi API connection successful!")
            return True
        else:
            logger.error(f"✗ Unexpected response: {response.choices[0].message.content}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Kimi API connection failed: {e}")
        return False

def process_markdown_files():
    """Process all markdown files in the input directory."""
    markdown_files = glob.glob(os.path.join(INPUT_DIR, "*.md"))
    
    if not markdown_files:
        logger.error(f"No .md files found in '{INPUT_DIR}'")
        return
    
    logger.info(f"Found {len(markdown_files)} markdown files to process")
    
    total_chunks = 0
    processed_files = 0
    
    for i, md_file in enumerate(markdown_files):
        filename = os.path.basename(md_file)
        logger.info(f"[{i+1}/{len(markdown_files)}] Processing: {filename}")
        
        # Read content
        content = read_markdown_file(md_file)
        if not content:
            logger.error(f"  Failed to read: {filename}")
            continue
        
        logger.info(f"  File size: {len(content):,} characters")
        
        # Skip very small files
        if len(content.strip()) < MIN_CHUNK_SIZE:
            logger.warning(f"  Skipping - file too small: {filename}")
            continue
        
        try:
            # Create smart chunks with comprehensive context
            if len(content.strip()) <= CHUNK_SIZE:
                # Single chunk - no need for comprehensive context
                chunks = [content]
                comprehensive_context = ""
            else:
                # Multiple chunks - extract comprehensive context
                chunks, comprehensive_context = create_smart_chunks(content, filename)
            
            # Save chunks
            saved_files = save_chunks(chunks, filename, comprehensive_context)
            
            if saved_files:
                logger.info(f"  ✅ Created {len(saved_files)} chunks from {filename}")
                if comprehensive_context:
                    logger.info(f"     Context: {len(comprehensive_context)} characters")
                total_chunks += len(saved_files)
                processed_files += 1
            else:
                logger.error(f"  ❌ Failed to save chunks for {filename}")
                
        except Exception as e:
            logger.error(f"  Error processing {filename}: {e}")
            continue
        
        # Small delay between files
        if i < len(markdown_files) - 1:
            time.sleep(2)
    
    logger.info("=" * 60)
    logger.info(f"✅ Processing completed!")
    logger.info(f"   Files processed: {processed_files}/{len(markdown_files)}")
    logger.info(f"   Total chunks created: {total_chunks}")
    logger.info(f"   Output directory: {OUTPUT_DIR}")
    logger.info("=" * 60)

def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("ENHANCED MARKDOWN CHUNKER WITH COMPREHENSIVE CONTEXT")
    logger.info("=" * 60)
    
    logger.info(f"This chunker extracts and maintains comprehensive context including:")
    logger.info(f"  • Composer information (periods, styles, nationality)")
    logger.info(f"  • Publisher details and catalogue systems")
    logger.info(f"  • Musical genres and instrumentation patterns")
    logger.info(f"  • Technical information (ISMNs, availability)")
    logger.info(f"  • Organizational structure and references")
    logger.info("")
    logger.info(f"Configuration:")
    logger.info(f"  Chunk size: {CHUNK_SIZE:,} characters")
    logger.info(f"  Overlap size: {OVERLAP_SIZE:,} characters")
    logger.info(f"  Min chunk size: {MIN_CHUNK_SIZE:,} characters")
    logger.info(f"  Input directory: {INPUT_DIR}")
    logger.info(f"  Output directory: {OUTPUT_DIR}")
    
    # Check API key
    if not os.environ.get("MOONSHOT_API_KEY"):
        logger.error("MOONSHOT_API_KEY not found in environment variables.")
        logger.error("Please add your Kimi API key to the .env file.")
        return
    
    # Check input directory
    if not os.path.exists(INPUT_DIR):
        logger.error(f"Input directory '{INPUT_DIR}' does not exist.")
        return
    
    # Test API connection
    if not test_kimi_connection():
        logger.error("API connection test failed. Please check your credentials.")
        return
    
    # Process files
    process_markdown_files()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()