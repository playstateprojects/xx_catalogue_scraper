#!/usr/bin/env python3
"""
Convert Document AI JSON files to properly formatted Markdown documents.

This script takes Google Document AI JSON outputs and converts them into 
well-structured markdown files, preserving document hierarchy and formatting.
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
INPUT_DIR = os.getenv('JSON_INPUT_DIR', 'doc_json')
OUTPUT_DIR = os.getenv('MARKDOWN_OUTPUT_DIR', 'markdown_documents')

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Output directory: {OUTPUT_DIR}")

def extract_document_title(blocks: List[Dict[str, Any]]) -> Optional[str]:
    """Extract document title from the first heading or significant text."""
    for block in blocks[:5]:  # Check first 5 blocks
        if "textBlock" in block:
            tb = block["textBlock"]
            text = tb.get("text", "").strip()
            block_type = tb.get("type", "").lower()
            
            # Look for heading blocks
            if block_type.startswith("heading") and text and len(text) < 100:
                return text
            
            # Look for title-like text (short, meaningful)
            if text and len(text) < 100 and len(text) > 10:
                # Skip if it looks like metadata
                if not any(keyword in text.lower() for keyword in ['page', 'document', 'pdf', 'extract']):
                    return text
    
    return None

def process_text_block(obj: Dict[str, Any], depth: int = 0) -> str:
    """
    Process a text block and convert to markdown with proper formatting.
    """
    result = ""
    
    if "textBlock" in obj:
        tb = obj["textBlock"]
        text = tb.get("text", "").strip()
        block_type = tb.get("type", "").lower()
        subblocks = tb.get("blocks", [])
        
        if not text and not subblocks:
            return ""
        
        # Handle different block types
        if block_type.startswith("heading") and text:
            # Determine heading level
            level = 1
            if block_type[-1].isdigit():
                level = min(int(block_type[-1]), 6)  # Cap at h6
            elif depth > 0:
                level = min(depth + 1, 6)
            
            result += f"{'#' * level} {text}\n\n"
            
        elif block_type == "title" and text:
            result += f"# {text}\n\n"
            
        elif block_type in ["subtitle", "subheading"] and text:
            result += f"## {text}\n\n"
            
        elif text:
            # Regular paragraph text
            # Clean up the text
            cleaned_text = clean_text(text)
            if cleaned_text:
                result += f"{cleaned_text}\n\n"
        
        # Process subblocks recursively
        for sub in subblocks:
            result += process_text_block(sub, depth + 1)
    
    return result

def process_table_block(table_block: Dict[str, Any]) -> str:
    """Convert a table block to markdown table format."""
    if not table_block:
        return ""
    
    rows = table_block.get("bodyRows", [])
    header_rows = table_block.get("headerRows", [])
    
    if not rows and not header_rows:
        return ""
    
    lines = []
    
    # Process header rows
    for row in header_rows:
        cells = []
        for cell in row.get("cells", []):
            cell_text = extract_cell_text(cell)
            cells.append(cell_text)
        
        if any(cell.strip() for cell in cells):  # Only add if there's content
            lines.append(" | ".join(cells))
            lines.append(" | ".join(["---"] * len(cells)))
    
    # Process body rows
    for row in rows:
        cells = []
        for cell in row.get("cells", []):
            cell_text = extract_cell_text(cell)
            cells.append(cell_text)
        
        if any(cell.strip() for cell in cells):  # Only add if there's content
            lines.append(" | ".join(cells))
    
    if lines:
        return "\n".join(lines) + "\n\n"
    
    return ""

def extract_cell_text(cell: Dict[str, Any]) -> str:
    """Extract text from a table cell."""
    cell_parts = []
    
    for block in cell.get("blocks", []):
        text = process_text_block(block).strip()
        if text:
            # Remove markdown formatting for table cells
            text = text.replace('\n', ' ').replace('|', '\\|')
            cell_parts.append(text)
    
    return " ".join(cell_parts).strip()

def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = " ".join(text.split())
    
    # Remove common OCR artifacts
    text = text.replace('•', '-')  # Convert bullets
    text = text.replace('–', '-')  # Convert en-dash
    text = text.replace('—', '--') # Convert em-dash
    
    return text.strip()

def process_document_blocks(blocks: List[Dict[str, Any]]) -> str:
    """Process all blocks in a document and return formatted markdown."""
    content_parts = []
    
    for block in blocks:
        if "textBlock" in block:
            text_content = process_text_block(block)
            if text_content:
                content_parts.append(text_content)
                
        elif "tableBlock" in block:
            table_content = process_table_block(block["tableBlock"])
            if table_content:
                content_parts.append(table_content)
                
        elif "blocks" in block:
            # Handle nested blocks
            nested_content = process_document_blocks(block["blocks"])
            if nested_content:
                content_parts.append(nested_content)
    
    return "".join(content_parts)

def generate_filename(json_filename: str, title: Optional[str] = None) -> str:
    """Generate a meaningful markdown filename."""
    base_name = Path(json_filename).stem
    
    if title:
        # Create filename from title
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')[:50]  # Limit length
        if safe_title:
            return f"{safe_title}.md"
    
    # Fallback to original filename
    return f"{base_name}.md"

def add_document_metadata(content: str, json_filename: str, stats: Dict[str, Any]) -> str:
    """Add metadata header to the markdown document."""
    metadata_lines = [
        "---",
        f"source_file: {json_filename}",
        f"processed_date: {stats.get('processed_date', 'unknown')}",
        f"total_pages: {stats.get('page_count', 'unknown')}",
        f"total_characters: {stats.get('char_count', len(content))}",
        "processing_tool: Document AI to Markdown Converter",
        "---",
        "",
    ]
    
    return "\n".join(metadata_lines) + content

def process_json_file(json_path: Path) -> bool:
    """
    Process a single JSON file and convert to markdown.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Processing: {json_path.name}")
        
        # Load JSON data
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract document layout blocks
        document_layout = data.get("documentLayout", {})
        blocks = document_layout.get("blocks", [])
        
        if not blocks:
            logger.warning(f"No blocks found in {json_path.name}")
            return False
        
        # Extract title for filename
        title = extract_document_title(blocks)
        
        # Process blocks to markdown
        markdown_content = process_document_blocks(blocks)
        
        if not markdown_content.strip():
            logger.warning(f"No content extracted from {json_path.name}")
            return False
        
        # Generate stats
        page_info = data.get("pages", [])
        stats = {
            'processed_date': Path(json_path).stat().st_mtime,
            'page_count': len(page_info),
            'char_count': len(markdown_content)
        }
        
        # Add metadata
        final_content = add_document_metadata(markdown_content, json_path.name, stats)
        
        # Generate output filename
        output_filename = generate_filename(json_path.name, title)
        output_path = Path(OUTPUT_DIR) / output_filename
        
        # Handle filename conflicts
        counter = 1
        while output_path.exists():
            name_part = output_path.stem
            output_path = Path(OUTPUT_DIR) / f"{name_part}_{counter}.md"
            counter += 1
        
        # Write markdown file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        
        logger.info(f"  ✅ Created: {output_path.name}")
        logger.info(f"     Title: {title or 'No title detected'}")
        logger.info(f"     Pages: {stats['page_count']}, Characters: {stats['char_count']:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing {json_path.name}: {e}")
        return False

def main():
    """Main execution function."""
    logger.info("Document AI JSON to Markdown Converter")
    logger.info(f"Input directory: {INPUT_DIR}")
    
    # Ensure directories exist
    if not os.path.exists(INPUT_DIR):
        logger.error(f"Input directory does not exist: {INPUT_DIR}")
        return
    
    ensure_output_dir()
    
    # Find JSON files
    input_path = Path(INPUT_DIR)
    json_files = list(input_path.glob("*.json"))
    
    if not json_files:
        logger.warning(f"No JSON files found in {INPUT_DIR}")
        return
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    # Process files
    successful = 0
    failed = 0
    
    for json_file in json_files:
        if process_json_file(json_file):
            successful += 1
        else:
            failed += 1
    
    # Summary
    logger.info(f"\nProcessing complete:")
    logger.info(f"  ✅ Successful: {successful}")
    logger.info(f"  ❌ Failed: {failed}")
    logger.info(f"  📁 Output directory: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()