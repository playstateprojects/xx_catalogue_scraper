#!/usr/bin/env python3
"""
Simple Markdown to JSON Processor using Kimi API JSON Mode.

This script reads markdown files from markdown_documents directory and 
converts them directly into structured JSON objects using Kimi's JSON mode.
"""

import json
import os
import glob
import time
import logging
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
        logging.FileHandler('markdown_to_json.log')
    ]
)
logger = logging.getLogger(__name__)

# Set up Kimi client
client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),
    base_url="https://api.moonshot.ai/v1",
    timeout=600,  # 10 minutes for large document processing
)

# Directories
INPUT_DIR = "markdown_documents"
OUTPUT_DIR = "kimi_works_json"
OUTPUT_FILE = "all_works.json"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# System prompt for Kimi JSON mode
system_prompt = """
You are an expert musicologist. You will analyze markdown documents containing classical music catalogue information and extract structured data about ALL musical works found in the document.

CRITICAL: You must extract EVERY SINGLE musical work mentioned in the document. Do not stop at just one work.

You must output ONLY a valid JSON object with this structure:
{
  "works": [
    {work1},
    {work2},
    {work3},
    ...all works found...
  ]
}

Each work object in the "works" array must have this exact structure:

{
  "Name": "Work title",
  "Composer": "Composer name", 
  "Source/Collection": "Source or collection name",
  "Publication Year": "YYYY or DD.MM.YYYY format",
  "First Performance": "YYYY or DD.MM.YYYY format", 
  "Duration": "Duration in minutes",
  "Availability": "Availability status",
  "Link to Score": "URL to score if available",
  "links": [],
  "Status": "Status information",
  "Notes": "Additional notes",
  "Genre": "Chamber Music|Choral|Opera|Orchestral|Solo|Vocal",
  "SubGenre": "Specific subgenre from allowed list",
  "Period": "Medieval|Renaissance|Baroque|Classical|Romantic|20th Century|Contemporary|unknown",
  "Instrumentation": [],
  "Scoring": "Available scoring as referenced in source",
  "Related Works": [],
  "Long Description": "Detailed description",
  "Short Description": "Brief description",
  "tags": [],
  "Catalog Number": "BWV, K, Op., fue, etc.",
  "ISMN": "International Standard Music Number",
  "OCLC": "WorldCat identifier",
  "ISWC": "International Standard Musical Work Code",
  "publisher": "Publisher name",
  "name of source": "Name of the source catalogue"
}

RULES:
1. Output ONLY the JSON object with "works" array - no explanations or additional text
2. Use empty string "" for unknown text fields
3. Use empty array [] for unknown array fields
4. SubGenre must be one of: Anthem, Aria, Bagatelle, Ballet, Canon, Cantata, Chaconne, Children's Opera, Chorale, Chorale Prelude, Comic Opera, Concerto, Concerto grosso, Dance, Divertimento, Divisions, Drame, Duet, Duo, Ensemble, Etude, Fantasia, Fugue, Grand Opera, Hymn, Impromptu, Incidental Music, Instrumental, Intermezzo, Lieder, Madrigal, Masque, Mass, Mazurka, Melodie, Minuet, Monody, Motet, Opera, Opera Buffa, Opera Seria, Oratorio, Overture, Partita, Passacaglia, Passion, Piano trio, Polonaise, Prelude, Quartet, Quintet, Requiem, Ricercar, Scherzo, Semi-opera, Serenade, Sinfonia, Singspiel, Small Mixed Ensemble, Sonata, Songs, Stylus Fantasticus, Suite, Symphonic Poem, Symphony, Toccata, Tone Poem, Trio, Trio Sonata, Unknown, Zarzuela
5. MANDATORY: Extract EVERY musical work mentioned in the document - scan the entire document thoroughly
6. Never guess - use empty values for uncertain information
7. Look for work titles, composer names, opus numbers, catalog numbers throughout the document
8. Include works mentioned in tables, lists, headings, and body text
"""

def read_markdown_file(file_path):
    """Read markdown file content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return None

def process_markdown_with_kimi(markdown_content, filename):
    """Process markdown content using Kimi API to extract work data."""
    
    user_prompt = f"""
    TASK: Extract ALL musical works from this markdown document. Read through the ENTIRE document carefully.
    
    Document filename: {filename}
    Document length: {len(markdown_content)} characters
    
    Document content:
    {markdown_content}
    
    INSTRUCTIONS:
    - Scan the entire document from top to bottom
    - Extract every musical work mentioned (titles, compositions, pieces, songs, etc.)
    - Look in headings, tables, lists, paragraphs, and any other text
    - Create a separate JSON object for each distinct work
    - If a document mentions 50 works, extract all 50 works
    - Do not stop after finding just one work
    
    Return a JSON object with a "works" array containing ALL works found.
    """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"  Processing with Kimi API (attempt {attempt + 1}/{max_retries})...")
            
            response = client.chat.completions.create(
                model="kimi-k2-0711-preview",
                messages=messages,
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=64000,  # Maximum possible tokens
                timeout=480  # 8 minutes per request
            )
            
            # Parse JSON response
            response_text = response.choices[0].message.content.strip()
            
            # Handle case where Kimi returns JSON object with works array
            try:
                parsed = json.loads(response_text)
                
                # Extract works from the response
                if isinstance(parsed, dict):
                    if "works" in parsed:
                        works = parsed["works"]
                    else:
                        # If it's a single work object, wrap it
                        works = [parsed]
                elif isinstance(parsed, list):
                    works = parsed
                else:
                    logger.error(f"  Unexpected response format: {type(parsed)}")
                    continue
                
                # Validate works have required fields
                valid_works = []
                for work in works:
                    if isinstance(work, dict) and work.get("Name") and work.get("Composer"):
                        valid_works.append(work)
                    else:
                        logger.warning(f"  Skipping invalid work object: {work}")
                
                if valid_works:
                    logger.info(f"  ✅ Extracted {len(valid_works)} valid works")
                    logger.info(f"  Works found: {[w.get('Name', 'Unnamed') for w in valid_works[:5]]}")  # Show first 5
                    return valid_works
                else:
                    logger.warning("  No valid works found in response")
                    logger.warning(f"  Raw works count: {len(works) if works else 0}")
                    if works:
                        logger.warning(f"  First work sample: {works[0] if works else 'None'}")
                    
            except json.JSONDecodeError as e:
                logger.error(f"  JSON decode error: {e}")
                logger.error(f"  Response text: {response_text[:200]}...")
                
        except Exception as e:
            logger.error(f"  API request failed: {e}")
            
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 2
            logger.info(f"  Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    logger.error("  ❌ Failed to process after all retries")
    return []

def test_kimi_connection():
    """Test Kimi API connection."""
    logger.info("Testing Kimi API connection...")
    
    try:
        response = client.chat.completions.create(
            model="kimi-k2-0711-preview",
            messages=[
                {"role": "system", "content": "Output only valid JSON."},
                {"role": "user", "content": "Return a JSON object with 'status': 'connected'"}
            ],
            response_format={"type": "json_object"},
            max_tokens=50,
            timeout=30
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
    """Main processing function."""
    logger.info("=" * 60)
    logger.info("MARKDOWN TO JSON PROCESSOR (KIMI API)")
    logger.info("=" * 60)
    
    # Check API key
    if not os.environ.get("MOONSHOT_API_KEY"):
        logger.error("MOONSHOT_API_KEY not found in environment variables.")
        logger.error("Please add your Kimi API key to the .env file.")
        return
    
    # Check input directory
    if not os.path.exists(INPUT_DIR):
        logger.error(f"Input directory '{INPUT_DIR}' does not exist.")
        return
    
    # Find markdown files
    markdown_files = glob.glob(os.path.join(INPUT_DIR, "*.md"))
    if not markdown_files:
        logger.error(f"No .md files found in '{INPUT_DIR}'.")
        return
    
    logger.info(f"Found {len(markdown_files)} markdown files to process")
    
    # Test API connection
    if not test_kimi_connection():
        logger.error("API connection test failed. Please check your credentials.")
        return
    
    # Process all markdown files
    all_works = []
    successful_files = 0
    
    for i, md_file in enumerate(markdown_files):
        filename = os.path.basename(md_file)
        logger.info(f"[{i+1}/{len(markdown_files)}] Processing: {filename}")
        
        # Read markdown content
        content = read_markdown_file(md_file)
        if not content:
            logger.error(f"  Failed to read file: {filename}")
            continue
        
        logger.info(f"  File size: {len(content):,} characters")
        
        # Skip very small files
        if len(content.strip()) < 100:
            logger.warning(f"  Skipping file - too small: {filename}")
            continue
        
        # Process with Kimi
        works = process_markdown_with_kimi(content, filename)
        
        if works:
            # Add source filename to each work
            for work in works:
                work["source_filename"] = filename
            
            all_works.extend(works)
            successful_files += 1
            logger.info(f"  ✅ Added {len(works)} works from {filename}")
        else:
            logger.error(f"  ❌ No works extracted from {filename}")
        
        # Small delay between files
        if i < len(markdown_files) - 1:
            time.sleep(1)
    
    # Save results
    if all_works:
        # Save as single JSON array
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_works, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info(f"✅ Processing completed!")
        logger.info(f"   Files processed: {successful_files}/{len(markdown_files)}")
        logger.info(f"   Total works extracted: {len(all_works)}")
        logger.info(f"   Output saved to: {output_path}")
        
        # Save individual JSON files as well
        individual_count = 0
        for work in all_works:
            try:
                composer = work.get("Composer", "Unknown").strip()
                name = work.get("Name", "Untitled").strip()
                
                # Create filename
                if composer and name:
                    filename = f"{composer}_{name}"
                else:
                    filename = f"work_{individual_count}"
                
                # Clean filename
                import re
                filename = re.sub(r'[^\w\s-]', '', filename)
                filename = re.sub(r'\s+', '_', filename)[:100]
                
                individual_path = os.path.join(OUTPUT_DIR, f"{filename}.json")
                
                # Handle duplicates
                counter = 1
                original_path = individual_path
                while os.path.exists(individual_path):
                    base = original_path.replace('.json', '')
                    individual_path = f"{base}_{counter}.json"
                    counter += 1
                
                with open(individual_path, 'w', encoding='utf-8') as f:
                    json.dump(work, f, indent=2, ensure_ascii=False)
                
                individual_count += 1
                
            except Exception as e:
                logger.error(f"Error saving individual work: {e}")
        
        logger.info(f"   Individual files saved: {individual_count}")
        logger.info("=" * 60)
        
    else:
        logger.error("❌ No works were extracted from any files.")
        logger.error("Check the log messages above for details.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()