#!/usr/bin/env python3
"""
Work Processor using Kimi API with JSON Mode.

This script processes individual work files and extracts structured JSON data
using Kimi's JSON mode API for guaranteed valid JSON output.
"""

import json
import os
import re
import time
import glob
import requests
from openai import OpenAI
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('work_processor_kimi.log')
    ]
)
logger = logging.getLogger(__name__)

# Set up Kimi client
client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),  # Changed to Kimi API key
    base_url="https://api.moonshot.ai/v1",       # Changed to Kimi API endpoint
    timeout=180,
)

# Google Search API credentials
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID")

# Directories and files
INDIVIDUAL_WORKS_DIR = "individual_works"
ORIGINAL_CHUNKS_DIR = "chunks_structured"
OUTPUT_DIR = "works_json"
PROCESSED_LOG = "processed_individual_works_json.txt"
WORK_TO_CHUNK_MAP = "work_to_chunk_map.json"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load the list of already processed files
processed_files = set()
if os.path.exists(PROCESSED_LOG):
    with open(PROCESSED_LOG, 'r') as f:
        processed_files = set(line.strip() for line in f)

# Load or create work to chunk mapping
work_to_chunk_map = {}
if os.path.exists(WORK_TO_CHUNK_MAP):
    with open(WORK_TO_CHUNK_MAP, 'r') as f:
        work_to_chunk_map = json.load(f)

# Enhanced system prompt optimized for Kimi JSON mode
kimi_system_prompt = """
You are an expert musicologist assistant. You will extract structured information about musical works from catalogue entries.

You must output ONLY a valid JSON object with the following structure:

{
  "Name": "Work title",
  "Composer": "Composer name", 
  "Source/Collection": "Source or collection name",
  "Publication Year": "YYYY or DD.MM.YYYY format",
  "First Performance": "YYYY or DD.MM.YYYY format",
  "Duration": "Duration in minutes",
  "Availability": "Availability status",
  "Link to Score": "URL to score if available",
  "links": ["array", "of", "relevant", "links"],
  "Status": "Status information",
  "Notes": "Additional notes",
  "Genre": "Chamber Music|Choral|Opera|Orchestral|Solo|Vocal",
  "SubGenre": "Specific subgenre from the allowed list",
  "Period": "Medieval|Renaissance|Baroque|Classical|Romantic|20th Century|Contemporary|unknown",
  "Instrumentation": ["array", "of", "instruments", "in", "English"],
  "Scoring": "Available scoring as referenced in source",
  "Related Works": ["array", "of", "related", "works"],
  "Long Description": "Detailed description",
  "Short Description": "Brief description", 
  "tags": ["array", "of", "descriptive", "tags"],
  "Catalog Number": "BWV, K, Op., fue, etc.",
  "ISMN": "International Standard Music Number",
  "OCLC": "WorldCat identifier",
  "ISWC": "International Standard Musical Work Code",
  "publisher": "Publisher name",
  "name of source": "Name of the source catalogue"
}

IMPORTANT RULES:
1. Use empty string "" for unknown text fields
2. Use empty array [] for unknown array fields  
3. SubGenre must be one of: Anthem, Aria, Bagatelle, Ballet, Canon, Cantata, Chaconne, Children's Opera, Chorale, Chorale Prelude, Comic Opera, Concerto, Concerto grosso, Dance, Divertimento, Divisions, Drame, Duet, Duo, Ensemble, Etude, Fantasia, Fugue, Grand Opera, Hymn, Impromptu, Incidental Music, Instrumental, Intermezzo, Lieder, Madrigal, Masque, Mass, Mazurka, Melodie, Minuet, Monody, Motet, Opera, Opera Buffa, Opera Seria, Oratorio, Overture, Partita, Passacaglia, Passion, Piano trio, Polonaise, Prelude, Quartet, Quintet, Requiem, Ricercar, Scherzo, Semi-opera, Serenade, Sinfonia, Singspiel, Small Mixed Ensemble, Sonata, Songs, Stylus Fantasticus, Suite, Symphonic Poem, Symphony, Toccata, Tone Poem, Trio, Trio Sonata, Unknown, Zarzuela
4. Extract information from both the short work description AND the larger catalogue context
5. Never guess information - use empty values if uncertain
6. Output ONLY the JSON object with no additional text or explanations
"""

def read_text_file(file_path):
    """Read the content of a text file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()

def build_work_to_chunk_map():
    """Creates a mapping from each work file to its original chunk file."""
    logger.info("Building work to chunk mapping...")
    
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.txt")) + \
                 glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.md"))
    chunk_files = glob.glob(os.path.join(ORIGINAL_CHUNKS_DIR, "*.txt")) + \
                  glob.glob(os.path.join(ORIGINAL_CHUNKS_DIR, "*.md"))
    
    mapping = {}
    
    # Read all chunk files into memory
    chunk_contents = {}
    for chunk_file in chunk_files:
        chunk_filename = os.path.basename(chunk_file)
        chunk_contents[chunk_filename] = read_text_file(chunk_file)
    
    # For each work file, find which chunk file it came from
    for work_file in work_files:
        work_filename = os.path.basename(work_file)
        full_work_content = read_text_file(work_file).strip()

        # Use only the last paragraph for matching
        paragraphs = [p.strip() for p in full_work_content.split("\n\n") if p.strip()]
        if not paragraphs:
            continue
        work_content = paragraphs[-1]

        # Skip very short content
        if len(work_content) < 10:
            continue
        
        # Try to find a match in the chunks
        matched_chunk = None
        for chunk_filename, chunk_content in chunk_contents.items():
            if work_content in chunk_content:
                matched_chunk = chunk_filename
                break
        
        if matched_chunk:
            mapping[work_filename] = matched_chunk
    
    # Save the mapping to a file
    with open(WORK_TO_CHUNK_MAP, 'w') as f:
        json.dump(mapping, f, indent=2)
    
    logger.info(f"Created mapping for {len(mapping)} work files to their original chunks")
    return mapping

def extract_work_info_kimi(work_text, work_filename, chunk_context):
    """Use Kimi API with JSON mode to extract work information."""
    
    # Create user prompt with context
    user_prompt = (
        f"Extract structured information from this musical work entry.\n\n"
        f"FILE NAME: {work_filename}\n\n"
        f"SHORT WORK DESCRIPTION:\n{work_text}\n\n"
        f"LARGER CATALOGUE CONTEXT:\n{chunk_context}\n\n"
        f"Extract information about the specific work described in the SHORT WORK DESCRIPTION. "
        f"Use the larger context to fill in missing details but do not guess. "
        f"Output the information in the specified JSON format."
    )
    
    messages = [
        {"role": "system", "content": kimi_system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"  Sending request to Kimi API (attempt {retry_count + 1}/{max_retries})...")
            start_time = time.time()
            
            response = client.chat.completions.create(
                model="kimi-k2-0711-preview",  # Use Kimi model
                messages=messages,
                response_format={"type": "json_object"},  # Enable JSON mode
                temperature=0.3,
                max_tokens=4000,  # Set reasonable token limit
                timeout=120
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"  Kimi API response received in {elapsed_time:.2f} seconds")
            
            # Parse the JSON response - Kimi guarantees valid JSON in json_object mode
            try:
                work_data = json.loads(response.choices[0].message.content)
                
                # Validate that we got the expected structure
                required_fields = ["Name", "Composer", "Genre"]
                if all(field in work_data for field in required_fields):
                    logger.info("  ✅ Valid work data extracted")
                    return work_data
                else:
                    missing = [f for f in required_fields if f not in work_data]
                    logger.warning(f"  ⚠ Missing required fields: {missing}")
                    if retry_count == max_retries - 1:
                        return work_data  # Return partial data on last attempt
                
            except json.JSONDecodeError as json_err:
                logger.error(f"  JSON parsing error: {json_err}")
                logger.error(f"  Raw response: {response.choices[0].message.content[:200]}...")
                
                # This shouldn't happen with Kimi JSON mode, but handle it
                if retry_count == max_retries - 1:
                    return None
                
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(2)
        
        except Exception as e:
            logger.error(f"  Error during Kimi API request: {type(e).__name__}: {e}")
            retry_count += 1
            if retry_count == max_retries:
                return None
            logger.info(f"  Retrying in {retry_count * 5} seconds...")
            time.sleep(retry_count * 5)
    
    return None

def is_meaningful_query(query):
    """Check if search query contains meaningful terms."""
    tokens = re.findall(r'\b\w+\b', query)
    return any(not token.isdigit() for token in tokens)

def search_for_ismn(work_data):
    """Search for ISMN and other identifiers using Google Search API."""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.warning("Google Search API credentials not found. Skipping ISMN search.")
        return work_data
    
    search_terms = []
    
    ismn = work_data.get("ISMN", "")
    catalog_number = work_data.get("Catalog Number", "")
    name = work_data.get("Name", "")
    composer = work_data.get("Composer", "")
    
    # Build search terms
    if ismn and ismn not in ["", "979-0-50012-332-3"]:
        search_terms.append(f"ISMN {ismn}")
    
    if catalog_number and catalog_number not in ["", "BWV 846", "fue 5320"]:
        if composer:
            search_terms.append(f"{composer} {catalog_number}")
        else:
            search_terms.append(catalog_number)
    
    if not search_terms and composer and name:
        search_terms.append(f"{composer} {name} sheet music")
    
    if not search_terms:
        logger.info("  No identifiers found for search. Skipping.")
        return work_data
    
    # Perform searches
    found_links = []
    for term in search_terms:
        if not is_meaningful_query(term):
            logger.info(f"  Skipping meaningless query: {term}")
            continue

        try:
            logger.info(f"  Searching for: {term}")
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': term,
                'num': 5
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                logger.warning(f"  Search API error: {response.status_code}")
                continue

            results = response.json()

            if 'items' in results:
                for item in results['items']:
                    title = item.get('title', '')
                    link = item.get('link', '')

                    relevant_terms = [
                        'score', 'sheet music', 'publication', 'music library',
                        'imslp', 'petrucci', 'musescore', 'musicnotes',
                        'edition', 'partitur', 'manuscript', 'facsimile',
                        'boosey', 'breitkopf', 'schott', 'ricordi',
                        'worldcat', 'naxos', 'catalogue', 'catalog',
                        'digital library', 'archive.org', 'public domain'
                    ]

                    is_relevant = any(term.lower() in title.lower() or term.lower() in link.lower() 
                                    for term in relevant_terms)

                    if is_relevant and link not in found_links:
                        found_links.append(link)

        except Exception as e:
            logger.error(f"  Error during search: {type(e).__name__}: {e}")
    
    # Add links to work data
    if found_links:
        logger.info(f"  Found {len(found_links)} relevant links")
        
        # Update Link to Score if empty
        if not work_data.get("Link to Score"):
            work_data["Link to Score"] = found_links[0]
            
        # Add all links
        work_data["links"] = found_links
    else:
        logger.info("  No relevant links found")
    
    return work_data

def check_kimi_api_connection():
    """Test the Kimi API connection."""
    logger.info("Testing Kimi API connection...")
    
    try:
        response = client.chat.completions.create(
            model="kimi-k2-0711-preview",
            messages=[
                {"role": "system", "content": "You are a helpful assistant. Respond only with valid JSON."},
                {"role": "user", "content": "Reply with a JSON object containing only the field 'status' with value 'connected'."}
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
            logger.error(f"✗ Kimi API responded but with unexpected content: {result}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Kimi API connection failed: {type(e).__name__}: {e}")
        return False

def process_work_files():
    """Process all individual work files and generate structured JSON data."""
    global work_to_chunk_map
    if not work_to_chunk_map:
        work_to_chunk_map = build_work_to_chunk_map()
    
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.txt"))
    files_to_process = [f for f in work_files if os.path.basename(f) not in processed_files]
    total_files = len(files_to_process)
    
    if total_files == 0:
        logger.info("No new files to process.")
        return 0
    
    logger.info(f"Found {total_files} files to process")
    
    def backup_processed_files():
        backup_file = f"{PROCESSED_LOG}.backup"
        with open(backup_file, 'w') as f:
            for filename in processed_files:
                f.write(f"{filename}\n")
    
    start_time_total = time.time()
    processed_count = 0
    successful_count = 0
    
    try:
        for i, work_file in enumerate(files_to_process):
            work_filename = os.path.basename(work_file)
            
            if work_filename in processed_files:
                logger.info(f"Skipping already processed file: {work_filename}")
                continue
            
            processed_count += 1
            
            # Progress tracking
            elapsed_time = time.time() - start_time_total
            if processed_count > 1:
                avg_time_per_file = elapsed_time / processed_count
                est_time_remaining = avg_time_per_file * (total_files - processed_count)
                est_completion = time.strftime("%H:%M:%S", time.gmtime(time.time() + est_time_remaining))
                logger.info(f"[{processed_count}/{total_files}] Processing: {work_filename}")
                logger.info(f"Progress: {processed_count/total_files*100:.1f}% | Est. completion at: {est_completion}")
            else:
                logger.info(f"[{processed_count}/{total_files}] Processing: {work_filename}")
            
            try:
                # Read work content
                work_text = read_text_file(work_file)
                logger.info(f"  Read file: {len(work_text)} characters")
                
                # Find original chunk for context
                chunk_filename = work_to_chunk_map.get(work_filename)
                chunk_text = "[No additional context was available.]"
                
                if chunk_filename:
                    chunk_file_path = os.path.join(ORIGINAL_CHUNKS_DIR, chunk_filename)
                    if os.path.exists(chunk_file_path):
                        logger.info(f"  Found original chunk: {chunk_filename}")
                        chunk_text = read_text_file(chunk_file_path)
                    else:
                        logger.warning(f"  Original chunk file not found: {chunk_filename}")
                else:
                    # Try to rebuild mapping for this file
                    temp_map = build_work_to_chunk_map()
                    chunk_filename = temp_map.get(work_filename)
                    if chunk_filename:
                        chunk_file_path = os.path.join(ORIGINAL_CHUNKS_DIR, chunk_filename)
                        if os.path.exists(chunk_file_path):
                            chunk_text = read_text_file(chunk_file_path)
                
                # Extract work data using Kimi API
                work_data = extract_work_info_kimi(work_text, work_filename, chunk_text)
                
                if work_data:
                    # Search for ISMN and add links
                    try:
                        logger.info("  Searching for ISMN and related links...")
                        work_data = search_for_ismn(work_data)
                    except Exception as search_err:
                        logger.warning(f"  Error during search: {type(search_err).__name__}: {search_err}")
                    
                    # Generate output filename
                    work_name = work_data.get("Name", "").strip()
                    composer = work_data.get("Composer", "").strip()
                    
                    if work_name and composer:
                        out_filename = f"{composer}_{work_name}"
                    elif work_name:
                        out_filename = work_name
                    elif composer:
                        out_filename = f"{composer}_unknown_work_{i+1}"
                    else:
                        out_filename = os.path.splitext(work_filename)[0]
                    
                    # Clean the filename
                    out_filename = re.sub(r'[^\w\s-]', '', out_filename)
                    out_filename = re.sub(r'\s+', '_', out_filename.strip())
                    out_filename = out_filename[:100]  # Limit length
                    
                    # Handle duplicates
                    final_filename = f"{out_filename}.json"
                    out_path = os.path.join(OUTPUT_DIR, final_filename)

                    if os.path.exists(out_path):
                        logger.info(f"  Merging with existing file: {final_filename}")
                        try:
                            with open(out_path, 'r', encoding='utf-8') as f:
                                existing_data = json.load(f)
                        except Exception as e:
                            logger.error(f"  Failed to read existing JSON: {e}")
                            existing_data = {}

                        # Simple merge strategy - prefer existing non-empty values
                        merged_data = {}
                        all_keys = set(existing_data.keys()).union(set(work_data.keys()))
                        
                        for key in all_keys:
                            old_val = existing_data.get(key, "")
                            new_val = work_data.get(key, "")

                            if isinstance(old_val, list) and isinstance(new_val, list):
                                merged_data[key] = sorted(list(set(old_val + new_val)))
                            elif isinstance(old_val, str) and isinstance(new_val, str):
                                merged_data[key] = old_val if old_val else new_val
                            else:
                                merged_data[key] = old_val if old_val else new_val

                        work_data = merged_data
                    
                    # Save the JSON file
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(work_data, f, indent=2, ensure_ascii=False)

                    logger.info(f"  ✅ Saved work data to {out_path}")
                    successful_count += 1
                else:
                    logger.error(f"  ✗ Failed to extract work data for {work_filename}")
                
                # Mark as processed
                processed_files.add(work_filename)
                
                # Update processed log
                with open(PROCESSED_LOG, 'w') as f:
                    for filename in processed_files:
                        f.write(f"{filename}\n")
                
                # Create backup every 5 files
                if processed_count % 5 == 0:
                    backup_processed_files()
            
            except Exception as e:
                logger.error(f"  Error processing {work_filename}: {type(e).__name__}: {e}")
        
        total_time = time.time() - start_time_total
        logger.info(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info(f"Successfully processed {successful_count} out of {processed_count} files")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user!")
        logger.info(f"Processed {processed_count} out of {total_files} files")
        logger.info(f"Successfully processed {successful_count} files")
        backup_processed_files()
        logger.info("Progress saved. Restart script to continue.")
    
    return successful_count

def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("MUSICAL WORKS JSON PROCESSOR (KIMI API JSON MODE)")
    logger.info("=" * 60)
    
    logger.info("This script will:")
    logger.info("1. Process each individual work text file")
    logger.info("2. Use original chunk files for additional context")
    logger.info("3. Extract structured information using Kimi API JSON mode")
    logger.info("4. Search for ISMNs and add relevant links")
    logger.info("5. Save results as JSON files in the output directory")
    
    # Check required API key
    if not os.environ.get("MOONSHOT_API_KEY"):
        logger.error("MOONSHOT_API_KEY not found in environment variables.")
        logger.error("Please add your Kimi API key to the .env file.")
        return
    
    # Check Google API credentials
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.warning("Google Search API credentials not found.")
        logger.warning("Set GOOGLE_API_KEY and GOOGLE_CSE_ID in .env file to enable link searching.")
        
        proceed = input("Proceed without Google Search functionality? (y/n): ")
        if proceed.lower() != 'y':
            logger.info("Exiting script.")
            return
    
    # Check directories
    if not os.path.exists(INDIVIDUAL_WORKS_DIR):
        logger.error(f"Input directory '{INDIVIDUAL_WORKS_DIR}' does not exist.")
        logger.error("Please run the work splitter script first.")
        return
    
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.txt"))
    if not work_files:
        logger.error(f"No .txt files found in '{INDIVIDUAL_WORKS_DIR}'.")
        logger.error("Please run the work splitter script first.")
        return
    
    if not os.path.exists(ORIGINAL_CHUNKS_DIR):
        logger.error(f"Original chunks directory '{ORIGINAL_CHUNKS_DIR}' not found.")
        logger.error("This script requires original chunks for context.")
        return
    
    # Test API connection
    if not check_kimi_api_connection():
        logger.error("Kimi API connection test failed.")
        logger.error("Check your MOONSHOT_API_KEY and internet connection.")
        return
    
    logger.info("Starting to process work files...")
    files_processed = process_work_files()
    
    logger.info("=" * 60)
    if files_processed > 0:
        logger.info(f"✅ Processing completed! Successfully processed {files_processed} work files.")
        logger.info(f"Results saved in the '{OUTPUT_DIR}' directory.")
    else:
        logger.error("❌ Processing completed, but no files were successfully processed.")
        logger.error("Check the error messages above for more information.")
    logger.info("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()