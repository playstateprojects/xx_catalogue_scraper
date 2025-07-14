import json
import os
import re
import time
import glob
import requests
from openai import OpenAI
from dotenv import load_dotenv
import difflib

load_dotenv()

# Set up OpenAI client
client = OpenAI(
    api_key=os.environ.get("DEEPSEEK_KEY"),
    base_url="https://api.deepseek.com",
    timeout=180,  # 3 minute timeout for the entire client
)

# Google Search API credentials
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID")  # Custom Search Engine ID

# Directories and files
INDIVIDUAL_WORKS_DIR = "individual_works"  # Directory with individual work txt files
ORIGINAL_CHUNKS_DIR = "chunks_structured"             # Directory with original chunk files
OUTPUT_DIR = "works_json"                  # Output directory for JSON files
PROCESSED_LOG = "processed_individual_works_json.txt"
WORK_TO_CHUNK_MAP = "work_to_chunk_map.json"  # Maps each work file to its original chunk

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

# Enhanced system prompt that always uses context
enhanced_system_prompt = """
You are an expert musicologist assistant. I will provide you with:

A short description of a musical work that may have limited information

Optionally, a longer context from a music catalogue which might contain information about the same work

Your task is to extract structured information only about the specific work described in the short description. 
Use the longer context if available to enrich your answer — especially to confirm the composer, instrumentation, or source — but do not guess if data is missing or ambiguous.

Create a complete and accurate JSON object with the following structure:
{
    "Name": "Symphony No. 5",
    "Composer": "composer name",
    "Source/Collection": "Beethoven's Complete Symphonies",
    "Publication Year": "1808",
    "First Performance": "1808-12-22",
    "Duration": "time to perform",
    "Availability": "Published",
    "Link to Score": "https://example.com/beethoven-symphony5",
    "links": ["other relevant links extracted from google"],
    "Status": "Complete",
    "Notes": "One of the most famous works in classical music",
    "Genre": "one of 'Chamber Music', 'Choral', 'Opera', 'Orchestral', 'Solo', 'Vocal'",
    "SubGenre": "one of Anthem, Aria, Bagatelle, Ballet, Canon, Cantata, Chaconne, Children's Opera, Chorale, Chorale Prelude, Comic Opera, Concerto, Concerto grosso, Dance, Divertimento, Divisions, Drame, Duet, Duo, Ensemble, Etude, Fantasia, Fugue, Grand Opera, Hymn, Impromptu, Incidental Music, Instrumental, Intermezzo, Lieder, Madrigal, Masque, Mass, Mazurka, Melodie, Minuet, Monody, Motet, Opera, Opera Buffa, Opera Seria, Oratorio, Overture, Partita, Passacaglia, Passion, Piano trio, Polonaise, Prelude, Quartet, Quintet, Requiem, Ricercar, Scherzo, Semi-opera, Serenade, Sinfonia, Singspiel, Small Mixed Ensemble, Sonata, Songs, Stylus Fantasticus, Suite, Symphonic Poem, Symphony, Toccata, Tone Poem, Trio, Trio Sonata, Unknown, Zarzuela",
    "Period": "one of 'Medieval', 'Renaissance', 'Baroque', 'Classical', 'Romantic', '20th Century', 'Contemporary', 'unknown',
    "Instrumentation": ["instrumentation in english and not abreviated if possible."],
    "Related Works": ["related work 1"],
    "Long Description": "This is a rich text description",
    "Short Description": "A famous symphony by Beethoven",
    "tags": ["descriptive property not covered elsewhere"],
    "Catalog Number": "for example BWV 846 or fue 5320",
    "ISMN": "for example 979-0-50012-332-3",
    "publisher": "",
    "name of source":"the filename osuplied by the user."
}

Focus on accuracy:
1. Only extract information about the specific work mentioned in the first text
2. Use the larger context to fill in missing details, especially the composer
3. If a field cannot be determined, use an empty string or appropriate empty array
4. Make reasonable deductions based on the text but do not invent information
5. For dates, use DD.MM.YYYY format when full dates are available, or just YYYY when only the year is known
6. Pay special attention to any catalog numbers (BWV, K, Op., fue, etc.) and put them in the "Catalog Number" field
7. If you find an ISMN, put it in the "ISMN" field
9. Instrumentation must be accurate, comprehensive, and translated to English.
Use the following abbreviation reference when decoding instruments:
Abkürzungen * Abbreviations

A | Alt/Altistin alto | M-St | Männerstimme | male voice
--- | --- | --- | --- | ---
A-Bfl | Altblockflöte alto recorder | Mar | Marimbaphon | marimba
A-Sax | Altsaxophon alto saxophone | MCh | Männerchor | men's choir
Akk | accordion Akkordeon | Mel-Instr | Melodieinstrument | melody instrument
B | bass Bass | Ms mSt | Mezzosopran mittlere Stimme | mezzo soprano medium voice
B-Klar | Bassklarinette bass clarinet |  |  | 
B.c. Bar | basso continuo Basso continuo baritone Bariton | Ob Orch | Oboe Orchester | oboe orchestra
Bar-Sax Bfl | baritone saxophone Baritonsaxophon Blockflöte recorder | Org | Orgel | organ
Bsn | bassoon Fagott | P | Partitur | score
Cemb CP CS E-Bass E-Git Engl-Hn Ens  FCh F-St Fg | harpsichord Cembalo Chorpartitur choir score choir score Chorpartitur electric bass Elektrobass electric guitar Elektrogitarre english horn Englischhorn Ensemble ensemble  women's choir Frauenchor female voice Frauenstimme bassoon Fagott | Perc Picc Pos ps S S Sax Schlzg Singst Spr St st | Schlaginstrumente Piccolo Posaune Klavierauszug Sopran/Sopranistin Partitur Saxophon Schlagzeug Singstimme Sprecher/in Stimmen -stimmig | percussion instruments score /voice number of part(s) piccolo trombone piano soprano score saxophone drums voice speaker parts
Fl | (transverse) flute Querflöte | Str | Streicher | strings
 |  | StrQu | Streichquartett | string quartet
GCh gem | mixed choir gemischter Chor gemischt(er) mixed | Synth | Synthesizer | synthesizer
Git | Gitarre guitar | T | Tenor | tenor
Glksp | glockenspiel Glockenspiel | Tb | Tuba | tuba
 |  | Timp | Pauke | timpani
h.m. | Mietmaterial hire material | Trp | Trompete | trumpet
Hn | French horn Horn, Waldhorn | tSt | tiefe Stimme | low voice
Hrf | harp Harfe |  |  | 
hSt | high voice hohe Stimme | Vc Vga | Violoncello Viola da gamba | violoncello viola da gamba
ΚΑ | Klavierauszug piano score | Vibr | Vibraphon | vibraphone
Kb | double bass Kontrabass | VI | Violine | violin
Keyb | keyboard Keyboard | Va | Viola | viola
KiCh | children's choir Kinderchor |  |  | 
kl. | small kleines |  |  | 
Klar | clarinet Klarinette |  |  | 
Klav | piano Klavier |  |  | 

Your output must be ONLY the valid JSON object with no additional text or explanations.
"""

def read_text_file(file_path):
    """Read the content of a text file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except UnicodeDecodeError:
        # Try with a different encoding if UTF-8 fails
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()

def build_work_to_chunk_map():
    """Creates a mapping from each work file to its original chunk file.
    This is done by searching for text matches between work files and chunk files."""
    print("Building work to chunk mapping...")
    
     # Get all individual work files and chunk files
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.{txt,md}"))
    chunk_files = glob.glob(os.path.join(ORIGINAL_CHUNKS_DIR, "*.{txt,md}"))
    
    # Initialize the mapping
    mapping = {}
    
    # Read all chunk files into memory (this could be inefficient for very large datasets)
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
        work_content = paragraphs[-1]  # Last non-empty paragraph

        # Skip very short content (it might match many chunks)
        if len(work_content) < 10:
            continue
        
      
        # Try to find a match in the chunks
        matched_chunk = None
        for chunk_filename, chunk_content in chunk_contents.items():
            # Check if the work content is a substring of the chunk content
            if work_content in chunk_content:
                matched_chunk = chunk_filename
                break
        
        if matched_chunk:
            mapping[work_filename] = matched_chunk
    
    # Save the mapping to a file
    with open(WORK_TO_CHUNK_MAP, 'w') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"Created mapping for {len(mapping)} work files to their original chunks")
    return mapping

def extract_work_info(work_text, work_filename, chunk_context):
    """Use OpenAI API to extract work information with context."""
    # Always use the enhanced prompt with both work text and chunk context
    user_prompt = (
        f"FILE NAME: {work_filename}\n\n"
        f"SHORT WORK DESCRIPTION:\n\n{work_text}\n\n"
        f"LARGER CATALOGUE CONTEXT:\n\n{chunk_context}\n\n"
        f"Extract the information about the specific work described in the SHORT WORK DESCRIPTION."
    )
    messages = [
        {"role": "system", "content": enhanced_system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"  Sending request to LLM (attempt {retry_count + 1}/{max_retries})...")
            start_time = time.time()
            
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                response_format={"type": "json_object"},
                temperature=0.3,
                timeout=120  # 2 minute timeout
            )
            
            elapsed_time = time.time() - start_time
            print(f"  LLM response received in {elapsed_time:.2f} seconds")
            
            # Parse the JSON response
            try:
                work_data = json.loads(response.choices[0].message.content)
                return work_data
            except json.JSONDecodeError as json_err:
                print(f"  JSON parsing error: {json_err}")
                print(f"  Raw response: {response.choices[0].message.content[:200]}...")
                retry_count += 1
                if retry_count == max_retries:
                    return None
                time.sleep(2)  # Wait before retrying
        
        except Exception as e:
            print(f"  Error during LLM request: {type(e).__name__}: {e}")
            retry_count += 1
            if retry_count == max_retries:
                return None
            print(f"  Retrying in {retry_count * 5} seconds...")
            time.sleep(retry_count * 5)  # Increasing backoff
    
    return None

def is_meaningful_query(query):
    tokens = re.findall(r'\b\w+\b', query)
    return any(not token.isdigit() for token in tokens)

def search_for_ismn(work_data):
    """
    Search for ISMN and other identifiers using Google Search API
    and add relevant links to the work data.
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("  ⚠ Google Search API credentials not found. Skipping ISMN search.")
        return work_data
    
    # Extract identifiers to search for
    search_terms = []
    
    # Try to get ISMN if explicitly stated
    ismn = work_data.get("ISMN", "")
    catalog_number = work_data.get("Catalog Number", "")
    name = work_data.get("Name", "")
    composer = work_data.get("Composer", "")
    
    # If we have an ISMN, use it for search
    if ismn and ismn != "for example 979-0-50012-332-3":
        search_terms.append(f"ISMN {ismn}")
    
    # If we have a catalog number, use it for search
    if catalog_number and catalog_number != "for example BWV 846 or fue 5320":
        if composer:
            search_terms.append(f"{composer} {catalog_number}")
        else:
            search_terms.append(catalog_number)
    
    # If no ISMN or catalog number found, try composer and name
    if not search_terms and composer and name:
        search_terms.append(f"{composer} {name} sheet music")
    
    # If we have no search terms, skip search
    if not search_terms:
        print("  ⚠ No identifiers found for search. Skipping.")
        return work_data
    
    # Perform searches
    found_links = []
    for term in search_terms:
        if not is_meaningful_query(term):
            print(f"  ⚠ Skipping meaningless query (just numbers): {term}")
            continue

        try:
            print(f"  Searching for: {term}")
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': term,
                'num': 5  # Number of results to return
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"  ⚠ Search API error: {response.status_code}")
                continue

            results = response.json()

            # Extract relevant links
            if 'items' in results:
                for item in results['items']:
                    title = item.get('title', '')
                    link = item.get('link', '')

                    # Updated relevant terms
                    relevant_terms = [
                        'score', 'sheet music', 'publication', 'music library',
                        'imslp', 'petrucci', 'musescore', 'musicnotes',
                        'edition', 'partitur', 'manuscript', 'facsimile',
                        'musikdruck', 'ueberlieferung', 'druck', 'urtext', 'verlag',
                        'boosey', 'breitkopf', 'schott', 'ricordi', 'allegro',
                        'worldcat', 'naxos', 'arkiv', 'catalogue', 'catalog',
                        'digital library', 'archive.org', 'public domain'
                    ]

                    is_relevant = any(term.lower() in title.lower() or term.lower() in link.lower() 
                                    for term in relevant_terms)

                    if is_relevant and link not in found_links:
                        found_links.append(link)

        except Exception as e:
            print(f"  ⚠ Error during search: {type(e).__name__}: {e}")
    
    # Add links to work data if found
    if found_links:
        print(f"  Found {len(found_links)} relevant links")
        
        # Update Link to Score if it's empty
        if not work_data.get("Link to Score") or work_data.get("Link to Score") == "https://example.com/beethoven-symphony5":
            work_data["Link to Score"] = found_links[0]
            
        # Add all links to a new property
        work_data["links"] = found_links
    else:
        print("  No relevant links found")
    
    return work_data

def check_api_connection():
    """Test the API connection before starting the full process."""
    print("Testing API connection...")
    
    # Test DeepSeek API
    deepseek_ok = False
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Reply with just the word 'connected' if you receive this."}
            ],
            max_tokens=10,
            timeout=30
        )
        if "connect" in response.choices[0].message.content.lower():
            print("✓ DeepSeek API connection successful!")
            deepseek_ok = True
        else:
            print(f"✗ DeepSeek API responded but with unexpected content: {response.choices[0].message.content}")
    except Exception as e:
        print(f"✗ DeepSeek API connection failed: {type(e).__name__}: {e}")
    
    # Test Google Search API if credentials exist
    google_ok = False
    if GOOGLE_API_KEY and GOOGLE_CSE_ID:
        try:
            print("Testing Google Search API connection...")
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': 'test',
                'num': 1
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                print("✓ Google Search API connection successful!")
                google_ok = True
            else:
                print(f"✗ Google Search API error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            print(f"✗ Google Search API connection failed: {type(e).__name__}: {e}")
    else:
        print("⚠ Google Search API credentials not found, will skip link search")
        google_ok = True  # Skip Google API check if credentials aren't provided
    
    return deepseek_ok

def process_work_files():
    """Process all individual work files and generate structured JSON data."""
    # Check if we need to build the work-to-chunk mapping
    global work_to_chunk_map
    if not work_to_chunk_map:
        work_to_chunk_map = build_work_to_chunk_map()
    
    # Get all work files
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.txt"))
    
    # Count total unprocessed files for progress tracking
    files_to_process = [f for f in work_files if os.path.basename(f) not in processed_files]
    total_files = len(files_to_process)
    
    if total_files == 0:
        print("No new files to process.")
        return 0
    
    print(f"Found {total_files} files to process")
    
    # Create a backup of processed files periodically
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
                print(f"Skipping already processed file: {work_filename}")
                continue
            
            processed_count += 1
            
            # Calculate progress and estimate time remaining
            elapsed_time = time.time() - start_time_total
            if processed_count > 1:  # Need at least one file processed to estimate
                avg_time_per_file = elapsed_time / processed_count
                est_time_remaining = avg_time_per_file * (total_files - processed_count)
                est_completion = time.strftime("%H:%M:%S", time.gmtime(time.time() + est_time_remaining))
                print(f"\n[{processed_count}/{total_files}] Processing: {work_filename}")
                print(f"Progress: {processed_count/total_files*100:.1f}% | Est. completion at: {est_completion}")
            else:
                print(f"\n[{processed_count}/{total_files}] Processing: {work_filename}")
            
            try:
                # Read work content
                work_text = read_text_file(work_file)
                print(f"  Read file: {len(work_text)} characters")
                
                # Try to find the original chunk this work came from
                chunk_filename = work_to_chunk_map.get(work_filename)
                
                if not chunk_filename:
                    print(f"  ⚠ No matching chunk found for {work_filename}, searching...")
                    
                    # If we don't have a mapping for this file, try to create one
                    temp_map = build_work_to_chunk_map()
                    chunk_filename = temp_map.get(work_filename)
                
                if chunk_filename:
                    chunk_file_path = os.path.join(ORIGINAL_CHUNKS_DIR, chunk_filename)
                    
                    if os.path.exists(chunk_file_path):
                        print(f"  Found original chunk: {chunk_filename}")
                        
                        # Read the chunk content
                        chunk_text = read_text_file(chunk_file_path)
                        
                        # Process with context
                        work_data = extract_work_info(work_text, work_filename, chunk_text)
                    else:
                        print(f"  ✗ Original chunk file not found: {chunk_filename}")
                        work_data = None
                else:
                    print("  ✗ Could not find matching original chunk — continuing with work text only")
                    work_data = extract_work_info(work_text, work_filename, "[No additional context was available.]")
                
                if work_data:
                    # Make sure publisher is set to Furore if it's empty
                   
                            
                    # Search for ISMN and add links
                    try:
                        print("  Searching for ISMN and related links...")
                        work_data = search_for_ismn(work_data)
                    except Exception as search_err:
                        print(f"  ⚠ Error during search: {type(search_err).__name__}: {search_err}")
                    
                    # Generate output filename (use work name and composer if available)
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
                    
                    # Ensure the filename is not too long
                    if len(out_filename) > 100:
                        out_filename = out_filename[:100]
                    
                    # Add a numeric suffix if a file with this name already exists
                    final_filename = f"{out_filename}.json"
                    out_path = os.path.join(OUTPUT_DIR, final_filename)

                    merge_log = []

                    if os.path.exists(out_path):
                        print(f"  ⚠ Duplicate detected. Attempting to merge with existing: {final_filename}")
                        try:
                            with open(out_path, 'r', encoding='utf-8') as f:
                                existing_data = json.load(f)
                        except Exception as e:
                            print(f"  ✗ Failed to read existing JSON: {e}")
                            existing_data = {}

                        # Merge fields
                        merged_data = {}
                        all_keys = set(existing_data.keys()).union(set(work_data.keys()))
                        for key in all_keys:
                            old_val = existing_data.get(key, "")
                            new_val = work_data.get(key, "")

                            if isinstance(old_val, list) and isinstance(new_val, list):
                                combined = sorted(list(set(old_val + new_val)))
                                if combined != old_val:
                                    merge_log.append(f"    • Field '{key}' extended from {old_val} → {combined}")
                                merged_data[key] = combined

                            elif isinstance(old_val, str) and isinstance(new_val, str):
                                if not old_val and new_val:
                                    merge_log.append(f"    • Field '{key}' was empty, updated to: '{new_val}'")
                                    merged_data[key] = new_val
                                else:
                                    merged_data[key] = old_val  # preserve existing
                            else:
                                # fallback: prefer existing non-empty
                                merged_data[key] = old_val if old_val else new_val

                        work_data = merged_data

                        if merge_log:
                            print("  📝 Merge log:")
                            for line in merge_log:
                                print(line)
                        else:
                            print("  ✓ No changes made during merge.")
                # Save merged or new work_data to JSON
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(work_data, f, indent=2, ensure_ascii=False)

                print(f"  ✓ Saved work data to {out_path}")
                successful_count += 1
                # Mark as processed regardless of success (to avoid getting stuck)
                processed_files.add(work_filename)
                
                # Update processed log
                with open(PROCESSED_LOG, 'w') as f:
                    for filename in processed_files:
                        f.write(f"{filename}\n")
                
                # Create backup every 5 files
                if processed_count % 5 == 0:
                    backup_processed_files()
            
            except Exception as e:
                print(f"  ✗ Error processing {work_filename}: {type(e).__name__}: {e}")
                # Continue to next file
            
            # Removed delay between processing files to speed up execution
            pass
        
        total_time = time.time() - start_time_total
        print(f"\nTotal processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print(f"Successfully processed {successful_count} out of {processed_count} files")
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user!")
        print(f"Processed {processed_count} out of {total_files} files before interruption")
        print(f"Successfully processed {successful_count} files")
        backup_processed_files()
        print(f"Progress saved. You can restart the script to continue from where you left off.")
    
    return successful_count

def main():
    print("\n" + "="*60)
    print(" MUSICAL WORKS JSON PROCESSOR (ALWAYS USE CONTEXT) ")
    print("="*60)
    
    print("\nThis script will:")
    print("1. Process each individual work text file")
    print("2. Always use original chunk files for additional context")
    print("3. Extract structured information about each work")
    print("4. Search for ISMNs and add relevant links")
    print("5. Save results as JSON files in the output directory")
    
    # Check Google API credentials
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("\n⚠ Warning: Google Search API credentials not found in environment variables.")
        print("  Set GOOGLE_API_KEY and GOOGLE_CSE_ID in your .env file to enable ISMN searching.")
        print("  The script will still process works without searching for links.")
        
        proceed = input("\nDo you want to proceed without Google Search functionality? (y/n): ")
        if proceed.lower() != 'y':
            print("Exiting script.")
            return
    
    # Check if there are files to process
    if not os.path.exists(INDIVIDUAL_WORKS_DIR):
        print(f"\n✗ Input directory '{INDIVIDUAL_WORKS_DIR}' does not exist.")
        print("Please run the work splitter script first to generate individual work files.")
        return
    
    work_files = glob.glob(os.path.join(INDIVIDUAL_WORKS_DIR, "*.txt"))
    if not work_files:
        print(f"\n✗ No .txt files found in '{INDIVIDUAL_WORKS_DIR}' directory.")
        print("Please run the work splitter script first to generate individual work files.")
        return
    
    # Check if original chunks directory exists
    if not os.path.exists(ORIGINAL_CHUNKS_DIR):
        print(f"\n✗ Original chunks directory '{ORIGINAL_CHUNKS_DIR}' not found.")
        print("This script requires the original chunks for context. Please create this directory with the original files.")
        return
    
    # Test API connection
    if not check_api_connection():
        print("\nTroubleshooting tips:")
        print("1. Check your DEEPSEEK_KEY in the .env file")
        print("2. Verify your internet connection")
        print("3. Check if the DeepSeek API is currently available")
        return
    
    print("\nStarting to process work files and generate JSON data...")
    files_processed = process_work_files()
    
    print("\n" + "="*60)
    if files_processed > 0:
        print(f"✓ Processing completed! Successfully processed {files_processed} work files.")
        print(f"  Results saved in the '{OUTPUT_DIR}' directory.")
    else:
        print("✗ Processing completed, but no files were successfully processed.")
        print("  Check the error messages above for more information.")
    print("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nScript terminated by user.")
    except Exception as e:
        print(f"\n\nUnexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()