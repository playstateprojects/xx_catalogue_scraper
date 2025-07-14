import json
import os
import re
import time
import glob
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Set up OpenAI client
client = OpenAI(
    api_key=os.environ.get("DEEPSEEK_KEY"),
    base_url="https://api.deepseek.com",
    timeout=180,  # 3 minute timeout for the entire client
)

# Directories and files
INDIVIDUAL_WORKS_DIR = "individual_works"  # Directory with individual work txt files
ORIGINAL_CHUNKS_DIR = "chunks"             # Directory with original chunk files
OUTPUT_DIR = "works_json"                  # Output directory for JSON files
PROCESSED_LOG = "processed_individual_works.txt"
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

# System prompt for OpenAI
system_prompt = """
You are an expert musicologist assistant. Your task is to extract structured information about a musical work from the text I provide, which is sourced from catalogues.

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
    "Status": "Complete",
    "Notes": "One of the most famous works in classical music",
    "Genre": "one of 'Chamber Music', 'Choral', 'Opera', 'Orchestral', 'Solo', 'Vocal'",
    "Period": "one of 'Medieval', 'Renaissance', 'Baroque', 'Classical', 'Early Romantic', 'Late Romantic', '20th Century', 'Contemporary', 'unknown'",
    "Instrumentation": ["of the following items: 'SATB', 'SATTB', 'String Trio', 'String Quartet', 'String Quintett', 'Piano Trio', 'Piano Quartet', 'Vocal Ensemble', 'Orchestra', 'Baroque orchestra', 'String Orchestra', 'Double Orchestra', 'Accordion', 'Alto (treble) recorder', 'Alto flute', 'Arpeggione', 'Alto saxophone', 'Alto voice', 'Alto soloist', 'Bagpipes', 'Baritone saxophone', 'Baritone voice', 'Baritone soloist', 'Bass clarinet', 'Bass drum', 'Bass flute', 'Bass guitar', 'Bass instrument', 'Bass recorder', 'Bass trombone', 'Bass Viola da Gamba', 'Bass voice', 'Bass soloist', 'Basset horn', 'Bassoon', 'Bodhrán', 'Bongos', 'Cantor', 'Carillon', 'Celeste', 'Cello', 'Cimbalon', 'Clarinet', 'Claves', 'Clavichord', 'Congregation', 'Contra bass clarinet', 'Cor Anglais', 'Cornet', 'Counter tenor voice', 'Counter tenor soloist', 'Crotales', 'Cymbal', 'Divisi', 'Double bass', 'Double bassoon', 'Drum kit', 'E flat clarinet', 'Electric Flute', 'Electric guitar', 'Electric organ', 'Electronics', 'Euphonium', 'Female voice', 'Flugel horn', 'Flute', 'Gamelan', 'Glockenspiel', 'Guitar', 'Harmonium', 'Harp', 'Harpsichord', 'High voice', 'Horn', 'Irish harp', 'Keyboard', 'Live electronics', 'Low voice', 'Lute', 'Male voice', 'Mandolin', 'Marimba', 'Medium voice', 'Melodica', 'Mezzo soprano voice', 'Mezzo soprano soloist', 'Number', 'Obbligato', 'Oboe', 'Oboe d\\'amore', 'Orchestra, orchestral', 'Organ', 'Percussion', 'Piano', 'Piccolo', 'Pipes', 'Quartet', 'Quintet', 'Recorder', 'Revised', 'Saxophone', 'Sean nós singer', 'Side drum', 'Soprano (descant) recorder', 'Soprano saxophone', 'Soprano voice', 'Soprano soloist', 'Speaker', 'Strings', 'Synthesiser', 'Tambourine', 'Tape', 'Tenor drum', 'Tenor horn', 'Tenor recorder', 'Tenor saxophone', 'Tenor voice', 'Tenor soloist', 'Timpani', 'Tin whistle', 'Tom toms', 'Traditional flute', 'Traditional violin or fiddle', 'Traditional voice', 'Treble instrument', 'Trombone', 'Trumpet', 'Tuba', 'Tubular bells', 'Uilleann pipes', 'Vibraphone', 'Viola', 'Viola d\\'amore', 'Viola da Gamba', 'Violin', 'Voice', 'Woodwind', 'Xylophone"],
    "Related Works": ["recRelatedWork1"],
    "Long Description": "<div>This is a rich text description of Symphony No. 5...</div>",
    "Short Description": "A famous symphony by Beethoven",
    "tags": ["descriptive property not covered elsewhere"]
}

Focus on accuracy:
1. If a field cannot be determined, use an empty string or appropriate empty array
2. Make reasonable deductions based on the text but do not invent information
3. For dates, use DD.MM.YYYY format when full dates are available, or just YYYY when only the year is known
4. When the composer is not explicitly stated, try to infer it from the text using:
   - Relationships with other works
   - Catalog numbers (e.g., BWV for Bach, K for Mozart, Op for many composers)
   - Style period references
   - Any other contextual clues

Your output must be ONLY the valid JSON object with no additional text or explanations.
"""

# System prompt for the enhanced context lookup
enhanced_system_prompt = """
You are an expert musicologist assistant. I'm providing you with two pieces of text:
1. A short description of a musical work that may have limited information
2. A larger context from a music catalogue that contains information about multiple works, including the one in question

Your task is to extract structured information about the specific work mentioned in the first text, using both texts as sources.

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
    "Status": "Complete",
    "Notes": "One of the most famous works in classical music",
    "Genre": "one of 'Chamber Music', 'Choral', 'Opera', 'Orchestral', 'Solo', 'Vocal'",
    "Period": "one of 'Medieval', 'Renaissance', 'Baroque', 'Classical', 'Early Romantic', 'Late Romantic', '20th Century', 'Contemporary', 'unknown'",
    "Instrumentation": "A string providing as much detail on instrumentation as possible with out adding new assumptions",
    "Related Works": ["related work 1"],
    "Long Description": "<div>This is a rich text description of Symphony No. 5...</div>",
    "Short Description": "A famous symphony by Beethoven",
    "tags": ["descriptive property not covered elsewhere"],
    "Catalog Number": "for example fue 5320",
    "ISMN": " for example 979-0-50012-332-3",
    "publisher": "always use Furore"
}

Focus on accuracy:
1. Only extract information about the specific work mentioned in the first text
2. Use the larger context to fill in missing details, especially the composer
3. If a field cannot be determined, use an empty string or appropriate empty array
4. Make reasonable deductions based on the text but do not invent information
5. For dates, use DD.MM.YYYY format when full dates are available, or just YYYY when only the year is known

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
        work_content = read_text_file(work_file).strip()
        
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

def extract_work_info(work_text, work_filename, use_enhanced=False, chunk_context=None):
    """Use OpenAI API to extract work information."""
    if use_enhanced and chunk_context:
        # Use the enhanced prompt with both work text and chunk context
        user_prompt = f"SHORT WORK DESCRIPTION:\n\n{work_text}\n\nLARGER CATALOGUE CONTEXT:\n\n{chunk_context}\n\nExtract the information about the specific work described in the SHORT WORK DESCRIPTION."
        prompt_to_use = enhanced_system_prompt
    else:
        # Use the standard prompt with just the work text
        user_prompt = f"Here is information about a musical work:\n\n{work_text}\n\nExtract the information into the JSON structure."
        prompt_to_use = system_prompt
    
    messages = [
        {"role": "system", "content": prompt_to_use},
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
                
                # First try processing with just the work text
                work_data = extract_work_info(work_text, work_filename)
                
                # Check if composer information is missing or empty
                if not work_data or not work_data.get("Composer") or work_data.get("Composer") == "":
                    print("  Composer information missing, trying to find original chunk context...")
                    
                    # Try to find the original chunk this work came from
                    chunk_filename = work_to_chunk_map.get(work_filename)
                    
                    if chunk_filename:
                        chunk_file_path = os.path.join(ORIGINAL_CHUNKS_DIR, chunk_filename)
                        
                        if os.path.exists(chunk_file_path):
                            print(f"  Found original chunk: {chunk_filename}")
                            
                            # Read the chunk content
                            chunk_text = read_text_file(chunk_file_path)
                            
                            # Try processing again with enhanced context
                            work_data = extract_work_info(
                                work_text, 
                                work_filename,
                                use_enhanced=True,
                                chunk_context=chunk_text
                            )
                        else:
                            print(f"  Original chunk file not found: {chunk_filename}")
                    else:
                        print("  Could not find matching original chunk")
                
                if work_data:
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
                    counter = 1
                    final_filename = f"{out_filename}.json"
                    out_path = os.path.join(OUTPUT_DIR, final_filename)
                    
                    while os.path.exists(out_path):
                        final_filename = f"{out_filename}_{counter}.json"
                        out_path = os.path.join(OUTPUT_DIR, final_filename)
                        counter += 1
                    
                    # Save work data as JSON
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(work_data, f, indent=2, ensure_ascii=False)
                    
                    print(f"  Saved work data to {out_path}")
                else:
                    print(f"  Failed to extract data for {work_filename}")
                
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
                print(f"  Error processing {work_filename}: {type(e).__name__}: {e}")
                # Continue to next file
            
            # Add a delay to avoid rate limiting
            if i < len(files_to_process) - 1:
                delay = 3  # Delay between requests
                print(f"  Waiting {delay} seconds before processing next file...")
                time.sleep(delay)
        
        total_time = time.time() - start_time_total
        print(f"\nTotal processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user!")
        print(f"Processed {processed_count} out of {total_files} files before interruption")
        backup_processed_files()
        print(f"Progress saved. You can restart the script to continue from where you left off.")
    
    return processed_count

def check_api_connection():
    """Test the API connection before starting the full process."""
    print("Testing API connection...")
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
            print("✓ API connection successful!")
            return True
        else:
            print(f"✗ API responded but with unexpected content: {response.choices[0].message.content}")
            return False
    except Exception as e:
        print(f"✗ API connection failed: {type(e).__name__}: {e}")
        return False

def main():
    print("\n" + "="*60)
    print(" MUSICAL WORKS JSON PROCESSOR ")
    print("="*60)
    
    print("\nThis script will:")
    print("1. Process each individual work text file")
    print("2. Use original chunk files for additional context when needed")
    print("3. Extract structured information about each work")
    print("4. Save results as JSON files in the output directory")
    
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
        print(f"\n⚠ Warning: Original chunks directory '{ORIGINAL_CHUNKS_DIR}' not found.")
        print("Will process without original context. This might result in less complete data.")
    
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
        print(f"✓ Processing completed! Processed {files_processed} work files.")
        print(f"  Results saved in the '{OUTPUT_DIR}' directory.")
    else:
        print("✗ Processing completed, but no files were processed.")
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
