import os
import re
import time
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
INPUT_DIR = "chunks_structured"
OUTPUT_DIR = "individual_works"
PROCESSED_LOG = "processed_chunks.txt"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load the list of already processed files
processed_files = set()
if os.path.exists(PROCESSED_LOG):
    with open(PROCESSED_LOG, 'r') as f:
        processed_files = set(line.strip() for line in f)

# System prompt for identifying and splitting works
system_prompt = """
You are an expert musicologist assistant. Your task is to identify individual musical works within a larger text that contains information about multiple works.

The text is sourced from music catalogues and may describe multiple compositions by one or more composers.

For each distinct musical work you identify, you will:
1. Extract the complete text related to that work only
2. Identify a title and composer if possible

Focus on accuracy:
1. Make sure each extracted text contains information about only ONE distinct musical work
2. Include ALL information about that work from the original text
3. Do not split sections that refer to the same work
4. Make sure the boundaries between works are correctly identified

For your response format, for each work you identify, output:

===WORK_START===
TITLE: [Work Title if identifiable, otherwise "Unknown Work"]
COMPOSER: [Composer name if identifiable, otherwise "Unknown Composer"]
---CONTENT---
[Complete text describing only this single work]
===WORK_END===

Use exactly this format with these exact markers so they can be parsed programmatically.
Repeat this format for each separate work you identify in the text.
"""

def read_text_file(file_path):
    """Read the content of a text file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def identify_separate_works(chunk_text, filename):
    """Use LLM to identify separate works within the chunk text."""
    user_prompt = f"Here is a section from a musical catalogue that contains information about multiple works:\n\n{chunk_text}\n\nIdentify each distinct musical work and extract the text for each one separately using the specified format."
    
    messages = [
        {"role": "system", "content": system_prompt},
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
                temperature=0.3,
                timeout=120  # 2 minute timeout
            )
            
            elapsed_time = time.time() - start_time
            print(f"  LLM response received in {elapsed_time:.2f} seconds")
            
            # Get the raw text response instead of parsing as JSON
            works_text = response.choices[0].message.content
            
            # Find all works in the text using regex
            works_data = parse_works_from_text(works_text)
            
            if works_data and len(works_data) > 0:
                print(f"  Successfully identified {len(works_data)} works")
                return works_data
            else:
                print(f"  Warning: No works found in LLM response")
                print(f"  Raw response start: {works_text[:200]}...")
                retry_count += 1
                time.sleep(2)  # Wait before retrying
        
        except Exception as e:
            print(f"  Error during LLM request: {type(e).__name__}: {e}")
            retry_count += 1
            if retry_count == max_retries:
                print(f"  Failed after {max_retries} attempts")
                return None
            print(f"  Retrying in {retry_count * 5} seconds...")
            time.sleep(retry_count * 5)  # Increasing backoff
    
    return None

def parse_works_from_text(text):
    """Extract works from the LLM response using regex patterns."""
    works = []
    
    # Pattern to find work blocks
    work_pattern = r'===WORK_START===\s*TITLE:\s*(.*?)\s*COMPOSER:\s*(.*?)\s*---CONTENT---\s*(.*?)\s*===WORK_END==='
    
    # Find all matches
    matches = re.findall(work_pattern, text, re.DOTALL)
    
    for match in matches:
        title = match[0].strip()
        composer = match[1].strip()
        content = match[2].strip()
        
        works.append({
            "work_title": title if title and title != "Unknown Work" else "",
            "composer": composer if composer and composer != "Unknown Composer" else "",
            "work_text": content
        })
    
    return works

def save_individual_work(work_data, chunk_filename, work_index):
    """Save a single work's text to its own file."""
    # Generate a filename using work title and composer if available
    title = work_data.get("work_title", "").strip()
    composer = work_data.get("composer", "").strip()
    
    if title and composer:
        filename_base = f"{composer}_{title}"
    elif title:
        filename_base = title
    elif composer:
        filename_base = f"{composer}_work_{work_index}"
    else:
        # If neither title nor composer are available, use the chunk filename and work index
        filename_base = f"{os.path.splitext(chunk_filename)[0]}_work_{work_index}"
    
    # Clean the filename
    filename_base = re.sub(r'[^\w\s-]', '', filename_base)
    filename_base = re.sub(r'\s+', '_', filename_base.strip())
    
    # Ensure the filename is not too long
    if len(filename_base) > 100:
        filename_base = filename_base[:100]
    
    # Add a numeric suffix if a file with this name already exists
    counter = 1
    filename = f"{filename_base}.txt"
    file_path = os.path.join(OUTPUT_DIR, filename)
    
    while os.path.exists(file_path):
        filename = f"{filename_base}_{counter}.txt"
        file_path = os.path.join(OUTPUT_DIR, filename)
        counter += 1
    
    # Save the work text to the file
    work_text = work_data.get("work_text", "")
    composer = work_data.get("composer", "").strip()

    # Append composer name at the top if not already present
    if composer and composer.lower() not in work_text.lower():
        work_text = f"Composer: {composer}\n\n" + work_text

    if not work_text:
        print(f"  Warning: No work text found for {filename}")
        return None
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(work_text)
    
    print(f"  Saved work to {file_path}")
    return filename

def process_chunk_files():
    """Process all files in the input directory and split them into individual works."""
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".md") or f.endswith(".txt")]
    total_works_created = 0
    
    # Count total unprocessed files for progress tracking
    files_to_process = [f for f in files if f not in processed_files]
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
        for i, filename in enumerate(files):
            if filename in processed_files:
                print(f"Skipping already processed file: {filename}")
                continue
            
            file_path = os.path.join(INPUT_DIR, filename)
            processed_count += 1
            
            # Calculate progress and estimate time remaining
            elapsed_time = time.time() - start_time_total
            if processed_count > 1:  # Need at least one file processed to estimate
                avg_time_per_file = elapsed_time / processed_count
                est_time_remaining = avg_time_per_file * (total_files - processed_count)
                est_completion = time.strftime("%H:%M:%S", time.gmtime(time.time() + est_time_remaining))
                print(f"\n[{processed_count}/{total_files}] Processing: {filename}")
                print(f"Progress: {processed_count/total_files*100:.1f}% | Est. completion at: {est_completion}")
            else:
                print(f"\n[{processed_count}/{total_files}] Processing: {filename}")
            
            try:
                # Read file content
                try:
                    chunk_text = read_text_file(file_path)
                    print(f"  Read file: {len(chunk_text)} characters")
                except Exception as e:
                    print(f"  Error reading file {filename}: {type(e).__name__}: {e}")
                    continue
                
                # Identify separate works
                works_data = identify_separate_works(chunk_text, filename)
                
                if works_data and len(works_data) > 0:
                    print(f"  Found {len(works_data)} separate works in {filename}")
                    
                    # Save each work to its own file
                    works_saved = 0
                    for j, work_data in enumerate(works_data):
                        try:
                            saved_filename = save_individual_work(work_data, filename, j+1)
                            if saved_filename:
                                works_saved += 1
                        except Exception as save_err:
                            print(f"  Error saving work #{j+1}: {type(save_err).__name__}: {save_err}")
                    
                    print(f"  Successfully saved {works_saved} out of {len(works_data)} works from {filename}")
                    total_works_created += works_saved
                    
                    # Add file to processed list
                    processed_files.add(filename)
                    
                    # Update processed log (write as simple text file)
                    with open(PROCESSED_LOG, 'w') as f:
                        for processed_file in processed_files:
                            f.write(f"{processed_file}\n")
                    
                    # Create backup every 5 files
                    if processed_count % 5 == 0:
                        backup_processed_files()
                else:
                    print(f"  Failed to identify separate works in {filename} or no works found")
                    
                    # Even if we failed to identify works, mark as processed to avoid
                    # getting stuck on problematic files
                    processed_files.add(filename)
                    with open(PROCESSED_LOG, 'w') as f:
                        for processed_file in processed_files:
                            f.write(f"{processed_file}\n")
            
            except Exception as file_err:
                print(f"  Unexpected error processing {filename}: {type(file_err).__name__}: {file_err}")
                # Continue to next file
            
            # Add a delay to avoid rate limiting
            # if i < len(files) - 1:
            #     delay = 3  # Slightly longer delay
            #     print(f"  Waiting {delay} seconds before processing next file...")
            #     time.sleep(delay)
        
        total_time = time.time() - start_time_total
        print(f"\nTotal processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user!")
        print(f"Processed {processed_count} out of {total_files} files before interruption")
        backup_processed_files()
        print(f"Progress saved. You can restart the script to continue from where you left off.")
    
    return total_works_created

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
    print(" MUSICAL WORKS SPLITTER SCRIPT (SIMPLIFIED) ")
    print("="*60)
    
    print("\nThis script will:")
    print("1. Read files from the 'chunks' directory")
    print("2. Use DeepSeek LLM to identify individual musical works")
    print("3. Split each chunk into separate files (one work per file)")
    print("4. Save the results in the 'individual_works' directory")
    
    # Check if there are files to process
    if not os.path.exists(INPUT_DIR):
        print(f"\n✗ Input directory '{INPUT_DIR}' does not exist. Creating it...")
        os.makedirs(INPUT_DIR)
        print(f"Please add your chunk files to the '{INPUT_DIR}' directory and run the script again.")
        return
    
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".md") or f.endswith(".txt")]
    if not files:
        print(f"\n✗ No .md or .txt files found in '{INPUT_DIR}' directory.")
        print(f"Please add your chunk files to the '{INPUT_DIR}' directory and run the script again.")
        return
    
    # Test API connection
    if not check_api_connection():
        print("\nTroubleshooting tips:")
        print("1. Check your DEEPSEEK_KEY in the .env file")
        print("2. Verify your internet connection")
        print("3. Check if the DeepSeek API is currently available")
        return
    
    print("\nStarting to process chunks and split them into individual works...")
    works_created = process_chunk_files()
    
    print("\n" + "="*60)
    if works_created > 0:
        print(f"✓ Processing completed! Created {works_created} individual work files.")
        print(f"  Results saved in the '{OUTPUT_DIR}' directory.")
    else:
        print("✗ Processing completed, but no work files were created.")
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