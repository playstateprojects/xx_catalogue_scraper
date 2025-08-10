# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a classical music works processing pipeline that extracts and processes composer works from PDF catalogues to create structured data and text chunks for a Retrieval-Augmented Generation (RAG) system. The pipeline handles everything from PDF processing through to cloud storage upload.

## Core Processing Pipeline

The system follows this main workflow:

1. **Document Processing**: Upload PDF catalogues to GCP bucket, extract text using Google Document AI
2. **JSON to Markdown**: Convert extracted JSON to markdown chunks for readability
3. **Work Identification**: Use LLM-powered splitting to identify individual musical works within chunks
4. **Metadata Enrichment**: Match composers to IDs and add structured metadata
5. **Cloud Upload**: Upload processed works to Cloudflare R2 storage

## Key Python Scripts

### Primary Processing Scripts
- `batch_process.py` - Processes PDFs using Google Document AI, outputs JSON
- `json2md_splitter.py` - Converts Document AI JSON to markdown chunks (3500 char max)
- `work_chunk_splitter.py` - Uses DeepSeek LLM to identify individual works within chunks
- `upload_works_to_r2.py` - Uploads processed works to Cloudflare R2 with metadata

### Data Processing Utilities
- `works_json_to_csv.py` - Converts work JSON files to CSV format with composer ID mapping
- `enhanced_work_proccessor_b.py` - Alternative work processor (unused in main pipeline)
- `layout_to_md.py` - Converts layout JSON to markdown
- `add_id_to_csv.py` - Utility to add IDs to CSV files

## Required Environment Variables

Create a `.env` file with these variables:

```
# DeepSeek API (for LLM work splitting)
DEEPSEEK_KEY=your_deepseek_api_key

# Cloudflare R2 Storage
R2_ACCESS_KEY_ID=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key
R2_ACCOUNT_ID=your_r2_account_id
R2_BUCKET=composer-data
```

Google Cloud credentials are configured separately via service account JSON.

## Key Data Files

- `composer_names.csv` - Maps composer names to unique IDs
- `work_to_chunk_map.json` - Maps individual works to their source chunks
- `upload_log.jsonl` - Tracks successfully uploaded works to avoid duplicates
- `unmatched_composers.log` - Lists composer names that couldn't be matched to IDs

## Directory Structure

- `catalogues/` - Source PDF files and Document AI JSON outputs
- `chunks_structured/` - Markdown chunks from JSON conversion
- `individual_works/` - Individual work files split from chunks
- `works_json/` - JSON files for individual works with metadata
- `JSON_FOR_DB/` - Database-ready JSON exports

## Common Development Tasks

### Processing New PDF Catalogues
```bash
# 1. Upload PDFs to GCP bucket, then run:
python batch_process.py

# 2. Convert JSON to markdown chunks:
python json2md_splitter.py

# 3. Split chunks into individual works:
python work_chunk_splitter.py

# 4. Convert to JSON with metadata:
python works_json_to_csv.py

# 5. Upload to cloud storage:
python upload_works_to_r2.py
```

### Resume Processing
The pipeline supports interruption and resumption. Progress is tracked in:
- `processed_chunks.txt` - Tracks processed chunk files
- `upload_log.jsonl` - Tracks uploaded works

## Dependencies

Install required packages:
```bash
pip install google-cloud-documentai google-cloud-storage openai python-dotenv pandas boto3
```

## LLM Integration

The system uses DeepSeek API for intelligent work splitting. The `work_chunk_splitter.py` script:
- Sends chunk text to LLM with structured prompt
- Parses LLM response to extract individual works
- Handles retries and rate limiting
- Filters works by composer presence and content quality

## Cloud Architecture

- **Google Cloud**: Document AI for PDF processing, Cloud Storage for intermediate files
- **Cloudflare R2**: Final storage for processed works with metadata
- **DeepSeek API**: LLM processing for work identification and splitting

The pipeline is designed to handle large catalogues (1000+ works) with robust error handling and progress tracking.