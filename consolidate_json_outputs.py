#!/usr/bin/env python3
"""
Consolidate scattered Document AI JSON outputs into a single directory.

This script finds all JSON files in nested Document AI output folders and 
copies them to a consolidated location with meaningful names.
"""

import os
import re
import logging
from pathlib import Path
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def consolidate_document_ai_outputs(
    bucket_name: str, 
    source_prefix: str, 
    dest_prefix: str,
    dry_run: bool = False
):
    """
    Consolidate scattered Document AI JSON files into a single directory.
    
    Args:
        bucket_name: GCS bucket name
        source_prefix: Source prefix where scattered files are located
        dest_prefix: Destination prefix for consolidated files
        dry_run: If True, only show what would be done without copying
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Find all JSON files in the source prefix
    blobs = list(bucket.list_blobs(prefix=source_prefix))
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
    
    if not json_blobs:
        logger.warning(f"No JSON files found with prefix: {source_prefix}")
        return 0
    
    logger.info(f"Found {len(json_blobs)} JSON files to consolidate")
    
    consolidated_count = 0
    
    for blob in json_blobs:
        try:
            # Generate consolidated filename
            consolidated_name = generate_consolidated_name(blob.name, source_prefix, dest_prefix)
            
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Consolidating:")
            logger.info(f"  From: {blob.name}")
            logger.info(f"  To:   {consolidated_name}")
            
            if not dry_run:
                # Check if destination already exists
                dest_blob = bucket.blob(consolidated_name)
                if dest_blob.exists():
                    logger.warning(f"  Destination already exists, skipping: {consolidated_name}")
                    continue
                
                # Copy the blob
                bucket.copy_blob(blob, bucket, consolidated_name)
                logger.info(f"  ✅ Copied successfully")
            
            consolidated_count += 1
            
        except Exception as e:
            logger.error(f"Error consolidating {blob.name}: {e}")
    
    action = "Would consolidate" if dry_run else "Consolidated"
    logger.info(f"{action} {consolidated_count} JSON files")
    
    return consolidated_count

def generate_consolidated_name(original_path: str, source_prefix: str, dest_prefix: str) -> str:
    """
    Generate a consolidated filename from the original nested path.
    
    Example transformation:
    outputs/annotated-catalog/1234567890/0/document-0.json
    -> outputs/consolidated/document-0_op1234567890_file0.json
    """
    # Remove the source prefix
    relative_path = original_path
    if original_path.startswith(source_prefix):
        relative_path = original_path[len(source_prefix):]
    
    # Split path into parts
    parts = relative_path.strip('/').split('/')
    
    if len(parts) >= 3:
        # Expected structure: operation_id/file_id/filename.json
        operation_id = parts[0]
        file_id = parts[1]
        filename = parts[-1]
        
        # Create a descriptive consolidated name
        base_name = filename.replace('.json', '')
        consolidated_filename = f"{base_name}_op{operation_id}_file{file_id}.json"
        
    elif len(parts) >= 2:
        # Fallback for different structure
        folder_parts = parts[:-1]
        filename = parts[-1]
        base_name = filename.replace('.json', '')
        folder_suffix = '_'.join(folder_parts)
        consolidated_filename = f"{base_name}_{folder_suffix}.json"
        
    else:
        # Single level, just use the filename
        consolidated_filename = parts[0]
    
    return dest_prefix + consolidated_filename

def main():
    """Main execution function."""
    # Configuration
    bucket_name = os.getenv('GCS_BUCKET_NAME', 'xx_catalogues')
    source_prefix = os.getenv('GCS_SOURCE_PREFIX', 'outputs/annotated-catalog/')
    dest_prefix = os.getenv('GCS_DEST_PREFIX', 'outputs/consolidated/')
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    logger.info("Document AI JSON Consolidation Tool")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Source prefix: {source_prefix}")
    logger.info(f"Destination prefix: {dest_prefix}")
    logger.info(f"Dry run: {dry_run}")
    
    if not source_prefix.endswith('/'):
        source_prefix += '/'
    if not dest_prefix.endswith('/'):
        dest_prefix += '/'
    
    try:
        count = consolidate_document_ai_outputs(
            bucket_name=bucket_name,
            source_prefix=source_prefix,
            dest_prefix=dest_prefix,
            dry_run=dry_run
        )
        
        if dry_run:
            logger.info(f"✅ Dry run completed. {count} files would be consolidated.")
            logger.info("Set DRY_RUN=false to actually perform the consolidation.")
        else:
            logger.info(f"✅ Consolidation completed. {count} files consolidated.")
            
    except Exception as e:
        logger.error(f"Consolidation failed: {e}")
        raise

if __name__ == "__main__":
    main()