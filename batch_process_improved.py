import os
import re
from typing import Optional
from pathlib import Path
import logging

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import InternalServerError, RetryError
from google.cloud import documentai
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('batch_process.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
CONFIG = {
    'project_id': os.getenv('GCP_PROJECT_ID', 'opus-xx'),
    'location': os.getenv('GCP_LOCATION', 'eu'),
    'processor_id': os.getenv('DOCUMENT_AI_PROCESSOR_ID', '94a82e4e9e57b04e'),
    'gcs_input_prefix': os.getenv('GCS_INPUT_PREFIX', 'gs://xx_catalogues/raw/'),
    'gcs_output_uri': os.getenv('GCS_OUTPUT_URI', 'gs://xx_catalogues/outputs/annotated-catalog/'),
    'input_mime_type': 'application/pdf',
    'timeout': int(os.getenv('PROCESSING_TIMEOUT', '600'))
}

def validate_config():
    """Validate that all required configuration is present."""
    required_vars = ['project_id', 'processor_id', 'gcs_input_prefix', 'gcs_output_uri']
    missing = [var for var in required_vars if not CONFIG[var]]
    
    if missing:
        raise ValueError(f"Missing required configuration: {missing}")
    
    # Validate GCS URIs
    if not CONFIG['gcs_input_prefix'].startswith('gs://'):
        raise ValueError("gcs_input_prefix must start with 'gs://'")
    
    if not CONFIG['gcs_output_uri'].startswith('gs://'):
        raise ValueError("gcs_output_uri must start with 'gs://'")
    
    if not CONFIG['gcs_output_uri'].endswith('/'):
        CONFIG['gcs_output_uri'] += '/'
    
    logger.info("Configuration validated successfully")
    logger.info(f"Input: {CONFIG['gcs_input_prefix']}")
    logger.info(f"Output: {CONFIG['gcs_output_uri']}")

def count_input_files():
    """Count PDF files in the input directory."""
    try:
        storage_client = storage.Client()
        
        # Parse bucket and prefix from GCS URI
        gcs_path = CONFIG['gcs_input_prefix'].replace('gs://', '')
        bucket_name = gcs_path.split('/')[0]
        prefix = '/'.join(gcs_path.split('/')[1:])
        
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        pdf_count = sum(1 for blob in blobs if blob.name.lower().endswith('.pdf'))
        
        logger.info(f"Found {pdf_count} PDF files to process")
        return pdf_count
    except Exception as e:
        logger.warning(f"Could not count input files: {e}")
        return 0

def batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_output_uri: str,
    gcs_input_prefix: str,
    input_mime_type: str = "application/pdf",
    processor_version_id: Optional[str] = None,
    field_mask: Optional[str] = None,
    timeout: int = 600,
) -> bool:
    """
    Process documents using Google Document AI.
    
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Set up client
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        client = documentai.DocumentProcessorServiceClient(client_options=opts)
        
        # Configure input
        gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_prefix)
        input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
        
        # Configure output
        gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=gcs_output_uri, field_mask=field_mask
        )
        output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)
        
        # Get processor path
        if processor_version_id:
            name = client.processor_version_path(
                project_id, location, processor_id, processor_version_id
            )
        else:
            name = client.processor_path(project_id, location, processor_id)
        
        # Create request
        request = documentai.BatchProcessRequest(
            name=name,
            input_documents=input_config,
            document_output_config=output_config,
        )
        
        logger.info("Starting batch processing operation...")
        operation = client.batch_process_documents(request)
        
        logger.info(f"Operation started: {operation.operation.name}")
        logger.info(f"Waiting up to {timeout} seconds for completion...")
        
        # Wait for completion with progress updates
        operation.result(timeout=timeout)
        
    except (RetryError, InternalServerError) as e:
        logger.error(f"Document AI processing failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        return False
    
    # Check operation status
    metadata = operation.metadata
    if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        logger.error(f"Batch processing failed: {metadata.state_message}")
        return False
    
    logger.info("Batch processing completed successfully")
    
    # Process and display results
    try:
        process_results(metadata)
        return True
    except Exception as e:
        logger.error(f"Error processing results: {e}")
        return False

def process_results(metadata):
    """Process and display the results of the batch operation."""
    storage_client = storage.Client()
    processed_count = 0
    consolidated_count = 0
    
    logger.info("Processing results...")
    
    # Parse the consolidated output location
    consolidated_uri = CONFIG['gcs_output_uri'] + 'consolidated/'
    matches = re.match(r"gs://(.*?)/(.*)", consolidated_uri)
    if not matches:
        logger.error(f"Invalid consolidated URI: {consolidated_uri}")
        return
    
    consolidated_bucket_name, consolidated_prefix = matches.groups()
    consolidated_bucket = storage_client.bucket(consolidated_bucket_name)
    
    for process in metadata.individual_process_statuses:
        logger.info(f"Processing output: {process.output_gcs_destination}")
        
        # Parse GCS destination
        matches = re.match(r"gs://(.*?)/(.*)", process.output_gcs_destination)
        if not matches:
            logger.warning(f"Could not parse GCS destination: {process.output_gcs_destination}")
            continue
        
        output_bucket, output_prefix = matches.groups()
        
        # List output files
        output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)
        
        for blob in output_blobs:
            if blob.content_type != "application/json":
                logger.warning(f"Skipping non-JSON file: {blob.name}")
                continue
            
            try:
                # Download and parse document
                logger.info(f"Processing: {blob.name}")
                document = documentai.Document.from_json(
                    blob.download_as_bytes(), ignore_unknown_fields=True
                )
                
                # Log basic stats
                text_length = len(document.text) if document.text else 0
                page_count = len(document.pages) if document.pages else 0
                
                logger.info(f"  Text length: {text_length:,} characters")
                logger.info(f"  Pages: {page_count}")
                
                # Create a meaningful filename for the consolidated location
                # Extract original filename from the nested path
                original_filename = extract_original_filename(blob.name)
                consolidated_name = f"{consolidated_prefix}{original_filename}"
                
                # Copy to consolidated location
                copy_blob_to_consolidated(
                    blob, consolidated_bucket, consolidated_name
                )
                
                logger.info(f"  Consolidated to: gs://{consolidated_bucket_name}/{consolidated_name}")
                consolidated_count += 1
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing {blob.name}: {e}")
    
    logger.info(f"Successfully processed {processed_count} documents")
    logger.info(f"Consolidated {consolidated_count} JSON files to: {consolidated_uri}")

def extract_original_filename(blob_path: str) -> str:
    """
    Extract a meaningful filename from the nested Document AI output path.
    
    Document AI creates paths like: outputs/annotated-catalog/12345/0/document.json
    We want to create: outputs_12345_0_document.json
    """
    # Remove the base prefix and create a flattened name
    path_parts = blob_path.split('/')
    
    # Find the JSON filename
    json_filename = path_parts[-1]
    
    # Get the nested folder parts (operation_id/file_id)
    if len(path_parts) >= 3:
        operation_part = path_parts[-3]
        file_part = path_parts[-2]
        
        # Create a descriptive filename
        base_name = json_filename.replace('.json', '')
        return f"{base_name}_{operation_part}_{file_part}.json"
    else:
        # Fallback if path structure is unexpected
        return json_filename

def copy_blob_to_consolidated(source_blob, dest_bucket, dest_name: str):
    """Copy a blob to the consolidated location."""
    try:
        source_bucket = source_blob.bucket
        source_bucket.copy_blob(source_blob, dest_bucket, dest_name)
        logger.debug(f"Copied {source_blob.name} to {dest_name}")
    except Exception as e:
        logger.error(f"Failed to copy {source_blob.name}: {e}")
        raise

def main():
    """Main execution function."""
    logger.info("Starting Document AI batch processing")
    
    try:
        # Validate configuration
        validate_config()
        
        # Count input files
        file_count = count_input_files()
        if file_count == 0:
            logger.warning("No PDF files found to process")
            return
        
        # Run batch processing
        success = batch_process_documents(**CONFIG)
        
        if success:
            logger.info("✅ Batch processing completed successfully")
            logger.info("Check the output GCS bucket for processed JSON files")
        else:
            logger.error("❌ Batch processing failed")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()