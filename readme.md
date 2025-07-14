## Classical Music Works Processing Pipeline for RAG System

This pipeline extracts and processes classical music works from catalogues to create text chunks for a Retrieval-Augmented Generation (RAG) system. The process involves:

1. **Document Processing**:
   - Upload PDF catalogues to GCP bucket
   - Use `batch_process.py` to extract text from all PDFs in the folder
   - Download the processed JSON outputs

2. **Text Chunking**:
   - Convert JSON to markdown using `json2md_splitter.py` 
   - Split works into logical chunks using `work_chunk_splitter.py` (LLM-powered)

The resulting chunks are optimized for retrieval and use in generative AI systems about classical music compositions.

3. **Metadata & Upload**:
   - Download the latest composer metadata to `composer_names.csv`
   - Upload processed works to cloud storage using `upload_works_to_r2.py`
