import azure.functions as func
import azure.durable_functions as df
import logging
import json
import os
import hashlib
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from pypdf import PdfReader, PdfWriter
from io import BytesIO
from datetime import datetime
import filetype
import fitz as pymupdf
from PIL import Image
import io
import base64

from doc_intelligence_utilities import analyze_pdf, extract_results, read_document
from aoai_utilities import generate_embeddings, classify_image, analyze_image, get_transcription
from ai_search_utilities import create_vector_index, get_current_index, insert_documents_vector, delete_documents_vector
from chunking_utils import create_chunks, split_text
import tempfile
import subprocess
import concurrent.futures

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    payload = json.loads(req.get_body())

    instance_id = await client.start_new(function_name, client_input=payload)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrators
@app.orchestration_trigger(context_name="context")
def pdf_orchestrator(context):
    """  
    Orchestrates the processing of PDF files for ingestion, analysis, and indexing.  
  
    This function handles the entire workflow of processing PDF files, including:  
    - Retrieving and validating input data from the context.  
    - Creating and updating status records in CosmosDB.  
    - Splitting PDF files into single-page chunks.  
    - Processing PDF chunks with Document Intelligence.  
    - Analyzing pages for embedded visuals if specified.  
    - Chunking extracts based on user specifications.  
    - Generating embeddings for extracted PDF files.  
    - Indexing the processed documents.  
    - Optionally deleting intermediate data.  
  
    Parameters:  
    - context (DurableOrchestrationContext): The context object provided by the Durable Functions runtime.

    API Arguments:
    - source_container (str): The name of the source container.
    - extract_container (str): The name of the extract container.
    - prefix_path (str): The prefix path for the files to be processed.
    - index_name (str): The name of the index to which the documents will be added.
    - automatically_delete (bool): A flag indicating whether to automatically delete intermediate data.
    - analyze_images (bool): A flag indicating whether to analyze images for embedded visuals.
    - overlapping_chunks (bool): A flag indicating whether to allow overlapping chunks. If false, page-wise chunks will be created.
    - chunk_size (int): The size of the chunks to be created.
    - overlap (int): The amount of overlap between chunks.  
  
    Returns:  
    - str: A JSON string containing the list of parent files, processed documents, indexed documents, and the index name.  
  
    Raises:  
    - Exception: If any step in the workflow fails, an exception is raised with an appropriate error message.  
    """

    first_retry_interval_in_milliseconds = 5000
    max_number_of_attempts = 2
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    ###################### DATA INGESTION START ######################
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container names from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    prefix_path = payload.get("prefix_path")
    index_name = payload.get("index_name")
    automatically_delete = payload.get("automatically_delete")
    analyze_images = payload.get("analyze_images")
    overlapping_chunks = payload.get("overlapping_chunks")
    chunk_size = payload.get("chunk_size")
    overlap = payload.get("overlap")
    entra_id = payload.get("entra_id")
    session_id = payload.get("session_id")
    cosmos_record_id = payload.get("cosmos_record_id")
    if cosmos_record_id is None:
        cosmos_record_id = context.instance_id
    if len(cosmos_record_id)==0:
        cosmos_record_id = context.instance_id

    # Create a status record in cosmos that can be updated throughout the course of this ingestion job
    try:
        payload = yield context.call_activity("create_status_record", json.dumps({'cosmos_id': cosmos_record_id}))
        context.set_custom_status('Created Cosmos Record Successfully')
    except Exception as e:
        context.set_custom_status('Failed to Create Cosmos Record')
        pass

    # Create a status record that can be used to update CosmosDB
    try:
        status_record = {}
        status_record['source_container'] = source_container
        status_record['extract_container'] = extract_container
        status_record['prefix_path'] = prefix_path
        status_record['index_name'] = index_name
        status_record['automatically_delete'] = automatically_delete
        status_record['analyze_images'] = analyze_images
        status_record['overlapping_chunks'] = overlapping_chunks
        status_record['chunk_size'] = chunk_size
        status_record['overlap'] = overlap
        status_record['id'] = cosmos_record_id
        status_record['entra_id'] = entra_id
        status_record['session_id'] = session_id
        status_record['status'] = 1
        status_record['status_message'] = 'Starting Ingestion Process'
        status_record['processing_progress'] = 0.1
        yield context.call_activity("update_status_record", json.dumps(status_record))
    except Exception as e:
        pass

    # Define intermediate containers that will hold transient data
    pages_container = f'{source_container}-pages'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    doc_intel_formatted_results_container = f'{source_container}-doc-intel-formatted-results'
    image_analysis_results_container = f'{source_container}-image-analysis-results'

    # Confirm that all storage locations exist to support document ingestion
    try:
        container_check = yield context.call_activity_with_retry("check_containers", retry_options, json.dumps({'source_container': source_container}))
        context.set_custom_status('Document Processing Containers Checked')
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Container Check')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Container Check'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize lists to store parent and extracted files
    parent_files = []
    extracted_files = []
    
     # Get the list of files in the source container
    try:
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': source_container, 'extensions': ['.pdf'], 'prefix': prefix_path}))
        context.set_custom_status('Retrieved Source Files')
    except Exception as e:
        context.set_custom_status('Ingestion Failed During File Retrieval')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During File Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    if len(files) == 0:
        context.set_custom_status('No PDF Files Found')
        status_record['status'] = 0
        status_record['status_message'] = 'No PDF Files Found'
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        raise Exception(f'No PDF files found in the source container matching prefix: {prefix_path}.')



    # For each PDF file, split it into single-page chunks and save to pages container
    try:
        split_pdf_tasks = []
        for file in files:
            # Append the file to the parent_files list
            parent_files.append(file)
            # Create a task to split the PDF file and append it to the split_pdf_tasks list
            split_pdf_tasks.append(context.call_activity_with_retry("split_pdf_files", retry_options, json.dumps({'source_container': source_container, 'pages_container': pages_container, 'file': file})))
        # Execute all the split PDF tasks and get the results
        split_pdf_files = yield context.task_all(split_pdf_tasks)
        # Flatten the list of split PDF files
        split_pdf_files = [item for sublist in split_pdf_files for item in sublist]

        # Convert the split PDF files from JSON strings to Python dictionaries
        pdf_pages = [json.loads(x) for x in split_pdf_files]

    except Exception as e:
        context.set_custom_status('Ingestion Failed During PDF Splitting')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During PDF Splitting'
        # Custom logic for incorrect file type
        if 'not of type PDF' in str(e):
            status_record['status_message'] = 'Ingestion Failed During PDF Splitting: Non-PDF File Type Detected'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('PDF Splitting Completed')
    status_record['status_message'] = 'Splitting Completed'
    status_record['processing_progress'] = 0.2
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # For each PDF page, process it with Document Intelligence and save the results to the document intelligence results (and formatted results) container
    try:
        extract_pdf_tasks = []
        for pdf in pdf_pages:
            # Append the child file to the extracted_files list
            extracted_files.append(pdf['child'])
            # Create a task to process the PDF page and append it to the extract_pdf_tasks list
            extract_pdf_tasks.append(context.call_activity("process_pdf_with_document_intelligence", json.dumps({'child': pdf['child'], 'parent': pdf['parent'], 'pages_container': pages_container, 'doc_intel_results_container': doc_intel_results_container, 'doc_intel_formatted_results_container': doc_intel_formatted_results_container})))
        # Execute all the extract PDF tasks and get the results
        extracted_pdf_files = yield context.task_all(extract_pdf_tasks)

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Document Intelligence Extraction')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Document Intelligence Extraction'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Document Extraction Completion')
    status_record['status_message'] = 'Document Extraction Completion'
    status_record['processing_progress'] = 0.6
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    #Analyze all pages and determine if there is additional visual content that should be described
    try:
        if analyze_images:
            image_analysis_tasks = []
            for pdf in pdf_pages:
                # Append the child file to the extracted_files list
                extracted_files.append(pdf['child'])
                # Create a task to process the PDF page and append it to the extract_pdf_tasks list
                image_analysis_tasks.append(context.call_activity("analyze_pages_for_embedded_visuals", json.dumps({'child': pdf['child'], 'parent': pdf['parent'], 'pages_container': pages_container, 'image_analysis_results_container': image_analysis_results_container})))
            # Execute all the extract PDF tasks and get the results
            analyzed_pdf_files = yield context.task_all(image_analysis_tasks)
            analyzed_pdf_files = [x for x in analyzed_pdf_files if x is not None]

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Image Analysis')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Image Analysis'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Image Analysis Completed')
    status_record['status_message'] = 'Image Analysis Completed'
    status_record['processing_progress'] = 0.7
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    
    # Assemble chunks based on user specification
    try:
        chunking_tasks = []
        for file in files:
            # Append the child file to the extracted_files list
            extracted_files.append(pdf['child'])
            # Create a task to process the PDF chunk and append it to the extract_pdf_tasks list
            chunking_tasks.append(context.call_activity("chunk_extracts", json.dumps({'parent': file, 'extract_container': extract_container, 'doc_intel_formatted_results_container': doc_intel_formatted_results_container, 'image_analysis_results_container': image_analysis_results_container, 'overlapping_chunks': overlapping_chunks, 'chunk_size': chunk_size, 'overlap': overlap})))
        # Execute all the extract PDF tasks and get the results
        chunked_pdf_files = yield context.task_all(chunking_tasks)
        chunked_pdf_files = [item for sublist in chunked_pdf_files for item in sublist]
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Chunking')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Chunking'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Extract Chunking Completed')
    status_record['status_message'] = 'Extract Chunking Completed'
    status_record['processing_progress'] = 0.7
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # For each extracted PDF file, generate embeddings and save the results
    try:
        generate_embeddings_tasks = []
        for file in chunked_pdf_files:
            # Create a task to generate embeddings for the extracted file and append it to the generate_embeddings_tasks list
            generate_embeddings_tasks.append(context.call_activity("generate_extract_embeddings", json.dumps({'extract_container': extract_container, 'file': file})))
        # Execute all the generate embeddings tasks and get the results
        processed_documents = yield context.task_all(generate_embeddings_tasks)
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Vectorization')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Vectorization'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Vectorization Completed')
    status_record['status_message'] = 'Vectorization Completed'
    status_record['processing_progress'] = 0.8
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    ###################### DATA INGESTION END ######################


    ###################### DATA INDEXING START ######################

    try:
        prefix_path = prefix_path.split('.')[0]

        # Use list of files in the extracts container
        files = processed_documents

        # Use the user's provided index name rather than the latest index
        latest_index = index_name
        
        # Get the current index and its fields
        index_detail, fields = get_current_index(index_name)

        context.set_custom_status('Index Retrieval Complete')
        status_record['status_message'] = 'Index Retrieval Complete'

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Index Retrieval')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Index Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize list to store tasks for inserting records
    try:
        insert_tasks = []
        for file in files:
            # Create a task to insert a record for the file and append it to the insert_tasks list
            insert_tasks.append(context.call_activity_with_retry("insert_record", retry_options, json.dumps({'file': file, 'index': latest_index, 'fields': fields, 'extracts-container': extract_container})))
        # Execute all the insert record tasks and get the results
        insert_results = yield context.task_all(insert_tasks)
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Indexing')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Indexing'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Indexing Completed')
    status_record['status_message'] = 'Ingestion Completed'
    status_record['processing_progress'] = 1
    status_record['status'] = 10
    yield context.call_activity("update_status_record", json.dumps(status_record))

    
    ###################### DATA INDEXING END ######################

    ###################### INTERMEDIATE DATA DELETION START ######################

    if automatically_delete:

        try:
            source_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': source_container,  'prefix': prefix_path}))
            chunk_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': pages_container,  'prefix': prefix_path}))
            doc_intel_result_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': doc_intel_results_container,  'prefix': prefix_path}))
            extract_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': extract_container,  'prefix': prefix_path}))

            context.set_custom_status('Ingestion & Clean Up Completed')
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Completed'
            status_record['cleanup_status'] = 10
            yield context.call_activity("update_status_record", json.dumps(status_record))
        
        except Exception as e:
            context.set_custom_status('Data Clean Up Failed')
            status_record['cleanup_status'] = -1
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Failed'
            status_record['cleanup_error_message'] = str(e)
            yield context.call_activity("update_status_record", json.dumps(status_record))
            logging.error(e)
            raise e

    ###################### INTERMEDIATE DATA DELETION END ######################

    # Update Cosmos record with final status
    status_record['parent_files'] = parent_files
    status_record['processed_documents'] = processed_documents
    status_record['indexed_documents'] = insert_results
    status_record['index_name'] = latest_index
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # Return the list of parent files and processed documents as a JSON string
    return json.dumps({'parent_files': parent_files, 'processed_documents': processed_documents, 'indexed_documents': insert_results, 'index_name': latest_index})


@app.orchestration_trigger(context_name="context")
def audio_video_orchestrator(context):
    """  
    Orchestrates the processing of audio/video files for ingestion, analysis, and indexing.  
  
    This function handles the entire workflow of processing PDF files, including:  
    ...
  
    Parameters:  
    - context (DurableOrchestrationContext): The context object provided by the Durable Functions runtime.

    API Arguments:
    - source_container (str): The name of the source container.
    - extract_container (str): The name of the extract container.
    - prefix_path (str): The prefix path for the files to be processed.
    - index_name (str): The name of the index to which the documents will be added.
    - automatically_delete (bool): A flag indicating whether to automatically delete intermediate data.
    - overlapping_chunks (bool): A flag indicating whether to allow overlapping chunks. If false, page-wise chunks will be created.
    - chunk_size (int): The size of the chunks to be created.
    - overlap (int): The amount of overlap between chunks.  
  
    Returns:  
    - str: A JSON string containing the list of parent files, processed documents, indexed documents, and the index name.  
  
    Raises:  
    - Exception: If any step in the workflow fails, an exception is raised with an appropriate error message.  
    """

    first_retry_interval_in_milliseconds = 5000
    max_number_of_attempts = 2
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    ###################### DATA INGESTION START ######################
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container names from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    prefix_path = payload.get("prefix_path")
    index_name = payload.get("index_name")
    automatically_delete = payload.get("automatically_delete")
    overlapping_chunks = payload.get("overlapping_chunks")
    chunk_size = payload.get("chunk_size")
    overlap = payload.get("overlap")
    entra_id = payload.get("entra_id")
    session_id = payload.get("session_id")
    cosmos_record_id = payload.get("cosmos_record_id")
    if cosmos_record_id is None:
        cosmos_record_id = context.instance_id
    if len(cosmos_record_id)==0:
        cosmos_record_id = context.instance_id

    # Create a status record in cosmos that can be updated throughout the course of this ingestion job
    try:
        payload = yield context.call_activity("create_status_record", json.dumps({'cosmos_id': cosmos_record_id}))
        context.set_custom_status('Created Cosmos Record Successfully')
    except Exception as e:
        context.set_custom_status('Failed to Create Cosmos Record')
        pass

    # Create a status record that can be used to update CosmosDB
    try:
        status_record = {}
        status_record['source_container'] = source_container
        status_record['extract_container'] = extract_container
        status_record['prefix_path'] = prefix_path
        status_record['index_name'] = index_name
        status_record['automatically_delete'] = automatically_delete
        status_record['overlapping_chunks'] = overlapping_chunks
        status_record['chunk_size'] = chunk_size
        status_record['overlap'] = overlap
        status_record['entra_id'] = entra_id
        status_record['session_id'] = session_id
        status_record['id'] = cosmos_record_id
        status_record['status'] = 1
        status_record['status_message'] = 'Starting Ingestion Process'
        status_record['processing_progress'] = 0.1
        yield context.call_activity("update_status_record", json.dumps(status_record))
    except Exception as e:
        pass

    # Define intermediate containers that will hold transient data
    transcripts_container = f'{source_container}-transcripts'
    
    # Confirm that all storage locations exist to support document ingestion
    try:
        container_check = yield context.call_activity_with_retry("check_audio_video_containers", retry_options, json.dumps({'source_container': source_container}))
        context.set_custom_status('Audio/Video Processing Containers Checked')
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Container Check')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Container Check'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize lists to store parent and extracted files
    parent_files = []
    extracted_files = []
    
     # Get the list of files in the source container
    try:
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': source_container, 'extensions': ['.mp3', '.mp4', '.mpweg', '.mpga', '.m4a', '.wav', '.webm'], 'prefix': prefix_path}))
        context.set_custom_status('Retrieved Source Files')
    except Exception as e:
        context.set_custom_status('Ingestion Failed During File Retrieval')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During File Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Retrieved Source Files')
    status_record['status_message'] = 'Retrieved Source Files'
    status_record['processing_progress'] = 0.1
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    if len(files) == 0:
        context.set_custom_status('No Audio/Video Files Found')
        status_record['status'] = 0
        status_record['status_message'] = 'No Audio/Video Files Found'
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        raise Exception(f'No audio/video files found in the source container matching prefix: {prefix_path}.')



    ###### UPDATE LOGIC HERE - TRANSCRIBE FILES
    try:
        transcribe_file_tasks = []
        for file in files:
            # Append the file to the parent_files list
            parent_files.append(file)
            # Create a task to transcribe the audio/video file and append it to the transcribe_file_tasks list
            transcribe_file_tasks.append(context.call_activity_with_retry("transcribe_audio_video_files", retry_options, json.dumps({'source_container': source_container, 'transcripts_container': transcripts_container, 'file': file})))
        transcribed_audio_files = yield context.task_all(transcribe_file_tasks)

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Transcription')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Transcription'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Audio Transcription Completed')
    status_record['status_message'] = 'Audio Transcription Completed'
    status_record['processing_progress'] = 0.6
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    #### THEN CHUNK TRANSCRIPTS
    try:
        chunking_tasks = []
        for file in transcribed_audio_files:
            chunking_tasks.append(context.call_activity("chunk_audio_video_transcripts", json.dumps({'parent': file, 'transcript_container': transcripts_container, 'extract_container': extract_container, 'overlapping_chunks': overlapping_chunks, 'chunk_size': chunk_size, 'overlap': overlap})))
        chunked_transcript_files = yield context.task_all(chunking_tasks)
        chunked_transcript_files = [item for sublist in chunked_transcript_files for item in sublist]
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Chunking')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Chunking'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Extract Chunking Completed')
    status_record['status_message'] = 'Extract Chunking Completed'
    status_record['processing_progress'] = 0.7
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    #### THEN GENERATE EMBEDDINGS

    # For each transcribed audio/video file, generate embeddings and save the results
    try:
        generate_embeddings_tasks = []
        for file in chunked_transcript_files:
            # Create a task to generate embeddings for the extracted file and append it to the generate_embeddings_tasks list
            generate_embeddings_tasks.append(context.call_activity("generate_extract_embeddings", json.dumps({'extract_container': extract_container, 'file': file})))
        # Execute all the generate embeddings tasks and get the results
        processed_documents = yield context.task_all(generate_embeddings_tasks)
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Vectorization')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Vectorization'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Vectorization Completed')
    status_record['status_message'] = 'Vectorization Completed'
    status_record['processing_progress'] = 0.8
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    ###################### DATA INGESTION END ######################


    ###################### DATA INDEXING START ######################

    try:
        prefix_path = prefix_path.split('.')[0]

        # Use list of files in the extracts container
        files = processed_documents

        # Use the user's provided index name rather than the latest index
        latest_index = index_name
        
        # Get the current index and its fields
        index_detail, fields = get_current_index(index_name)

        context.set_custom_status('Index Retrieval Complete')
        status_record['status_message'] = 'Index Retrieval Complete'

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Index Retrieval')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Index Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize list to store tasks for inserting records
    try:
        insert_tasks = []
        for file in files:
            # Create a task to insert a record for the file and append it to the insert_tasks list
            insert_tasks.append(context.call_activity_with_retry("insert_record", retry_options, json.dumps({'file': file, 'index': latest_index, 'fields': fields, 'extracts-container': extract_container})))
        # Execute all the insert record tasks and get the results
        insert_results = yield context.task_all(insert_tasks)
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Indexing')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Indexing'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Indexing Completed')
    status_record['status_message'] = 'Ingestion Completed'
    status_record['processing_progress'] = 1
    status_record['status'] = 10
    yield context.call_activity("update_status_record", json.dumps(status_record))

    ###################### DATA INDEXING END ######################

    ###################### INTERMEDIATE DATA DELETION START ######################

    if automatically_delete:

        try:
            source_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': source_container,  'prefix': prefix_path}))
            transcript_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': transcripts_container,  'prefix': prefix_path}))
            extract_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': extract_container,  'prefix': prefix_path}))

            context.set_custom_status('Ingestion & Clean Up Completed')
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Completed'
            status_record['cleanup_status'] = 10
            yield context.call_activity("update_status_record", json.dumps(status_record))
        
        except Exception as e:
            context.set_custom_status('Data Clean Up Failed')
            status_record['cleanup_status'] = -1
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Failed'
            status_record['cleanup_error_message'] = str(e)
            yield context.call_activity("update_status_record", json.dumps(status_record))
            logging.error(e)
            raise e

    ###################### INTERMEDIATE DATA DELETION END ######################

    # Update Cosmos record with final status
    status_record['parent_files'] = parent_files
    status_record['processed_documents'] = processed_documents
    status_record['indexed_documents'] = insert_results
    status_record['index_name'] = latest_index
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # Return the list of parent files and processed documents as a JSON string
    return json.dumps({'parent_files': parent_files, 'processed_documents': processed_documents, 'indexed_documents': insert_results, 'index_name': latest_index})


@app.orchestration_trigger(context_name="context")
def non_pdf_orchestrator(context):

    first_retry_interval_in_milliseconds = 5000
    max_number_of_attempts = 2
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    ###################### DATA INGESTION START ######################
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container names from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    prefix_path = payload.get("prefix_path")
    index_name = payload.get("index_name")
    automatically_delete = payload.get("automatically_delete")
    analyze_images = payload.get("analyze_images")
    overlapping_chunks = payload.get("overlapping_chunks")
    chunk_size = payload.get("chunk_size")
    overlap = payload.get("overlap")
    entra_id = payload.get("entra_id")
    session_id = payload.get("session_id")
    cosmos_record_id = payload.get("cosmos_record_id")
    if cosmos_record_id is None:
        cosmos_record_id = context.instance_id
    if len(cosmos_record_id)==0:
        cosmos_record_id = context.instance_id

    # Create a status record in cosmos that can be updated throughout the course of this ingestion job
    try:
        payload = yield context.call_activity("create_status_record", json.dumps({'cosmos_id': cosmos_record_id}))
        context.set_custom_status('Created Cosmos Record Successfully')
    except Exception as e:
        context.set_custom_status('Failed to Create Cosmos Record')
        pass

    # Create a status record that can be used to update CosmosDB
    try:
        status_record = {}
        status_record['source_container'] = source_container
        status_record['extract_container'] = extract_container
        status_record['prefix_path'] = prefix_path
        status_record['index_name'] = index_name
        status_record['automatically_delete'] = automatically_delete
        status_record['analyze_images'] = analyze_images
        status_record['overlapping_chunks'] = overlapping_chunks
        status_record['chunk_size'] = chunk_size
        status_record['overlap'] = overlap
        status_record['id'] = cosmos_record_id
        status_record['entra_id'] = entra_id
        status_record['session_id'] = session_id
        status_record['status'] = 1
        status_record['status_message'] = 'Starting Ingestion Process'
        status_record['processing_progress'] = 0.1
        yield context.call_activity("update_status_record", json.dumps(status_record))
    except Exception as e:
        pass

    # Get source files
    try:
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': source_container, 'extensions': ['.doc', '.docx', '.dot', '.dotx', '.odt', '.ott', '.fodt', '.sxw', '.stw', '.uot', '.rtf', '.txt', '.xls', '.xlsx', '.xlsm', '.xlt', '.xltx', '.ods', '.ots', '.fods', '.sxc', '.stc', '.uos', '.csv', '.ppt', '.pptx', '.pps', '.ppsx', '.pot', '.potx', '.odp', '.otp', '.fodp', '.sxi', '.sti', '.uop', '.odg', '.otg', '.fodg', '.sxd', '.std', '.svg', '.html', '.htm', '.xps', '.epub'], 'prefix': prefix_path}))
        context.set_custom_status('Retrieved Source Files')
    except Exception as e:
        context.set_custom_status('Ingestion Failed During File Retrieval')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During File Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Convert documents to pdf
    try:
        pdf_file_conversion_tasks = []
        for file in files:
            pdf_file_conversion_tasks.append(context.call_activity_with_retry("convert_pdf_activity", retry_options, json.dumps({'container': source_container, 'filename': file})))
        converted_pdf_files = yield context.task_all(pdf_file_conversion_tasks)
        context.set_custom_status('Converted Documents to PDF')
    except Exception as e:
        context.set_custom_status('Ingestion Failed During PDF Conversion')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During PDF Conversion'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Converted Document to PDF')
    status_record['status_message'] = 'Converted Document to PDF'
    status_record['processing_progress'] = 0.1
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # Call the PDF orchestrator to process the PDF file
    try:
        updated_payload = context.get_input()
        updated_payload['prefix_path'] = prefix_path.split('.')[0] # account for conversion to pdf if full path provided
        pdf_orchestrator_response = yield context.call_sub_orchestrator("pdf_orchestrator", updated_payload)
        return pdf_orchestrator_response
    except Exception as e:
        raise e


@app.orchestration_trigger(context_name="context")
def delete_documents_orchestrator(context):
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container name, index stem name, and prefix path from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    index_name = payload.get("index_name")
    prefix_path = payload.get("prefix_path")
    delete_all_files = payload.get("delete_all_files")

    # TO-DO: Update prefix with _page and _chunk postfixes for greater precision

    pages_container = f'{source_container}-pages'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    doc_intel_formatted_results_container = f'{source_container}-doc-intel-formatted-results'
    image_analysis_results_container = f'{source_container}-image-analysis-results'
    transcripts_container = f'{source_container}-transcripts'

    prefix_path = prefix_path.split('.')[0]

    source_files = yield context.call_activity("get_source_files", json.dumps({'source_container': source_container,  'prefix': prefix_path, 'extensions': ['.pdf']}))
    page_files = yield context.call_activity("get_source_files", json.dumps({'source_container': pages_container,  'prefix': prefix_path, 'extensions': ['.pdf']}))
    doc_intel_result_files = yield context.call_activity("get_source_files", json.dumps({'source_container': doc_intel_results_container,  'prefix': prefix_path, 'extensions': ['.json']}))
    extract_files = yield context.call_activity("get_source_files", json.dumps({'source_container': extract_container,  'prefix': prefix_path, 'extensions': ['.json']}))
    doc_intel_formatted_results_files = yield context.call_activity("get_source_files", json.dumps({'source_container': doc_intel_formatted_results_container,  'prefix': prefix_path, 'extensions': ['.json']}))
    image_analysis_files = yield context.call_activity("get_source_files", json.dumps({'source_container': image_analysis_results_container,  'prefix': prefix_path, 'extensions': ['.json']}))
    transcripts_files = yield context.call_activity("get_source_files", json.dumps({'source_container': transcripts_container,  'prefix': prefix_path, 'extensions': ['.json']}))

    deleted_ai_search_documents = yield context.call_activity("delete_records", json.dumps({'file': extract_files, 'index': index_name, 'extracts-container': extract_container}))

    deleted_extract_files = []
    deleted_doc_intel_files = []
    deleted_doc_intel_formatted_files = []
    deleted_image_analysis_files = []
    deleted_page_files = []
    deleted_source_files = []
    deleted_transcripts_files = []

    if delete_all_files:
        delete_extract_file_tasks = []
        for file in extract_files:
            delete_extract_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': extract_container, 'prefix': file})))
        deleted_extract_files = yield context.task_all(delete_extract_file_tasks)

        delete_doc_intel_file_tasks = []
        for file in doc_intel_result_files:
            delete_doc_intel_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': doc_intel_results_container, 'prefix': file})))
        deleted_doc_intel_files = yield context.task_all(delete_doc_intel_file_tasks)

        delete_doc_intel_formatted_file_tasks = []
        for file in doc_intel_formatted_results_files:
            delete_doc_intel_formatted_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': doc_intel_formatted_results_container, 'prefix': file})))
        deleted_doc_intel_formatted_files = yield context.task_all(delete_doc_intel_formatted_file_tasks)

        delete_image_analysis_file_tasks = []
        for file in image_analysis_files:
            delete_image_analysis_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': image_analysis_results_container, 'prefix': file})))
        deleted_image_analysis_files = yield context.task_all(delete_image_analysis_file_tasks)

        delete_page_file_tasks = []
        for file in page_files:
            delete_page_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': pages_container, 'prefix': file})))
        deleted_page_files = yield context.task_all(delete_page_file_tasks)

        delete_source_file_tasks = []
        for file in source_files:
            delete_source_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': source_container, 'prefix': file})))
        deleted_source_files = yield context.task_all(delete_source_file_tasks)

        delete_transcripts_file_tasks = []
        for file in transcripts_files:
            delete_transcripts_file_tasks.append(context.call_activity("delete_source_files", json.dumps({'source_container': transcripts_container, 'prefix': file})))
        deleted_transcripts_files = yield context.task_all(delete_transcripts_file_tasks)


    return json.dumps({'deleted_ai_search_documents': deleted_ai_search_documents, 
                       'deleted_extract_files': deleted_extract_files, 
                       'deleted_doc_intel_files': deleted_doc_intel_files, 
                       'deleted_doc_intel_formatted_files': deleted_doc_intel_formatted_files,
                       'deleted_image_analysis_files': deleted_image_analysis_files,
                       'deleted_transcript_files': deleted_transcripts_files,
                       'deleted_page_files': deleted_page_files, 
                       'deleted_source_files': deleted_source_files})

# Activities
@app.activity_trigger(input_name="activitypayload")
def get_source_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    extensions = data.get("extensions")
    prefix = data.get("prefix")
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    try:
        # Get a ContainerClient object from the BlobServiceClient
        container_client = blob_service_client.get_container_client(source_container)
        # List all blobs in the container that start with the specified prefix
        blobs = container_client.list_blobs(name_starts_with=prefix)

    except Exception as e:
        # If the container does not exist, return an empty list
        return []

    if not container_client.exists():
        return []
    
    # Initialize an empty list to store the names of the files
    files = []

    # For each blob in the container
    for blob in blobs:
        # If the blob's name ends with the specified extension
        if '.' + blob.name.lower().split('.')[-1] in extensions:
            # Append the blob's name to the files list
            files.append(blob.name)

    # Return the list of file names
    return files

@app.activity_trigger(input_name="activitypayload")
def delete_source_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    prefix = data.get("prefix")
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object from the BlobServiceClient
    container_client = blob_service_client.get_container_client(source_container)

    if not container_client.exists():
        return []
    
    # List all blobs in the container that start with the specified prefix
    blobs = container_client.list_blobs(name_starts_with=prefix)

    # Initialize an empty list to store the names of the files
    files = []

    # For each blob in the container
    for blob in blobs:
        files.append(blob.name)

    for file in files:
        blob = container_client.get_blob_client(file)
        blob.delete_blob()

    # Return the list of file names
    return files

@app.activity_trigger(input_name="activitypayload")
def check_containers(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    
    pages_container = f'{source_container}-pages'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    doc_intel_formatted_results_container = f'{source_container}-doc-intel-formatted-results'
    image_analysis_results_container = f'{source_container}-image-analysis-results'
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    try:
        blob_service_client.create_container(doc_intel_results_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(pages_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(image_analysis_results_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(doc_intel_formatted_results_container)
    except Exception as e:
        pass

    # Return the list of file names
    return True

@app.activity_trigger(input_name="activitypayload")
def check_audio_video_containers(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    
    transcripts_container = f'{source_container}-transcripts'
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    try:
        blob_service_client.create_container(transcripts_container)
    except Exception as e:
        pass

    # Return the list of file names
    return True

@app.activity_trigger(input_name="activitypayload")
def split_pdf_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, chunks container, and file name from the payload
    source_container = data.get("source_container")
    pages_container = data.get("pages_container")
    file = data.get("file")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object for the source and chunks containers
    source_container = blob_service_client.get_container_client(source_container)
    pages_container = blob_service_client.get_container_client(pages_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = source_container.get_blob_client(file)

    # Initialize an empty list to store the PDF chunks
    pdf_chunks = []

    # If the PDF file exists
    if  pdf_blob_client.exists():

        blob_data = pdf_blob_client.download_blob().readall()

        kind = filetype.guess(blob_data)

        if kind.EXTENSION != 'pdf':
            raise Exception(f'{file} is not of type PDF. Detected MIME type: {kind.EXTENSION}')

        # Create a PdfReader object for the PDF file
        pdf_reader = PdfReader(BytesIO(blob_data))

        # Get the number of pages in the PDF file
        num_pages = len(pdf_reader.pages)

        # For each page in the PDF file
        for i in range(num_pages):
            # Create a new file name for the PDF chunk
            new_file_name = file.replace('.pdf', '') + '_page_' + str(i+1) + '.pdf'

            # Create a PdfWriter object
            pdf_writer = PdfWriter()
            # Add the page to the PdfWriter object
            pdf_writer.add_page(pdf_reader.pages[i])

            # Create a BytesIO object for the output stream
            output_stream = BytesIO()
            # Write the PdfWriter object to the output stream
            pdf_writer.write(output_stream)

            # Reset the position of the output stream to the beginning
            output_stream.seek(0)

            # Get a BlobClient object for the PDF chunk
            pdf_chunk_blob_client = pages_container.get_blob_client(blob=new_file_name)

            # Upload the PDF chunk to the chunks container
            pdf_chunk_blob_client.upload_blob(output_stream, overwrite=True)
            
            # Append the parent file name and child file name to the pdf_chunks list
            pdf_chunks.append(json.dumps({'parent': file, 'child': new_file_name}))

    # Return the list of PDF chunks
    return pdf_chunks
    
@app.activity_trigger(input_name="activitypayload")
def process_pdf_with_document_intelligence(activitypayload: str):
    """
    Process a PDF file using Document Intelligence.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    child = data.get("child")
    parent = data.get("parent")
    pages_container = data.get("pages_container")
    doc_intel_results_container = data.get("doc_intel_results_container")
    doc_intel_formatted_results_container = data.get("doc_intel_formatted_results_container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the pages, Document Intelligence results, and DI formatted results containers
    pages_container_client = blob_service_client.get_container_client(container=pages_container)
    doc_intel_results_container_client = blob_service_client.get_container_client(container=doc_intel_results_container)
    doc_intel_formatted_results_container_client = blob_service_client.get_container_client(container=doc_intel_formatted_results_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = pages_container_client.get_blob_client(blob=child)

    # Initialize a flag to indicate whether the PDF file has been processed
    processed = False

    # Create a new file name for the processed PDF file
    updated_filename = child.replace('.pdf', '.json')

    # Get a BlobClient object for the Document Intelligence results file
    doc_results_blob_client = doc_intel_results_container_client.get_blob_client(blob=updated_filename)
    # Check if the Document Intelligence results file exists
    if doc_results_blob_client.exists():

        # Get a BlobClient object for the extracts file
        extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=updated_filename)

        # If the extracts file exists
        if extract_blob_client.exists():

            # Download the PDF file as a stream
            pdf_stream_downloader = (pdf_blob_client.download_blob())

            # Calculate the MD5 hash of the PDF file
            md5_hash = hashlib.md5()
            for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
                md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()

            # Load the extracts file as a JSON string
            extract_data = json.loads((extract_blob_client.download_blob().readall()).decode('utf-8'))

            # If the checksum in the extracts file matches the checksum of the PDF file
            if 'checksum' in extract_data.keys():
                if extract_data['checksum']==checksum:
                    # Set the processed flag to True
                    processed = True

    # If the PDF file has not been processed
    if not processed:
        # Extract the PDF file with AFR, save the AFR results, and save the extract results

        # Download the PDF file
        pdf_data = pdf_blob_client.download_blob().readall()
        # Analyze the PDF file with Document Intelligence
        doc_intel_result = analyze_pdf(pdf_data)

        # Get a BlobClient object for the Document Intelligence results file
        doc_intel_result_client = doc_intel_results_container_client.get_blob_client(updated_filename)

        # Upload the Document Intelligence results to the Document Intelligence results container
        doc_intel_result_client.upload_blob(json.dumps(doc_intel_result), overwrite=True)

        # Extract the results from the Document Intelligence results
        page_map = extract_results(doc_intel_result, updated_filename)

        # Extract the page number from the child file name
        page_number = child.split('_')[-1]
        page_number = page_number.replace('.pdf', '')
        # Get the content from the page map
        content = page_map[0][1]

        # Generate a unique ID for the record
        id_str = child 
        hash_object = hashlib.sha256()
        hash_object.update(id_str.encode('utf-8'))
        id = hash_object.hexdigest()

        # Download the PDF file as a stream
        pdf_stream_downloader = (pdf_blob_client.download_blob())

        # Calculate the MD5 hash of the PDF file
        md5_hash = hashlib.md5()
        for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
            md5_hash.update(byte_block)
        checksum = md5_hash.hexdigest()

        # Create a record for the PDF file
        record = {
            'content': content,
            'sourcefile': parent,
            'sourcepage': child,
            'pagenumber': page_number,
            'category': 'manual',
            'id': str(id),
            'checksum': checksum
        }

        # Get a BlobClient object for the extracts file
        extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=updated_filename)

        # Upload the record to the extracts container
        extract_blob_client.upload_blob(json.dumps(record), overwrite=True)

    # Return the updated file name
    return updated_filename

@app.activity_trigger(input_name="activitypayload")
def analyze_pages_for_embedded_visuals(activitypayload: str):
    """
    Analyze a single page in a PDF to determine if there are charts, graphs, etc.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    child = data.get("child")
    parent = data.get("parent")
    pages_container = data.get("pages_container")
    image_analysis_results_container = data.get("image_analysis_results_container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Download pdf
    pages_container_client = blob_service_client.get_container_client(container=pages_container)
    image_analysis_results_container_client = blob_service_client.get_container_client(container=image_analysis_results_container)
    pdf_blob_client = pages_container_client.get_blob_client(blob=child)

    # Download the blob (PDF)
    downloaded_blob = pdf_blob_client.download_blob()
    pdf_bytes = downloaded_blob.readall()

    png_bytes_io = pdf_bytes_to_png_bytes(pdf_bytes, 1)

    # Convert to base64 for transmission or storage
    png_bytes = png_bytes_io.getvalue()
    png_bytes = base64.b64encode(png_bytes).decode('ascii')

    # Set result filename
    file_name = child.replace('.pdf', '.json')

    # Chucksum calculation - download the PDF file as a stream
    pdf_stream_downloader = (pdf_blob_client.download_blob())

    # Calculate the MD5 hash of the PDF file
    md5_hash = hashlib.md5()
    for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
        md5_hash.update(byte_block)
    checksum = md5_hash.hexdigest()

    # Do a baseline check here to see if the file already exists in the image analysis results container
    # If yes then return the file name without processing
    image_analysis_results_blob_client = image_analysis_results_container_client.get_blob_client(blob=file_name)
    if image_analysis_results_blob_client.exists():
        extract_data = json.loads((image_analysis_results_blob_client.download_blob().readall()))
        # If the checksum in the extracts file matches the checksum of the PDF file
        if 'checksum' in extract_data.keys():
            if extract_data['checksum']==checksum:
                return file_name

    
    ## TO-DO: ADD LOGIC FOR CHECKSUM CALCULATION HERE TO PREVENT DUPLICATE PROCESSING
    # Save records to the image analysis results container in JSON format
    visual_analysis_result = {'checksum':checksum, 'visual_description':''}

    contains_visuals = classify_image(png_bytes)

    if contains_visuals:
        visual_description = analyze_image(png_bytes)
        try:
            visual_description = json.loads(visual_description)
        except Exception as e:
            pass
        file_name = child.replace('.pdf', '.json')
        visual_analysis_result['visual_description'] = str(visual_description)
        image_analysis_results_blob_client = image_analysis_results_container_client.get_blob_client(blob=file_name)
        image_analysis_results_blob_client.upload_blob(json.dumps(visual_analysis_result), overwrite=True)

    else:
        image_analysis_results_blob_client = image_analysis_results_container_client.get_blob_client(blob=file_name)
        image_analysis_results_blob_client.upload_blob(json.dumps(visual_analysis_result), overwrite=True)
    
    return file_name

@app.activity_trigger(input_name="activitypayload")
def transcribe_audio_video_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the source container, extract container, transcription results container, and file name from the payload
    source_container_name = data.get("source_container")
    transcripts_container_name = data.get("transcripts_container")
    file = data.get("file")

    # Create new file names for the transcript and extract files
    transcript_file_name = file.split('.')[0] + '.json'
    extract_file_name = file.split('.')[0] + '.json'

    # Generate a unique ID for the record
    id_str = file
    hash_object = hashlib.sha256()  
    hash_object.update(id_str.encode('utf-8'))  
    id = hash_object.hexdigest()  

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the source, extract, and transcription results containers
    source_container = blob_service_client.get_container_client(source_container_name)
    transcript_container = blob_service_client.get_container_client(transcripts_container_name)

    # Get a BlobClient object for the transcript file
    transcript_blob_client = transcript_container.get_blob_client(blob=transcript_file_name)

    # If the transcript file does not exist
    if not transcript_blob_client.exists():

        # Get a BlobClient object for the audio file
        audio_blob_client = source_container.get_blob_client(blob=file)

        # Download the audio file
        audio_data = audio_blob_client.download_blob().readall()

        # Get the extension of the audio file
        _, extension = os.path.splitext(audio_blob_client.blob_name)

        # Download the audio file to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as temp_file:
            temp_file.write(audio_data)

            print(f'Saved {audio_blob_client.blob_name} to {temp_file.name}\n')

            # Get the path of the temporary file
            local_audio_file = temp_file.name

            try:
                # Transcribe the audio file
                transcript = get_transcription(local_audio_file)
            except Exception as e:
                print(f'Error transcribing {audio_blob_client.blob_name}: {e}')
                logging.error(f'Error transcribing {audio_blob_client.blob_name}: {e}')
                pass


            # Create a record for the transcript
            record = {
                'sourcefile': file,
                'content': transcript,
                'category': 'audio-video'
            }

            # Upload the transcript to the transcription results container
            transcript_blob_client.upload_blob(json.dumps(record), overwrite=True)

    # Return the name of the extract file
    return extract_file_name


@app.activity_trigger(input_name="activitypayload")
def chunk_extracts(activitypayload: str):
    """
    UPDATE!!!!
    Analyze a single page in a PDF to determine if there are charts, graphs, etc.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    parent = data.get("parent")
    extract_container = data.get("extract_container")
    doc_intel_formatted_results_container = data.get("doc_intel_formatted_results_container")
    image_analysis_results_container = data.get("image_analysis_results_container")
    overlapping_chunks = data.get("overlapping_chunks")
    chunk_size = data.get("chunk_size")
    overlap = data.get("overlap")

    prefix = parent.split('.')[0]


    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get container clients
    extract_container_client = blob_service_client.get_container_client(container=extract_container)
    doc_intel_formatted_results_container_client = blob_service_client.get_container_client(container=doc_intel_formatted_results_container)
    image_analysis_results_container_client = blob_service_client.get_container_client(container=image_analysis_results_container)

    extracted_files = []

    for blob in doc_intel_formatted_results_container_client.list_blobs(name_starts_with=prefix):
        extracted_files.append(blob.name)

    out_files = []

    if overlapping_chunks==False:
        for file in extracted_files:
            # Get a BlobClient object for the extracts file
            extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=file)
            image_analysis_client = image_analysis_results_container_client.get_blob_client(blob=file)

            # Load the extracts file as a JSON string
            extract_data = json.loads((extract_blob_client.download_blob().readall()).decode('utf-8'))

            # Load the image analysis file as a JSON string
            if image_analysis_client.exists():
                image_analysis_data = json.loads(image_analysis_client.download_blob().readall())


                if len(image_analysis_data['visual_description'])>0:
                    visual_description = image_analysis_data['visual_description']
                    extract_data['content'] += f'\n\nVisual Components Description:\n{str(visual_description)}'
                    extract_data['content'] = str(extract_data['content'])

            id_str = extract_data['content'] + extract_data['sourcepage']
            hash_object = hashlib.sha256()
            hash_object.update(id_str.encode('utf-8'))
            id = hash_object.hexdigest()

            extract_data['id'] = id

            # Get a BlobClient object for the extracts file
            final_extract_blob_client = extract_container_client.get_blob_client(blob=file)
            final_extract_blob_client.upload_blob(json.dumps(extract_data), overwrite=True)
            out_files.append(file)
    else:
        chunks_content_dict = {}
        for file in extracted_files:
            # Get a BlobClient object for the extracts file
            extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=file)
            image_analysis_client = image_analysis_results_container_client.get_blob_client(blob=file)

            # Load the extracts file as a JSON string
            extract_data = json.loads((extract_blob_client.download_blob().readall()).decode('utf-8'))

            # Load the image analysis file as a JSON string
            if image_analysis_client.exists():
                image_analysis_data = json.loads(image_analysis_client.download_blob().readall())


                if len(image_analysis_data['visual_description'])>0:
                    visual_description = image_analysis_data['visual_description']
                    extract_data['content'] += f'\n\nVisual Components Description:\n{str(visual_description)}'
                    extract_data['content'] = str(extract_data['content'])

            id_str = extract_data['content'] + file
            hash_object = hashlib.sha256()
            hash_object.update(id_str.encode('utf-8'))
            id = hash_object.hexdigest()

            extract_data['id'] = id

            page_number = int(extract_data['pagenumber'])

            chunks_content_dict[page_number] = extract_data

        chunked_content = create_chunks(chunks_content_dict, chunk_size, overlap)

        for idx, chunk in enumerate(chunked_content):
            
            # Get a BlobClient object for the extracts file
            filename = f'{prefix}_chunk_{idx}.json'
            final_extract_blob_client = extract_container_client.get_blob_client(blob=filename)
            final_extract_blob_client.upload_blob(json.dumps(chunk), overwrite=True)
            out_files.append(filename)

    return out_files


@app.activity_trigger(input_name="activitypayload")
def chunk_audio_video_transcripts(activitypayload: str):
    """
    UPDATE!!!!
    Analyze a single page in a PDF to determine if there are charts, graphs, etc.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    parent = data.get("parent")
    extract_container = data.get("extract_container")
    transcript_container = data.get("transcript_container")
    overlapping_chunks = data.get("overlapping_chunks")
    chunk_size = data.get("chunk_size")
    overlap = data.get("overlap")

    prefix = parent.split('.')[0]


    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get container clients
    extract_container_client = blob_service_client.get_container_client(container=extract_container)
    transcript_container_client = blob_service_client.get_container_client(container=transcript_container)
    
    transcript_files = []

    for blob in transcript_container_client.list_blobs(name_starts_with=prefix):
        transcript_files.append(blob.name)

    out_files = []

    if overlapping_chunks==False:
        for file in transcript_files:
            # Get a BlobClient object for the transcript file
            transcript_blob_client = transcript_container_client.get_blob_client(blob=file)
            
            # Load the transcript file as a JSON string
            transcript_data = json.loads((transcript_blob_client.download_blob().readall()).decode('utf-8'))

            chunks = split_text(transcript_data['content'], 800, 0)

            chunks = [x[2] for x in chunks]

            for idx, chunk in enumerate(chunks):

                id_str = chunk + transcript_data['sourcefile'] + str(idx)
                hash_object = hashlib.sha256()
                hash_object.update(id_str.encode('utf-8'))
                id = hash_object.hexdigest()

                extract_data = {}
                extract_data['id'] = id
                extract_data['content'] = chunk
                extract_data['sourcefile'] = transcript_data['sourcefile']
                extract_data['chunkcount'] = idx

                filename = file.split('.')[0] + f'_chunk_{idx}.json'

                # Get a BlobClient object for the extracts file
                final_extract_blob_client = extract_container_client.get_blob_client(blob=filename)
                final_extract_blob_client.upload_blob(json.dumps(extract_data), overwrite=True)
                out_files.append(filename)
    else:
        for file in transcript_files:
            transcript_blob_client = transcript_container_client.get_blob_client(blob=file)
            
            # Load the transcript file as a JSON string
            transcript_data = json.loads((transcript_blob_client.download_blob().readall()).decode('utf-8'))

            chunks = split_text(transcript_data['content'], chunk_size, overlap)

            chunks = [x[2] for x in chunks]

            for idx, chunk in enumerate(chunks):

                id_str = chunk + transcript_data['sourcefile'] + str(idx+1)
                hash_object = hashlib.sha256()
                hash_object.update(id_str.encode('utf-8'))
                id = hash_object.hexdigest()

                extract_data = {}
                extract_data['id'] = id
                extract_data['content'] = chunk
                extract_data['sourcefile'] = transcript_data['sourcefile']
                extract_data['chunkcount'] = idx+1
                extract_data['category'] = transcript_data['category']

                filename = file.split('.')[0] + f'_chunk_{idx+1}.json'

                # Get a BlobClient object for the extracts file
                final_extract_blob_client = extract_container_client.get_blob_client(blob=filename)
                final_extract_blob_client.upload_blob(json.dumps(extract_data), overwrite=True)
                out_files.append(filename)

    return out_files

@app.activity_trigger(input_name="activitypayload")
def generate_extract_embeddings(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the extract container and file name from the payload
    extract_container = data.get("extract_container")
    file = data.get("file")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extract container
    extract_container_client = blob_service_client.get_container_client(container=extract_container)

    # Get a BlobClient object for the extract file
    extract_blob = extract_container_client.get_blob_client(blob=file)

    # Load the extract file as a JSON string
    extract_data =  json.loads((extract_blob.download_blob().readall()).decode('utf-8'))

    # If the extract data does not contain embeddings
    if 'embeddings' not in extract_data.keys():

        # Extract the content from the extract data
        content = extract_data['content']

        # Generate embeddings for the content
        embeddings = generate_embeddings(content)

        # Update the extract data with the embeddings
        updated_record = extract_data
        updated_record['embeddings'] = embeddings

        # Upload the updated extract data to the extract container
        extract_blob.upload_blob(json.dumps(updated_record), overwrite=True)

    # Return the file name
    return file

@app.activity_trigger(input_name="activitypayload")
def insert_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the file name, index, fields, and extracts container from the payload
    file = data.get("file")
    index = data.get("index")
    fields = data.get("fields")
    extracts_container = data.get("extracts-container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extracts container
    container_client = blob_service_client.get_container_client(container=extracts_container)

    # Get a BlobClient object for the file
    blob_client = container_client.get_blob_client(blob=file)

    # Download the file as a string
    file_data = (blob_client.download_blob().readall()).decode('utf-8')

    # Load the file data as a JSON string
    file_data =  json.loads(file_data)

    # Filter the file data to only include the specified fields
    file_data = {key: value for key, value in file_data.items() if key in fields}

    # Insert the file data into the specified index
    insert_documents_vector([file_data], index)

    # Return the file name
    return file

@app.activity_trigger(input_name="activitypayload")
def delete_records(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the file name, index, fields, and extracts container from the payload
    file = data.get("file")
    index = data.get("index")
    extracts_container = data.get("extracts-container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extracts container
    container_client = blob_service_client.get_container_client(container=extracts_container)

    records_to_delete = []

    for f in file:
        # Get a BlobClient object for the file
        blob_client = container_client.get_blob_client(blob=f)

        # Download the file as a string
        file_data = (blob_client.download_blob().readall()).decode('utf-8')

        # Load the file data as a JSON string
        file_data =  json.loads(file_data)

        # Insert the file data into the specified index
        records_to_delete.append(file_data)
    
    deleted_records = delete_documents_vector(records_to_delete, index)

    # Return the file name
    return deleted_records

@app.activity_trigger(input_name="activitypayload")
def convert_pdf_activity(activitypayload: str):
    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the index stem name from the payload
    container = data.get("container")
    filename = data.get("filename")

    updated_filename = filename.split('.')[0] + '.pdf'

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extracts container
    container_client = blob_service_client.get_container_client(container=container)

    # Get a BlobClient object for the file
    blob_client = container_client.get_blob_client(blob=filename)

    # Retrieve the file as a stream and load the bytes
    file_bytes = blob_client.download_blob().readall()

    try:

        pdf_bytes = convert_to_pdf_helper(file_bytes)

    except Exception as e:
        raise Exception(f"An error occurred: {e}")

    # Get a BlobClient object for the converted PDF file
    pdf_blob_client = container_client.get_blob_client(blob=updated_filename)

    # Upload the PDF file
    pdf_blob_client.upload_blob(pdf_bytes, overwrite=True)

    return json.dumps({'container': container, 'filename': updated_filename})

# Standalone Functions

# This function creates a new index
@app.route(route="create_new_index", auth_level=func.AuthLevel.FUNCTION)
def create_new_index(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the JSON payload from the request
    data = req.get_json()
    # Extract the index stem name and fields from the payload
    stem_name = data.get("index_stem_name")
    fields = data.get("fields")
    description = data.get("description")
    omit_timestamp = data.get("omit_timestamp")

    # fields = {
    #     "content": "string", "pagenumber": "int", "sourcefile": "string", "sourcepage": "string", "category": "string"
    # }

    # Call the function to create a vector index with the specified stem name and fields
    response = create_vector_index(stem_name, fields, omit_timestamp)

    # Return the response
    return response


@app.route(route="get_active_index", auth_level=func.AuthLevel.FUNCTION)
def get_active_index(req: func.HttpRequest) -> func.HttpResponse:
    # Get the JSON payload from the request
    data = req.get_json()
    # Extract the index stem name from the payload
    stem_name = data.get("index_stem_name")
    
    # Call the function to get the current index for the specified stem name
    latest_index, fields = get_current_index(stem_name)

    return latest_index


@app.activity_trigger(input_name="activitypayload")
def update_status_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    cosmos_container = os.environ['COSMOS_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    response = container.upsert_item(data)
    return True

@app.activity_trigger(input_name="activitypayload")
def create_status_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    cosmos_id = data.get("cosmos_id")
    cosmos_container = os.environ['COSMOS_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    data['id'] = cosmos_id

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    # response = container.read_item(item=cosmos_id)
    response = container.create_item(data)
    if type(response) == dict:
        return response
    return json.loads(response)

def create_profile_record(data):
    cosmos_container = os.environ['COSMOS_PROFILE_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    # response = container.read_item(item=cosmos_id)
    response = container.create_item(data)
    if type(response) == dict:
        return response

@app.activity_trigger(input_name="activitypayload")
def update_profile_record(activitypayload: str):

    data = json.loads(activitypayload)
    index_name = data.get("index_name")
    contains_data = data.get("contains_data")

    cosmos_container = os.environ['COSMOS_PROFILE_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    query = f'select * from c where c.id="{index_name}"'

    response = container.read_item(item=index_name, partition_key=index_name)

    response['contains_data'] = contains_data

    response = container.upsert_item(response)

    return response

def pdf_bytes_to_png_bytes(pdf_bytes, page_number=1):
    # Load the PDF from a bytes object
    pdf_stream = io.BytesIO(pdf_bytes)
    document = pymupdf.open("pdf", pdf_stream)

    # Select the page
    page = document.load_page(page_number - 1)  # Adjust for zero-based index

    # Render page to an image
    pix = page.get_pixmap()

    # Convert the PyMuPDF pixmap into a Pillow Image
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    # Create a BytesIO object for the output PNG
    png_bytes_io = io.BytesIO()

    # Save the image to the BytesIO object using Pillow
    img.save(png_bytes_io, "PNG")


    # Rewind the BytesIO object to the beginning
    png_bytes_io.seek(0)

    # Close the document
    document.close()

    # Return the BytesIO object containing the PNG image
    return png_bytes_io

@app.route(route="convert_file_to_pdf", auth_level=func.AuthLevel.FUNCTION)
def convert_file_to_pdf(req: func.HttpRequest) -> func.HttpResponse:
    # Get the JSON payload from the request
    data = req.get_json()
    # Extract the index stem name from the payload
    container = data.get("container")
    filename = data.get("filename")

    updated_filename = filename.split('.')[0] + '.pdf'

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extracts container
    container_client = blob_service_client.get_container_client(container=container)

    # Get a BlobClient object for the file
    blob_client = container_client.get_blob_client(blob=filename)

    # Retrieve the file as a stream and load the bytes
    file_bytes = blob_client.download_blob().readall()

    try:

        pdf_bytes = convert_to_pdf_helper(file_bytes)

    except Exception as e:
        raise Exception(f"An error occurred: {e}")

    # Get a BlobClient object for the converted PDF file
    pdf_blob_client = container_client.get_blob_client(blob=updated_filename)

    # Upload the PDF file
    pdf_blob_client.upload_blob(pdf_bytes, overwrite=True)

    return json.dumps({'container': container, 'filename': updated_filename})

def convert_to_pdf_helper(input_bytes):
    """
    Converts a document to PDF using LibreOffice and returns the PDF as a byte string.

    :param input_bytes: The content of the input document as bytes.
    :return: The PDF content as a byte string.
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdirname:

        input_path = os.path.join(tmpdirname, 'temp_input')
        output_path = os.path.join(tmpdirname, 'temp_input.pdf')
      
        # Write the input bytes to the temporary file
        with open(input_path, 'wb') as f:
            f.write(input_bytes)

        # Convert the document to PDF using LibreOffice
        command = [
            'libreoffice', '--headless', '--convert-to', 'pdf', '--outdir', tmpdirname, input_path
        ]
        out = None
        try:
            subprocess.run(command, check=True)
        except Exception as e:
            out = e
            print(f"An error occurred: {e}")

        print(os.listdir(tmpdirname))

        # Read the output PDF file as a byte string
        with open(output_path, 'rb') as f:
            pdf_bytes = f.read()
    
    # Return the PDF bytes
    return pdf_bytes

@app.route(route="pdf_ingest_speed_mode", auth_level=func.AuthLevel.FUNCTION)
def pdf_ingest_speed_mode(req: func.HttpRequest) -> func.HttpResponse:
    # Utility function to ingest a PDF document quickly for interactive chat purposes

    data = req.get_json()
    
    # Extract the index stem name from the payload
    filename = data.get("filename")
    filedata = data.get("filedata")
    filedata = base64.b64decode(filedata)
    container = data.get("container")
    index_name = data.get("index_name")
    entra_id = data.get("entra_id")
    session_id = data.get("session_id")

    # Write file to blob storage
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    container_client = blob_service_client.get_container_client(container=container)
    blob_client = container_client.get_blob_client(blob=filename)
    blob_client.upload_blob(filedata, overwrite=True)

    # Process with document intelligence and retrieve formatted data
    analysis = analyze_pdf(filedata)
    page_map = extract_results(analysis, filename)

    def process_pages(page, entra_id, session_id, filename):
        content = page[1]
        embeddings = generate_embeddings(content)
        id_str = content + filename + session_id
        id = hashlib.sha256(id_str.encode('utf-8')).hexdigest()
        return {
            'id': id,
            'content': content,
            'embeddings': embeddings,
            'sourcefile': filename,
            'pagenumber': page[0],
            # 'user_id': entra_id,
            # 'session_id': session_id
        }

    # Page-wise chunks
    with concurrent.futures.ThreadPoolExecutor() as executor:
        chunked_docs = list(executor.map(process_pages, page_map, [entra_id]*len(page_map), [session_id]*len(page_map), [filename]*len(page_map)))

    # Insert into index
    insert_documents_vector(chunked_docs, index_name)

    return json.dumps({'index': index_name})

# Extract only text from a PDF, PPTX, Word, HTML, PNG, or JPG file
@app.route(route="ocr_doc_text_speed_mode", auth_level=func.AuthLevel.FUNCTION)
def ocr_doc_text_speed_mode(req: func.HttpRequest) -> func.HttpResponse:
    # Utility function to ingest a PDF document quickly for interactive chat purposes

    data = req.get_body()

    # Process with document intelligence and retrieve formatted data
    text = read_document(data)
    
    return json.dumps({'text': text})

# Extract only text from PDF documents
@app.route(route="read_pdf_text_speed_mode", auth_level=func.AuthLevel.FUNCTION)
def read_pdf_text_speed_mode(req: func.HttpRequest) -> func.HttpResponse:
    # Utility function to ingest a PDF document quickly for interactive chat purposes

    data = req.get_body()
    
    pdf_document = pymupdf.open('pdf', BytesIO(data))

    extracted_text = ""

    # Iterate over all the pages
    for page in range(len(pdf_document)):
        # Get the text from the page
        page_text = pdf_document[page].get_text() + '\n'
        # Append the text to the extracted_text string
        extracted_text += page_text

    return json.dumps({'text': extracted_text})