# Generative AI Ingestion Functions - Quickstart

### Contains new functionality for image analysis and variable document chunking with optional overlap, multi-modal ingestion (support for audio, video, and non-PDF file formats)

## Project Overview

The 'Azure Functions Quickstart - Generative AI Data Ingestion Functions' project is an Azure Durable Functions project aimed at streamlining the process of ingesting, chunking, and vectorizing PDF-based documents. These processes are critical for indexing and utilizing data in Retrieval Augmented Generation (RAG) patterns within Generative AI applications.

By leveraging Azure Durable Functions, the project orchestrates the complex workflows involved in data processing, ensuring efficiency and scalability. It includes capabilities for creating and managing Azure AI Search indexes, updating index aliases for deployment strategies, and indexing large volumes of pre-processed documents in bulk.

## Features
- **Ingestion and Chunking**: Automated breakdown of documents and audio files into chunks for easier processing.
- **Vectorization**: Transformation of textual and auditory information into vector embeddings suitable for AI models.
- **Index Management**: Tools for creating and updating Azure AI Search indexes to optimize data retrieval.
- **Workflow Orchestration**: Utilization of Durable Functions to coordinate and manage data processing tasks.
- **Postman Collection**: Sample postman collection (``) demonstrating calling of all functions.

## Getting Started

### Prerequisites (Required Services)
- An active Azure subscription.
- Azure Function App.
- Azure Storage Account.
- Azure Cognitive Services, including Document Intelligence and Azure OpenAI.
- Azure AI Search Service instance.
- Azure Cosmos DB
- Azure Data Factory

### Installation
1. Clone the repository to your desired environment.
2. Install Azure Functions Core Tools if not already available.
3. In the project directory, install dependencies with `pip install -r requirements.txt`.

### Configuration
Configure the environment variables in your Azure Function App settings as follows:

| Variable Name                | Description                                               |
|------------------------------|-----------------------------------------------------------|
| `STORAGE_CONN_STR`           | Azure Storage account connection string                   |
| `DOC_INTEL_ENDPOINT`         | Endpoint for Azure Document Intelligence service          |
| `DOC_INTEL_KEY`              | Key for Azure Document Intelligence service               |
| `AOAI_KEY`                   | Key for Azure OpenAI service                              |
| `AOAI_ENDPOINT`              | Endpoint for Azure OpenAI service                         |
| `AOAI_EMBEDDINGS_MODEL`      | Model for generating embeddings with Azure OpenAI         |
| `AOAI_EMBEDDINGS_DIMENSIONS`      | Number of vector dimensions associated with the Azure OpenAI embedding model (1536 for `text-embedding-ada-002`)         |
| `AOAI_GPT_VISION_MODEL`      | Azure OpenAI GPT-4 model with vision support (GPT-4o or GPT-4-Turbo w/ Vision) |
| `SEARCH_ENDPOINT`            | Endpoint for Azure AI Search service                      |
| `SEARCH_KEY`                 | Key for Azure AI Search service                           |
| `SEARCH_SERVICE_NAME`        | Name of the Azure AI Search service instance              |
| `COSMOS_ENDPOINT`        | Endpoint for the Azure Cosmos DB instance              |
| `COSMOS_KEY`        | Key for the Azure Cosmos DB instance              |
| `COSMOS_DATABASE`        | Name of the Azure Cosmos DB database which will hold status records              |
| `COSMOS_CONTAINER`        | Name of the Azure Cosmos DB collection which will hold status records              |
| `COSMOS_PROFILE_CONTAINER`        | Name of the Azure Cosmos DB collection which will index profile records              |

<i>Note: review the `sample.settings.json` to create a `local.settings.json` environment file for local execution.</i>


## Deployment

### Container Deployment (Recommended)

Build and push the container image to an Azure Container Registry, then update your app configuration with the published image.

```
docker login <ACRNAME>.azurecr.io
docker build . -t <ACRNAME>.azurecr.io/ingestionfunctions:1
docker push <ACRNAME>.azurecr.io/ingestionfunctions:1
```

Configuration update using the Azure CLI - Note: this can be performed manually.
```
az functionapp config container set \
  --name <function-app-name> \
  --resource-group <resource-group-name> \
  --docker-custom-image-name <ACRNAME>.azurecr.io/ingestionfunctions:1 \
  --docker-registry-server-url <registry-url> \
  --docker-registry-server-user <username> \
  --docker-registry-server-password <password>
```

### CLI Deployment

The code contained within this repo can be deployed to your Azure Function app using the [deployment approaches outlined in this document](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies?tabs=windows). For initial deployment, we recommend using either the [Azure Functions Extension for VS Code](https://learn.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=node-v4%2Cpython-v2%2Cisolated-process&pivots=programming-language-python) or the Azure Functions Core tools locally:

```
# Azure Functions Core Tools Deployment
func azure functionapp publish <YOUR-FUNCTION-APP-NAME>
func azure functionapp publish <YOUR-FUNCTION-APP-NAME> --publish-settings-only
```

## Deployment using CLI into existing Container Apps Environment without existing Function App

1. Build & push the Docker image as indicated above to the Container Registry.

1. Create a new Container Apps Environment workload profile to run your Function Apps in (the default Consumption profile is likely insufficient for this workload)

```shell
az containerapp env workload-profile add -g <resource-group-name> -n <container-apps-environment> --workload-profile-name func --workload-profile-type D4 --min-nodes 1 --max-nodes 40
```

1. Deploy your Function App to this new workload profile.

```shell
az functionapp create --name <function-app-name> --storage-account <storage-account-name> --environment <container-app-environment-name> --workload-profile-name func --resource-group <resource-group-name> --functions-version 4 --runtime python --image <container-registry-login-server>/ingestionfunction:1
```

1. Update the configuration values with your environment. You can create a local JSON file (`container-app.settings.json`, modeled on the `sample.container-app.settings` file)with all of these values and reference it in the following command. You will need to copy the various values for this file from the various Azure servies in the Portal.

```shell
az functionapp config appsettings set -g <resource-group-name> -n <function-app-name> --settings "@container-app.settings.json"
```

## Utilization & Testing

Shown below are some of the common calls the created functions for creating, and populating an Azure AI Search index using files uploaded to Azure Blob Storage.

### Create Index (Manual Execution)

See `Create_Index` in Postman collection

POST to `https://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/create_new_index?code=<YOUR-FUNCTION-KEY>`
```
{
    "index_stem_name": "rag-index",
    "fields": {"content": "string", "pagenumber": "int", "sourcefile": "string", "sourcepage": "string", "category": "string"},
    "description": "A detailed description of what types of documents, and their contents, will be added to the newly created index",
    "dimensions": 3072 # Number of dimensions associated with vector embedding model 
}
```

### Get Current Index 

See `Get_Active_Index` in Postman collection

POST to `https://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/get_active_index?code=<YOUR-FUNCTION-KEY>`
```
# Sample Payload
{
    "index_stem_name":"rag-index"
}
```


### Trigger PDF Ingestion

#### Arguments
| Parameter               | Type    | Description                                                                                   |  
|-------------------------|---------|-----------------------------------------------------------------------------------------------|  
| `source_container`      | `str`   | The name of the source container which contains knowledgebase documents.                      |  
| `extract_container`     | `str`   | The name of the extract container where processed results should be stored.                   |  
| `prefix_path`           | `str`   | The prefix path for the file (or files) to be processed.                                      |  
| `index_name`            | `str`   | The name of the index to which the documents will be added.                                   |  
| `automatically_delete`  | `bool`  | A flag indicating whether to automatically delete intermediate data.                          |  
| `analyze_images`        | `bool`  | A flag indicating whether to analyze images for embedded visuals, and append descriptions to processed chunks. |  
| `chunking_strategy`    | `str`  | A string indicating what chunking strategy to use. Supported options are: `semantic` (recommended), `pagewise`, and `fixed_size` |  
| `max_chunk_size`            | `int`   | The size of the chunks to be created in tokens. For semantic chunking this is the largest allowed chunk size.                                              |  
| `chunk_overlap`               | `int`   | The amount of overlap between chunks in tokens (used only for `fixed_size` chunking).                                               |  
| `embedding_model`       | `str`   | The name of the Azure OpenAI embedding model deployment                                       | 
| `cosmos_logging`        | `bool`  | A flag indicating whether or not to publish logs to Cosmos DB                                 |  


See `Trigger_PDF_Ingestion` in Postman collection.

POST to `https://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/orchestrators/pdf_orchestrator?code=<YOUR-FUNCTION-KEY>`
```
# Sample Payload
{
    "source_container": "<SOURCE_STORAGE_CONTAINER>",
    "extract_container": "<EXTRACT_STORAGE_CONTAINER>",
    "prefix_path": "<UPLOADED_FILE_PATH>",
    "index_name": "<YOUR_INDEX_NAME>",
    "automatically_delete": <TRUE_OR_FALSE>,
    "analyze_images": <TRUE_OR_FALSE>,
    "chunking_strategy": "<SELECTED_CHUNKING_STRATEGY>",
    "max_chunk_size": <MAX_CHUNK_SIZE_IN_TOKENS>,
    "chunk_overlap": <OVERLAP_SIZE_IN_TOKENS>,
    "embedding_model": "<AOAI_EMBEDDING_MODEL_DEPLOYMENT_NAME>",
    "cosmos_logging": "<TRUE_OR_FALSE>",
}
```

### 🧪 Preliminary Testing 🧪
To test your deployment and confirm everything is working as expected, use the [step-by-step testing guide](guides/RTX_IngestionFunctionsDeploymentGuide.pdf) linked in this repo!

## Functions Deep Dive

### Orchestrators
The project contains orchestrators tailored for specific data types:
- `pdf_orchestrator`: Orchestrates the processing of PDF files, including chunking, extracting text & tables, generating embeddings, insertion into an Azure AI Search index, and cleanup of staged processing data.
- `non_pdf_orchestrator`: Orchestrates the processing of unstructured text files not in PDF format (Word, PowerPoint, HTML, etc.) including all steps listed above.
- `audio_video_orchestrator`: Orchestrates the processing of audio & video files, including transcript generation using OpenAI's whisper model followed by chunking, extracting text & tables, generating embeddings, insertion into an Azure AI Search index, and cleanup of staged processing data.
- `main_orchestrator`: General purpose orchestrator which determines file types and routes to the appropriate ingestion orchestrator.
- `delete_documents_orchestrator`: Orchestrates the deletion of processed documents within your Azure Storage Account and Azure AI Search Index.

### Activities
The orchestrators utilize the following activities to perform discrete tasks:
- `get_source_files`: Retrieves a list of files from a specified Azure Storage container based on a user-provided prefix path.
- `delete_source_files`: Deletes files from a specified Azure Storage container based on a user-provided prefix path.
- `check_containers`: Ensures that all containers for storing processed intermediate results are present in Azure storage.
- `split_pdf_files`: Splits PDF files into individual pages and stores them as separate files.
- `process_pdf_with_document_intelligence`: Processes PDF chunks using Azure Document Intelligence and extracts relevant data.
- `analyze_pages_for_embedded_visuals`: Uses a GPT-4 vision model to describe embedded visuals within document pages for inclusion in indexed content.
- `chunk_extracts`: Breaks documents into page-wise chunks, or chunks of token size X with overlap of Y
- `generate_extract_embeddings`: Generates vector embeddings for the processed text data
- `insert_record`: Inserts processed data records into the Azure AI Search index.
- `delete_record`: Deletes processed data from from the Azure AI Search index.
- `create_status_record`: Adds an entry into Cosmos DB for logging and monitoring
- `update_status_record`: Updates an entry in Cosmos DB with processing and/or error details.


### Standalone Functions (HTTP Triggered)
In addition to orchestrators and activities, the project includes standalone functions for index management which can be triggered via a HTTP request:
- `create_new_index`: Creates a new Azure AI Search index with the specified fields.
- `get_active_index`: Retrieves the most current Azure AI Search index based on a user-provided root name.

---
