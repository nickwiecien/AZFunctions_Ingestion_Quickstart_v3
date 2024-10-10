import datetime
from pathlib import Path
import time
import json
import openai
import os
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
import requests
import re
import logging

def create_openai_client():
    openai.api_type = "azure"
    openai.api_base = os.environ['AOAI_ENDPOINT']
    openai.api_version = "2023-03-15-preview"
    
    if('AOAI_KEY' in os.environ):
        openai.api_key = os.environ['AOAI_KEY']
    
        client = AzureOpenAI(
            azure_endpoint=os.environ['AOAI_ENDPOINT'], 
            api_key=os.environ['AOAI_KEY'], 
            api_version="2023-03-15-preview"
        )
    else:
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(
                managed_identity_client_id=os.environ['UserAssignedManagedIdentityClientId'],
            ),
            [os.environ['AzureServicePrincipalOpenAIAudience']]
        )
        client = AzureOpenAI(
            azure_endpoint=os.environ['AOAI_ENDPOINT'], 
            azure_ad_token_provider=token_provider, 
            api_version="2023-03-15-preview",
            default_headers={"Ocp-Apim-Subscription-Key": os.environ['OcpApimSubscriptionKey']}
        )

    return client

def generate_embeddings(text, model_name=None):
    """
    Generates embeddings for the given text using the specified embeddings model provided by OpenAI.

    Args:
        text (str): The text to generate embeddings for.

    Returns:
        embeddings (list): The embeddings generated for the given text.
    """

    # Configure OpenAI with Azure settings
    client = create_openai_client()

    embedding_model = os.environ['AOAI_EMBEDDINGS_MODEL']
    if model_name is not None:
        embedding_model = model_name

    # Initialize variable to track if the embeddings have been processed
    processed = False
    # Attempt to generate embeddings, retrying on failure
    while not processed:
        try:
            # Make API call to OpenAI to generate embeddings
            response = client.embeddings.create(input=text, model=embedding_model)
            processed = True
        except Exception as e:  # Catch any exceptions and retry after a delay
            logging.error(e)
            print(e)

            # Added to handle exception where passed context exceeds embedding model's context window
            if 'maximum context length' in str(e):
                text = text[:int(len(text)*0.95)]

            time.sleep(5)

    # Extract embeddings from the response
    embeddings = response.data[0].embedding
    return embeddings

def get_transcription(filename):
    """
    Transcribes the given audio file using the specified transcription model provided by OpenAI.

    Args:
        filename (str): The path to the audio file to transcribe.

    Returns:
        transcript (str): The transcription of the audio file.
    """

    # Configure OpenAI with Azure settings
    openai.api_type = "azure"
    openai.api_base = os.environ['AOAI_WHISPER_ENDPOINT']
    openai.api_key = os.environ['AOAI_WHISPER_KEY']
    openai.api_version = "2023-09-01-preview"

    # Specify the model and deployment ID for the transcription
    model_name = os.environ['AOAI_WHISPER_MODEL_TYPE'] # "whisper-1"
    deployment_id =  os.environ['AOAI_WHISPER_MODEL']

    # Specify the language of the audio
    audio_language="en"

    # Initialize an empty string to store the transcript
    transcript = ''

    # Initialize variable to track if the audio has been transcribed
    transcribed = False

    client = AzureOpenAI(
        api_key=os.environ['AOAI_WHISPER_KEY'], azure_endpoint=os.environ['AOAI_WHISPER_ENDPOINT'], api_version="2024-02-01"
    )

    # Attempt to transcribe the audio, retrying on failure
    while not transcribed:
        try:
            result = client.audio.transcriptions.create(
                file=open(filename, "rb"),            
                model=deployment_id
            )
            transcript = result.text
            transcribed = True
        except Exception as e:  # Catch any exceptions and retry after a delay
            if 'Maximum content size limit' in str(e):
                raise e
            logging.error(e)
            time.sleep(10)
            pass

    # If a transcript was generated, return it
    if len(transcript)>0:
        return transcript

    # If no transcript was generated, raise an exception
    raise Exception("No transcript generated")

def classify_image(b64_image_bytes):
    classification_msg = """
    You review images of individual document pages and determine if there is non-textual 
    visual content such as charts, graphs, diagrams, infographics, reference photographs, 
    screenshots, 3D models, or flowcharts.

    Return only TRUE or FALSE for the provided image.
    """

    user_content = {
        "role": "user",
        "content": [
            {
            "type": "image_url",
            "image_url": {
                    "url": f"data:image/jpeg;base64,{b64_image_bytes}"
                     , "detail": "high"
                }
            }  
        ]
    }

    messages = [ 
            { "role": "system", "content": classification_msg }, 
            user_content
    ]

    api_base = os.environ['AOAI_ENDPOINT']
    api_key = os.environ['AOAI_KEY']
    deployment_name = os.environ['AOAI_GPT_VISION_MODEL']

    base_url = f"{api_base}openai/deployments/{deployment_name}" 
    headers = {   
        "Content-Type": "application/json",   
        "api-key": api_key 
    } 
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview" 
    data = { 
        "messages": messages, 
        "temperature": 0.0,
        "top_p": 0.95,
        "max_tokens": 50
    }   

    # Make the API call   
    processed = False
    out_str = ''

    while not processed:
        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(data)) 
            resp_str = response.json()['choices'][0]['message']['content']
            out_str = resp_str
            processed = True
        except Exception as e:
            if 'exceeded token rate' in str(e).lower():
                time.sleep(5)
            else:
                processed = True
                
    if 'true' in out_str.lower():
        return True
    else:
        return False
    

def analyze_image(b64_image_bytes):
    classification_msg = """
    You review images of individual document pages and describe non-textual visual content such as charts, 
    graphs, diagrams, infographics, reference photographs, screenshots, 3D models, or flowcharts.

    Your response should be a JSON object describing the essential non-textual visual content on the page. 
    The key should refer to the object and the value should be a detailed description.
    """

    user_content = {
        "role": "user",
        "content": [
            {
            "type": "image_url",
            "image_url": {
                    "url": f"data:image/jpeg;base64,{b64_image_bytes}"
                     , "detail": "high"
                }
            }  
        ]
    }

    messages = [ 
            { "role": "system", "content": classification_msg }, 
            user_content
    ]

    api_base = os.environ['AOAI_ENDPOINT']
    api_key = os.environ['AOAI_KEY']
    deployment_name = os.environ['AOAI_GPT_VISION_MODEL']

    base_url = f"{api_base}openai/deployments/{deployment_name}" 
    headers = {   
        "Content-Type": "application/json",   
        "api-key": api_key 
    } 
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview" 
    data = { 
        "messages": messages, 
        "temperature": 0.0,
        "top_p": 0.95,
        "max_tokens": 800
    }   

    # Make the API call   
    processed = False
    out_str = ''

    while not processed:
        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(data)) 
            resp_str = response.json()['choices'][0]['message']['content']
            out_str = resp_str
            processed = True
        except Exception as e:
            if 'exceeded token rate' in str(e).lower():
                time.sleep(5)
            else:
                processed = True
                break

    # Regex pattern to match the outer-most JSON object
    pattern = re.compile(r'\{.*\}', re.DOTALL)
    # Search for the JSON object
    match = pattern.search(out_str)

    # Extract and print the JSON object if found
    if match:
        out_str = match.group(0)

    return out_str
        

def generate_qna_pair_helper(content):
    sys_msg = """You are a helpful AI assistant who reviews snippets of documents and generates a question and answer pair that can be UNIQUELY answered by the content within the provided document. 
    The question-answer pair you generate should be specific to the underlying information in the provided documents, rather than a question about the document itself. 
    To the extent possible, these questions should cover broader ideas.
    Ideally, these questions should be answerable without an individual having the document directly in front of them.
    For instance, ask 'What are the emerging trends in AI in 2024?' rather than 'What are the key AI trends listed in the document?' 

    Your output format should be a JSON object with the following structure:

    {
        'question': '',
        'answer':''
    }
    """
    user_msg = f"""Generate a question/answer pair based on the following content.

    ## CONTENT: {content}
    """
    
    messages = [ 
            { "role": "system", "content": sys_msg }, 
            {"role": "user", "content": user_msg}
    ]

    api_base = os.environ['AOAI_ENDPOINT']
    api_key = os.environ['AOAI_KEY']
    deployment_name = os.environ['AOAI_GPT_VISION_MODEL']

    base_url = f"{api_base}openai/deployments/{deployment_name}" 
    headers = {   
        "Content-Type": "application/json",   
        "api-key": api_key 
    } 
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview" 
    data = { 
        "messages": messages, 
        "temperature": 0.0,
        "top_p": 0.95,
        "max_tokens": 500,
        "response_format": {"type": "json_object"}
    }   

    # Make the API call   
    processed = False
    out_str = ''

    while not processed:
        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(data)) 
            resp_str = response.json()['choices'][0]['message']['content']
            out_str = resp_str
            processed = True
        except Exception as e:
            if 'exceeded token rate' in str(e).lower():
                time.sleep(5)
            else:
                processed = True
                
    return json.loads(out_str)
    