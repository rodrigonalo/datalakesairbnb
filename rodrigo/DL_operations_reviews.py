# Databricks notebook source
# Install or upgrade azure-storage-blob, pandas, and openai packages
%pip install azure-storage-blob pandas openai

# COMMAND ----------

# Upgrade typing_extensions to ensure compatibility
%pip install --upgrade openai typing_extensions
%restart_python

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI 


# COMMAND ----------

# Define your Azure Blob Storage connection string and container name
connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakestoragerentscape;AccountKey=w6Edf3np1A18vQIei31unvKWjGpyDUBqexvVauAwCeqOmnF1Bq7WsIEVplSEW+hT0q4ZzDi2KNh4+AStrOcI6g==;EndpointSuffix=core.windows.net"
container_name = "rentscape-blob"

# Initialize BlobServiceClient and ContainerClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Function to download blob and load it into a DataFrame
def download_and_load_blob(blob_name):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.StringIO(blob_data.decode('utf-8')))
        
        # Generate a variable name from the blob name, making it a valid Python variable
        variable_name = blob_name.replace('/', '_').replace('.csv', '')
        
        # Set the DataFrame to a global variable with this name
        globals()[variable_name] = df
        print(f"Loaded {blob_name} into DataFrame: {variable_name}")
    except Exception as e:
        print(f"Failed to process {blob_name}: {e}")

# List of blobs to process (only CSV files containing "listings" in the title)
blob_names = [
    blob.name for blob in container_client.list_blobs() 
    if blob.name.endswith('.csv') and 'reviews' in blob.name
]

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=10) as executor:
    # Submit tasks for each blob
    futures = {executor.submit(download_and_load_blob, blob_name): blob_name for blob_name in blob_names}
    
    # Process results as they complete
    for future in as_completed(futures):
        future.result()  # Ensures all processing completes

# Now, each CSV file is loaded into a DataFrame with a variable name based on its file name

# COMMAND ----------

# MAGIC %md Topic classification

# COMMAND ----------

# MAGIC %md
# MAGIC Language of the comments

# COMMAND ----------

from openai import OpenAI 
client = OpenAI(api_key='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')

# Initialize a counter variable
execution_count = 0
total_rows = len(prague_reviews)  # Total number of rows in the DataFrame

def detectar_idioma(comment):
    global execution_count  # Use the global counter
    execution_count += 1  # Increment the counter for each processed row
    remaining_rows = total_rows - execution_count  # Calculate remaining rows
    
    # Check if the comment is missing or empty
    if not isinstance(comment, str) or not comment.strip():
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Comment: {comment}\nDetected Language: N/A\n")
        return "N/A"
    
    # Construct the prompt
    prompt = f"Identify the language of the following text: '{comment}'. Respond with only the name of the language, nothing else."

    try:
        # Make the API call to OpenAI
        completion = client.chat.completions.create(
            model="gpt-4o-mini",  # Replace with your preferred model
            messages=[
                {"role": "system", "content": "You are an expert."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract the language from the response
        language = completion.choices[0].message.content.strip()

        # Print the comment, detected language, execution count, and remaining rows
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Comment: {comment}\nDetected Language: {language}\n")

        return language

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error"

# Apply the language detection function to the 'comments' column and create a new 'language' column
prague_reviews['openai_language'] = prague_reviews['comments'].apply(detectar_idioma)
barcelona_reviews['openai_language'] = barcelona_reviews['comments'].apply(detectar_idioma)


