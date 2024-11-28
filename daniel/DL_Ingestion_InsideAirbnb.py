# Databricks notebook source
# MAGIC %md
# MAGIC ## Part 1: Ingestion of Data via URL + Unzipping .gz and Loading on Azure Blob Storage

# COMMAND ----------

import requests
import gzip
from azure.storage.blob import BlobServiceClient
import io

# COMMAND ----------

# MAGIC %md
# MAGIC Connection settings and establishment of connection to blob storage:

# COMMAND ----------

# Azure Blob Storage
connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakestoragerentscape;AccountKey=w6Edf3np1A18vQIei31unvKWjGpyDUBqexvVauAwCeqOmnF1Bq7WsIEVplSEW+hT0q4ZzDi2KNh4+AStrOcI6g==;EndpointSuffix=core.windows.net"
container_name = "rentscape-blob"

# COMMAND ----------

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Dictionary containing paths to URLs of .gz files and target location in blob storage:

# COMMAND ----------

# Define the URLs and corresponding blob names for each file
file_info = [
    {
        "url": "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/data/listings.csv.gz",
        "blob_name": "barcelona_listings.csv"
    },
    {
        "url": "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/data/reviews.csv.gz",
        "blob_name": "barcelona_reviews.csv"
    },
    {
        "url": "https://data.insideairbnb.com/czech-republic/prague/prague/2024-06-24/data/listings.csv.gz",
        "blob_name": "prague_listings.csv"
    },
    {
        "url": "https://data.insideairbnb.com/czech-republic/prague/prague/2024-06-24/data/reviews.csv.gz",
        "blob_name": "prague_reviews.csv"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC This code processes a list of URLs pointing to .gz compressed CSV files, decompresses them, and uploads the decompressed CSV data to Azure Blob Storage:
# MAGIC
# MAGIC - Download the .gz File: For each file URL in file_info, it sends a request to download the file. If the download is successful, it proceeds; otherwise, it skips to the next file.
# MAGIC
# MAGIC - Decompress the .gz Content: The downloaded content is opened in text mode ('rt') with UTF-8 encoding specified. Using gzip.open() with io.BytesIO() allows decompression directly in memory without saving the file locally. If there’s an error during decompression, it moves to the next file.
# MAGIC
# MAGIC - Upload to Azure Blob Storage: The decompressed CSV data (stored as plain text) is then uploaded to Azure Blob Storage with a specified blob_name using the Azure Blob client. If an error occurs during upload, the code logs it and proceeds with the next file.
# MAGIC
# MAGIC Finally, after all files are processed, it prints a message indicating completion. This setup efficiently handles multiple files, using in-memory processing and error handling for each step.

# COMMAND ----------

# Process each URL and upload the decompressed CSV to Azure Blob Storage
for file in file_info:
    url = file["url"]
    blob_name = file["blob_name"]
    
    # Step 1: Download the .gz file from the URL
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Downloaded {blob_name} successfully.")
    else:
        print(f"Failed to download {blob_name}. Status code: {response.status_code}")
        continue

    # Step 2: Decompress the .gz content with utf-8 encoding
    try:
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8', errors='ignore') as f:
            csv_content = f.read()  # Read the decompressed CSV content as text
    except Exception as e:
        print(f"Failed to decompress {blob_name}: {e}")
        continue

    # Step 3: Upload the decompressed CSV content to Azure Blob Storage
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_content, overwrite=True)
        print(f"Uploaded decompressed CSV content to {blob_name} in Azure Blob Storage.")
    except Exception as e:
        print(f"Failed to upload {blob_name}: {e}")

print("File upload process completed for all files.")

# COMMAND ----------


