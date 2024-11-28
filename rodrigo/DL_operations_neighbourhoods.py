# Databricks notebook source
# Upgrade/install required packages
#%pip install --upgrade typing_extensions azure-storage-blob pandas openai

# Restart Python to apply changes (if necessary)
#%restart_python


# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI 

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakestorageentscape;AccountKey=w6EdF3np1A8vQIe13iunvKwjdGpyDU8qexvVauAwCeqOmF1Bq7HsIEYp15BWHtDq4zZDi2kNh4A+Str0cT6g==;EndpointSuffix=core.windows.net"
container_name = "rentscape-blob"

# Initialize BlobServiceClient and ContainerClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Test connection by listing blobs
try:
    blobs = container_client.list_blobs()
    print(f"Connected successfully. Found blobs: {[blob.name for blob in blobs]}")
except Exception as e:
    print(f"Connection failed: {e}")


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
    if blob.name.endswith('.csv') and 'listings' in blob.name
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

# MAGIC %md
# MAGIC Language of the comments

# COMMAND ----------

# Process Prague Listings
unique_neighbourhoods_prague = prague_listings['host_neighbourhood'].unique()
unique_neighbourhoods_prague_df = pd.DataFrame(unique_neighbourhoods_prague, columns=['Unique_Neighbourhoods'])
unique_neighbourhoods_prague_df = unique_neighbourhoods_prague_df.sort_values(by='Unique_Neighbourhoods', ascending=True)
unique_neighbourhoods_prague_df.reset_index(drop=True, inplace=True)


# Process Barcelona Listings
unique_neighbourhoods_barcelona = barcelona_listings['host_neighbourhood'].unique()
unique_neighbourhoods_barcelona_df = pd.DataFrame(unique_neighbourhoods_barcelona, columns=['Unique_Neighbourhoods'])
unique_neighbourhoods_barcelona_df = unique_neighbourhoods_barcelona_df.sort_values(by='Unique_Neighbourhoods', ascending=True)
unique_neighbourhoods_barcelona_df.reset_index(drop=True, inplace=True)

# COMMAND ----------

from openai import OpenAI
import pandas as pd

# Initialize OpenAI client
client = OpenAI(api_key='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')

# Initialize a counter variable
execution_count = 0
total_rows = len(unique_neighbourhoods_df)  # Total number of rows in the DataFrame being processed

# Function to assess neighborhoods with regulations
def assess_neighbourhood_with_regulations(neighbourhood):
    global execution_count  # Use the global counter
    execution_count += 1  # Increment the counter for each processed row

    # Calculate remaining rows
    remaining_rows = total_rows - execution_count

    # Check if the neighbourhood name is missing or empty
    if not isinstance(neighbourhood, str) or not neighbourhood.strip():
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Neighbourhood: {neighbourhood}\nExposure Score: N/A\nRising Star: N/A\nRegulations: N/A\n")
        return "N/A", "N/A", "N/A"

    # Construct the enhanced prompt
    prompt = (
        f"Analyze the neighborhood '{neighbourhood}' in the context of Airbnb listings and news. "
        f"1) From 1 to 5, rate how much this neighborhood has appeared in news articles over the last few years regarding Airbnb being a problem. "
        f"2) Is this area a 'Rising Star' in terms of popularity? Respond with 'Yes' or 'No'. "
        f"3) Are there any local regulations on short-term rentals? Respond with 'Yes' or 'No'. "
        f"Provide the answers in the format: Exposure Score: <number>, Rising Star: <Yes/No>, Regulations: <Yes/No>. Do not include any additional text."
    )

    try:
        # Make the API call to OpenAI
        completion = client.chat.completions.create(
            model="gpt-4o-mini",  # Replace with your preferred model
            messages=[
                {"role": "system", "content": "You are an expert in analyzing urban issues related to tourism and housing."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract the response and split it into the three outputs
        response = completion.choices[0].message.content.strip()
        exposure_score, rising_star, regulations = response.replace(" ", "").split(",")
        exposure_score = exposure_score.split(":")[1]
        rising_star = rising_star.split(":")[1]
        regulations = regulations.split(":")[1]

        # Print the neighbourhood, scores, execution count, and remaining rows
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Neighbourhood: {neighbourhood}\nExposure Score: {exposure_score}\nRising Star: {rising_star}\nRegulations: {regulations}\n")

        return exposure_score, rising_star, regulations

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error", "Error", "Error"

# Apply the function to each row of 'Unique_Neighbourhoods' and update the DataFrame
unique_neighbourhoods_df[['Exposure_Score', 'Rising_Star', 'Regulations']] = unique_neighbourhoods_df['Unique_Neighbourhoods'].apply(
    lambda neighbourhood: pd.Series(assess_neighbourhood_with_regulations(neighbourhood))
)

# Display the updated DataFrame
unique_neighbourhoods_df

# COMMAND ----------

from openai import OpenAI
import pandas as pd

# Initialize OpenAI client
client = OpenAI(api_key='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')

# Initialize a counter variable
execution_count = 0

# General function to assess neighborhoods with regulations
def assess_neighbourhood_with_regulations(neighbourhood, total_rows):
    global execution_count  # Use the global counter
    execution_count += 1  # Increment the counter for each processed row

    # Calculate remaining rows
    remaining_rows = total_rows - execution_count

    # Check if the neighbourhood name is missing or empty
    if not isinstance(neighbourhood, str) or not neighbourhood.strip():
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Neighbourhood: {neighbourhood}\nExposure Score: N/A\nRising Star: N/A\nRegulations: N/A\n")
        return "N/A", "N/A", "N/A"

    # Construct the enhanced prompt
    prompt = (
        f"Analyze the neighborhood '{neighbourhood}' in the context of Airbnb listings and news. "
        f"1) From 1 to 5, rate how much this neighborhood has appeared in news articles over the last few years regarding Airbnb being a problem. "
        f"2) Is this area a 'Rising Star' in terms of popularity? Respond with 'Yes' or 'No'. "
        f"3) Are there any local regulations on short-term rentals? Respond with 'Yes' or 'No'. "
        f"Provide the answers in the format: Exposure Score: <number>, Rising Star: <Yes/No>, Regulations: <Yes/No>. Do not include any additional text."
    )

    try:
        # Make the API call to OpenAI
        completion = client.chat.completions.create(
            model="gpt-4o-mini",  # Replace with your preferred model
            messages=[
                {"role": "system", "content": "You are an expert in analyzing urban issues related to tourism and housing."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract the response and split it into the three outputs
        response = completion.choices[0].message.content.strip()
        exposure_score, rising_star, regulations = response.replace(" ", "").split(",")
        exposure_score = exposure_score.split(":")[1]
        rising_star = rising_star.split(":")[1]
        regulations = regulations.split(":")[1]

        # Print the neighbourhood, scores, execution count, and remaining rows
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Neighbourhood: {neighbourhood}\nExposure Score: {exposure_score}\nRising Star: {rising_star}\nRegulations: {regulations}\n")

        return exposure_score, rising_star, regulations

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error", "Error", "Error"


# Function to process a DataFrame
def process_neighbourhoods(df):
    global execution_count
    execution_count = 0  # Reset the counter for each DataFrame
    total_rows = len(df)  # Calculate total rows for the current DataFrame

    # Apply the assessment function to each neighbourhood
    df[['Exposure_Score', 'Rising_Star', 'Regulations']] = df['Unique_Neighbourhoods'].apply(
        lambda neighbourhood: pd.Series(assess_neighbourhood_with_regulations(neighbourhood, total_rows))
    )
    return df


# Process Prague Neighbourhoods
print("Processing Prague Neighbourhoods...")
unique_neighbourhoods_prague_df = process_neighbourhoods(unique_neighbourhoods_prague_df)
print(unique_neighbourhoods_prague_df)

# Process Barcelona Neighbourhoods
print("\nProcessing Barcelona Neighbourhoods...")
unique_neighbourhoods_barcelona_df = process_neighbourhoods(unique_neighbourhoods_barcelona_df)
print(unique_neighbourhoods_barcelona_df)


# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd

# SAS token and container URL
sas_token = "sp=racwdli&st=2024-11-15T22:29:50Z&se=2024-11-16T06:29:50Z&spr=https&sv=2022-11-02&sr=c&sig=1r4X75ZOYoTNXlX0yxXnZaP5dEvIh21iPPxxhmSHO%2BU%3D"
container_url = "https://datalakestoragerentscape.blob.core.windows.net/openai-rentscape-blob"


# Function to upload a DataFrame to Blob Storage
def upload_dataframe_to_blob(df, blob_name):
    # Save DataFrame to a local CSV file
    file_path = f"{blob_name}.csv"
    df.to_csv(file_path, index=False)

    # Connect to the Blob Container
    blob_service_client = BlobServiceClient(account_url=container_url, credential=sas_token)
    container_client = blob_service_client.get_container_client(container="openai-rentscape-blob")

    # Upload the CSV file to the Blob Storage
    with open(file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    
    print(f"Uploaded {blob_name} to {container_url}")

# Upload `unique_neighbourhoods_prague_df` to Blob Storage
upload_dataframe_to_blob(unique_neighbourhoods_prague_df, "unique_neighbourhoods_prague.csv")

# Upload `unique_neighbourhoods_barcelona_df` to Blob Storage
upload_dataframe_to_blob(unique_neighbourhoods_barcelona_df, "unique_neighbourhoods_barcelona.csv")



# COMMAND ----------

unique_neighbourhoods_prague
