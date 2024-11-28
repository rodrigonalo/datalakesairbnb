# Databricks notebook source
# Install or upgrade azure-storage-blob, pandas, and openai packages
#%pip install azure-storage-blob pandas openai

# COMMAND ----------

# Upgrade typing_extensions to ensure compatibility
# %pip install --upgrade openai typing_extensions
# %restart_python

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

prague_listings

# COMMAND ----------

# MAGIC %md Topic classification

# COMMAND ----------

from openai import OpenAI
import pandas as pd

# Initialize OpenAI client
client = OpenAI(api_key='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')

# Initialize a counter variable
execution_count = 0
total_rows_prague = len(prague_listings)  # Total number of rows in the Prague DataFrame
total_rows_barcelona = len(barcelona_listings)  # Total number of rows in the Barcelona DataFrame

# Define the categories
categories = [
    "Spacious kitchen", "Family-friendly", "Central city location", "With a pool", "Panoramic views", 
    "Perfect for couples", "Pet-friendly", "Close to the beach", "Modern apartment", "Rustic and cozy",
    "Terrace or balcony", "With jacuzzi", "Remote work space", "Good natural lighting", "Quiet area",
    "Nearby public transport", "Historic neighborhood", "Ideal for large groups", "Eco-friendly home",
    "Accessible for people with reduced mobility", "Minimalist decor", "Equipped with Smart TV", "With gym",
    "Near parks or green areas", "Industrial style", "Spacious loft", "Cleaning service included", "Free parking",
    "Bohemian decor", "Near restaurants", "Event space", "Traditional house", "Close to the mountains", 
    "Romantic atmosphere", "Luxury apartment", "Barbecue area", "Budget-friendly apartment", "With fireplace", 
    "Safe area", "Kids' space", "Flexible check-in", "King-size bed", "Pet-free apartment", "Near train station",
    "Perfect for weekend getaways", "Breakfast included", "With laundry", "Long-term rental available",
    "Unique architectural design", "Perfect for weddings", "Access to water activities", "Near tourist attractions",
    "Countryside atmosphere", "Green space", "Fully furnished", "With storage space", "Surrounded by wineries",
    "Cyclist-friendly area", "Near the airport", "Suitable for small pets", "Includes kitchen utensils", 
    "With lake access", "High ceilings", "Thematic decor", "Backpacker-friendly", "Ocean view", 
    "Near supermarkets", "Private access", "House with garden", "Ensuite bathroom", "Warm and relaxing atmosphere", 
    "Colonial style", "Near sports areas", "With river access", "Picnic area", "Perfect for students", 
    "Includes coworking space", "Suitable for parties", "Advanced security system", "Vintage decor", 
    "Heated pool", "Space with outdoor fireplace", "Close to shopping areas", "Boutique apartment", 
    "Independent entrance", "Compact studio", "Surfer-friendly", "Stair-free apartment", "With sauna", 
    "Forest views", "Tropical decor", "Oceanfront property", "Meditation area", "Gourmet kitchen", 
    "Artistic atmosphere", "Close to a spa", "Perfect for retreats", "With yoga space", "Suitable for large families", 
    "Camping area"
]

# Function to classify property descriptions
def classify_description(property_description, total_rows):
    global execution_count  # Use the global counter
    execution_count += 1  # Increment the counter for each processed row
    remaining_rows = total_rows - execution_count  # Calculate remaining rows

    # Check if the property description is missing or empty
    if not isinstance(property_description, str) or not property_description.strip():
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Property Description: {property_description}\nDetected Categories: N/A\n")
        return "N/A"

    # Create the prompt
    prompt = f"""
    Analyze the following Airbnb property description and classify it into the top 5 most relevant categories from the provided list. Return the result as a single message with the categories separated by commas.

    Property Description: {property_description}

    Categories: {', '.join(categories)}

    Output format: Category1, Category2, Category3, Category4, Category5
    """

    try:
        # Make the API call to OpenAI
        completion = client.chat.completions.create(
            model="gpt-4o-mini",  # Replace with your preferred model
            messages=[
                {"role": "system", "content": "You are an Airbnb expert."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract the categories from the response
        categories_detected = completion.choices[0].message.content.strip()

        # Print the property description, detected categories, execution count, and remaining rows
        print(f"Execution Count: {execution_count}")
        print(f"Remaining Rows: {remaining_rows}")
        print(f"Property Description: {property_description}\nDetected Categories: {categories_detected}\n")

        return categories_detected

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error"

# Apply the classification function to the 'description' column in both DataFrames
prague_listings['openai_classification'] = prague_listings['description'].apply(
    lambda x: classify_description(x, total_rows_prague)
)
barcelona_listings['openai_classification'] = barcelona_listings['description'].apply(
    lambda x: classify_description(x, total_rows_barcelona)
)


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
upload_dataframe_to_blob(prague_listings, "prague_listings.csv")

# Upload `unique_neighbourhoods_barcelona_df` to Blob Storage
upload_dataframe_to_blob(barcelona_listings, "barcelona_listings.csv")


