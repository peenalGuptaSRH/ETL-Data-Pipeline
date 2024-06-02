# import pandas as pd
# from alpha_vantage.timeseries import TimeSeries
# import time
# from google.cloud import storage
# import os

# # Your API key
# API_key = 'JJR7QD3YQR80U9I2'

# # Initialize the TimeSeries object with your API key and specify the output format as pandas DataFrame
# ts = TimeSeries(key=API_key, output_format='pandas')

# # Symbol for the company
# symbol = 'GOOGL'

# # Fetch the monthly adjusted data for the symbol
# try:
#     data, metadata = ts.get_monthly_adjusted(symbol=symbol)
#     data['symbol'] = symbol
#     print(f"Fetched data for {symbol}")

#     # Reset the index to have a proper DataFrame format
#     data.reset_index(inplace=True)

#     # Display the DataFrame's head
#     print(data.head())

#     # Save the DataFrame to a local CSV file
#     local_csv_file = 'googl_stock_data.csv'
#     data.to_csv(local_csv_file, index=False)

#     print("Data has been saved to googl_stock_data.csv locally")
# except Exception as e:
#     print(f"Could not fetch data for {symbol}: {e}")

# # GCP configurations
# bucket_name = 'bkt-ranking-data_new'
# destination_blob_name = 'stock_data_del.csv'
# project_id = 'data-engineering-2-pipline'

# # Function to create a GCS bucket
# def create_bucket(bucket_name, project_id, location='US'):
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(bucket_name)
#     new_bucket = storage_client.create_bucket(bucket, location=location)
#     print(f'Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}')

# # Function to upload the CSV file to a GCS bucket
# def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, project_id):
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
    
#     # Check if the file already exists in the bucket
#     if blob.exists():
#         # If the file exists, download it and append the new data
#         blob.download_to_filename('temp_existing_file.csv')
#         existing_data = pd.read_csv('temp_existing_file.csv')
#         new_data = pd.read_csv(source_file_name)
#         combined_data = pd.concat([existing_data, new_data])
#         combined_data.to_csv(source_file_name, index=False)
#         print(f'Appended data to existing {destination_blob_name} in {bucket_name}.')

#     blob.upload_from_filename(source_file_name)
#     print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# # Initialize GCS client
# storage_client = storage.Client(project=project_id)
# bucket = storage_client.bucket(bucket_name)

# # Check if the bucket exists
# if not bucket.exists():
#     create_bucket(bucket_name, project_id)
# else:
#     print(f'Bucket {bucket_name} already exists.')

# # Upload the CSV file to GCS bucket
# upload_to_gcs(bucket_name, local_csv_file, destination_blob_name, project_id)

# ---------------------Below code is to fetch data from top 10 companies 
# import pandas as pd
# from alpha_vantage.timeseries import TimeSeries
# import time
# from google.cloud import storage

# # Your API key
# API_key = 'JJR7QD3YQR80U9I2'

# # Initialize the TimeSeries object with your API key and specify the output format as pandas DataFrame
# ts = TimeSeries(key=API_key, output_format='pandas')

# # List of 10 companies' stock symbols
# symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V', 'UNH']

# # Dictionary to hold the data for each company
# data_list = []

# # Loop through each symbol and fetch the monthly adjusted data
# for symbol in symbols:
#     try:
#         data, metadata = ts.get_monthly_adjusted(symbol=symbol)
#         data['symbol'] = symbol
#         data_list.append(data)
#         print(f"Fetched data for {symbol}")
#         # To avoid hitting the API rate limit
#         time.sleep(12)  # Alpha Vantage allows 5 API requests per minute in the free tier
#     except Exception as e:
#         print(f"Could not fetch data for {symbol}: {e}")

# # Concatenate all DataFrames into a single DataFrame
# combined_df = pd.concat(data_list)

# # Reset the index to have a proper DataFrame format
# combined_df.reset_index(inplace=True)

# # Display the combined DataFrame's head
# print(combined_df.head())

# # Save the combined DataFrame to a local CSV file
# local_csv_file = 'top_10_companies_stock_data.csv'
# combined_df.to_csv(local_csv_file, index=False)

# print("Data has been saved to top_10_companies_stock_data.csv locally")

# # GCP configurations
# bucket_name = 'bkt-ranking-data_new'
# destination_blob_name = 'stock_data_del.csv'
# project_id = 'data-engineering-2-pipline'

# # Function to create a GCS bucket
# def create_bucket(bucket_name, project_id, location='US'):
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(bucket_name)
#     new_bucket = storage_client.create_bucket(bucket, location=location)
#     print(f'Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}')

# # Function to upload the CSV file to a GCS bucket
# def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, project_id):
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
    
#     # Check if the file already exists in the bucket
#     if blob.exists():
#         # If the file exists, download it and append the new data
#         blob.download_to_filename('temp_existing_file.csv')
#         existing_data = pd.read_csv('temp_existing_file.csv')
#         new_data = pd.read_csv(source_file_name)
#         combined_data = pd.concat([existing_data, new_data])
#         combined_data.to_csv(source_file_name, index=False)
#         print(f'Appended data to existing {destination_blob_name} in {bucket_name}.')

#     blob.upload_from_filename(source_file_name)
#     print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# # Initialize GCS client
# storage_client = storage.Client(project=project_id)
# bucket = storage_client.bucket(bucket_name)

# # Check if the bucket exists
# if not bucket.exists():
#     create_bucket(bucket_name, project_id)
# else:
#     print(f'Bucket {bucket_name} already exists.')

# # Upload the CSV file to GCS bucket
# upload_to_gcs(bucket_name, local_csv_file, destination_blob_name, project_id)

# ---------------final code to fetch data from top 20 companies-------------

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
from google.cloud import storage

# Your API key
API_key = 'JJR7QD3YQR80U9I2'

# Initialize the TimeSeries object with your API key and specify the output format as pandas DataFrame
ts = TimeSeries(key=API_key, output_format='pandas')

# List of 20 companies' stock symbols
symbols = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V', 'UNH',
    'BABA', 'PG', 'MA', 'DIS', 'ADBE', 'PYPL', 'INTC', 'KO', 'PFE', 'PEP'
]

# Dictionary to hold the data for each company
data_list = []

# Loop through each symbol and fetch the monthly adjusted data
for symbol in symbols:
    try:
        data, metadata = ts.get_monthly_adjusted(symbol=symbol)
        data['symbol'] = symbol
        data_list.append(data)
        print(f"Fetched data for {symbol}")
        # To avoid hitting the API rate limit
        time.sleep(12)  # Alpha Vantage allows 5 API requests per minute in the free tier
    except Exception as e:
        print(f"Could not fetch data for {symbol}: {e}")

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(data_list)

# Reset the index to have a proper DataFrame format
combined_df.reset_index(inplace=True)

# Display the combined DataFrame's head
print(combined_df.head())

# Save the combined DataFrame to a local CSV file
local_csv_file = 'top_20_companies_stock_data.csv'
combined_df.to_csv(local_csv_file, index=False)

print("Data has been saved to top_20_companies_stock_data.csv locally")

# GCP configurations
bucket_name = 'bkt-ranking-data_new'
destination_blob_name = 'stock_data_del.csv'
project_id = 'data-engineering-2-pipline'

# Function to create a GCS bucket
def create_bucket(bucket_name, project_id, location='US'):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, location=location)
    print(f'Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}')

# Function to upload the CSV file to a GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, project_id):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Check if the file already exists in the bucket
    if blob.exists():
        # If the file exists, download it and append the new data
        blob.download_to_filename('temp_existing_file.csv')
        existing_data = pd.read_csv('temp_existing_file.csv')
        new_data = pd.read_csv(source_file_name)
        combined_data = pd.concat([existing_data, new_data])
        combined_data.to_csv(source_file_name, index=False)
        print(f'Appended data to existing {destination_blob_name} in {bucket_name}.')

    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Initialize GCS client
storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)

# Check if the bucket exists
if not bucket.exists():
    create_bucket(bucket_name, project_id)
else:
    print(f'Bucket {bucket_name} already exists.')

# Upload the CSV file to GCS bucket
upload_to_gcs(bucket_name, local_csv_file, destination_blob_name, project_id)




