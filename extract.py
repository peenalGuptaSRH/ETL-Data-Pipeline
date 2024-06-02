from faker import Faker
import random
import csv
from google.cloud import storage
from google.cloud import bigquery

# Initialize Faker
fake = Faker()

# Function to generate fake stock data
def generate_stock_data():
    stock_data = {
        "symbol": fake.bothify(text='???#:NASDAQ'),
        "type": "stock",
        "name": fake.company(),
        "price": round(random.uniform(100, 300), 2),
        "change": round(random.uniform(1, 50), 2),
        "change_percent": round(random.uniform(0.1, 20), 4),
        "previous_close": round(random.uniform(100, 300), 2),
        "pre_or_post_market": round(random.uniform(100, 300), 2),
        "pre_or_post_market_change": round(random.uniform(0, 10), 2),
        "pre_or_post_market_change_percent": round(random.uniform(0, 2), 4),
        "last_update_utc": fake.date_time_this_year().isoformat(),
        "currency": "USD",
        "exchange": "NASDAQ",
        "exchange_open": "2023-03-13 09:30:00",
        "exchange_close": "2023-03-13 16:00:00",
        "timezone": "America/New_York",
        "utc_offset_sec": -14400,
        "country_code": "US",
        "google_mid": fake.bothify(text='/m/#######')
    }
    return stock_data

# Generate fake stock data for 10 stocks
stocks = [generate_stock_data() for _ in range(10)]

# CSV file name
csv_file_name = 'stock_data.csv'

# Write data to CSV
with open(csv_file_name, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=stocks[0].keys())
    writer.writeheader()
    writer.writerows(stocks)

print(f"Data written to {csv_file_name}")

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

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Function to create a BigQuery dataset
def create_bigquery_dataset(dataset_name, project_id, location='US'):
    bigquery_client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    new_dataset = bigquery_client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {new_dataset.dataset_id} created in location {new_dataset.location}.")

# Set your GCS bucket name, source file name, destination file name, and project ID
bucket_name = 'bucket-datapipeline'
destination_blob_name = 'stock_data_del.csv'
project_id = 'testdatapipeline-425000'
dataset_name = 'stock_data_dataset'

# 1 check if the bucket exist in GCP
#if not exits -> create bucket
# Create the bucket (uncomment if the bucket does not exist)
create_bucket(bucket_name, project_id)

#else - bucket exist
# IF csv file exist or not -> 'stock_data_del.csv'
# if not exist -> upload csv file to GCS bucket
#if file exist in bucket -> append the data in the csv file

# Upload the CSV file to GCS
upload_to_gcs(bucket_name, csv_file_name, destination_blob_name, project_id)

# Create the BigQuery dataset
create_bigquery_dataset(dataset_name, project_id)