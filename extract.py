from faker import Faker
import random
import csv
from google.cloud import storage

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

# Function to upload the CSV file to a GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, project_id):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Set your GCS bucket name, source file name, destination file name, and project ID
bucket_name = 'bkt-ranking-data_new'
destination_blob_name = 'stock_data.csv'
project_id = 'data-engineering-2-pipline'

# Upload the CSV file to GCS
upload_to_gcs(bucket_name, csv_file_name, destination_blob_name, project_id)
