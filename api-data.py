import requests
import pandas as pd

def fetch_data_from_api(api_url):
    # Send a GET request to the API
    response = requests.get(api_url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        return data
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def create_csv_from_data(data, csv_filename):
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data)
    
    # Save the DataFrame to a CSV file
    df.to_csv(csv_filename, index=False)

def main():
    # Define the API URL
    api_url = "https://api.example.com/data"
    
    # Fetch the data from the API
    data = fetch_data_from_api(api_url)
    
    # Define the CSV file name
    csv_filename = "output.csv"
    
    # Create a CSV file from the data
    create_csv_from_data(data, csv_filename)
    
    print(f"Data has been saved to {csv_filename}")

if __name__ == "__main__":
    main()
