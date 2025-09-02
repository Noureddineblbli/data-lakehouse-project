import json
import requests

def ingest_api_data():
    url = "http://api:8000/users"
    output_file = "/staging/users.json"  # Changed to /staging/users.json
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        with open(output_file, "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"Data successfully written to {output_file}")
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        raise

if __name__ == "__main__":
    ingest_api_data()