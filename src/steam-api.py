import httpx
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
from pathlib import Path
from google.cloud import storage
import os

DATA_PATH = Path(__file__).resolve().parent / "data"
CATALOGUE_JSON_PATH = DATA_PATH / "catalogue.json"
CATALOGUE_AVRO_PATH = DATA_PATH / "calalogue.avro"

# # Fetch and save JSON data
# url = f"https://api.steampowered.com/ISteamApps/GetAppList/v2/"

# response = httpx.get(url)

# # Write JSON data to file
# with open(CATALOGUE_JSON_PATH, "w") as f:
#     json.dump(response.json(), f)

# # Load JSON data
# with open(CATALOGUE_JSON_PATH, "r") as f:
#     json_data = json.load(f)

# # Extract the list of apps
# apps = json_data["applist"]["apps"]

# # Define Avro schema
# schema = avro.schema.parse(json.dumps({
#     "type": "record",
#     "name": "App",
#     "fields": [
#         {"name": "appid", "type": "long"},
#         {"name": "name", "type": "string"}
#     ]
# }))

# # Write to Avro
# with open(CATALOGUE_AVRO_PATH, 'wb') as out:
#     writer = DataFileWriter(out, DatumWriter(), schema)
#     for app in apps:
#         writer.append(app)
#     writer.close()


# Set up GCS Bucket connection

# Path to the service account key
SERVICE_ACCOUNT_KEY_PATH = Path(__file__).resolve().parent.parent / "Credentials" / "service-key.json"

# Set up GCS Bucket connection
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT_KEY_PATH)

# Uploading an object
def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    # Initialize a storage client
    client = storage.Client(project="steamed-bunz",credentials=SERVICE_ACCOUNT_KEY_PATH)

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # Upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

bucket_name = "steamed-bunz-extract"
upload_to_bucket(bucket_name, CATALOGUE_AVRO_PATH, "steam-catalogue/current.avro")