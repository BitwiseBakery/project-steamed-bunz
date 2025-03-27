import httpx
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
from pathlib import Path

from google.cloud import storage

DATA_PATH = Path(__file__).resolve().parent / "data"
CATALOGUE_JSON_PATH = DATA_PATH / "catalogue.json"
CATALOGUE_AVRO_PATH = DATA_PATH / "calalogue.avro"

# # Fetch and save JSON data
# url = f"https://api.steampowered.com/ISteamApps/GetAppList/v2/"

# response = httpx.get(url)

# # Write JSON data to file
# with open(CATALOGUE_JSON_PATH, "w") as f:
#     json.dump(response.json(), f)

# Load JSON data
with open(CATALOGUE_JSON_PATH, "r") as f:
    json_data = json.load(f)

# Extract the list of apps
apps = json_data["applist"]["apps"]

# Define Avro schema
schema = avro.schema.parse(json.dumps({
    "type": "record",
    "name": "App",
    "fields": [
        {"name": "appid", "type": "long"},
        {"name": "name", "type": "string"}
    ]
}))

# Write to Avro
# with open(CATALOGUE_AVRO_PATH, 'wb') as out:
#     writer = DataFileWriter(out, DatumWriter(), schema)
#     for app in apps:
#         writer.append(app)
#     writer.close()

def write_read(bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client(project="bitwisebakery")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)     
    
    with blob.open("w") as f:
        writer = DataFileWriter(f, DatumWriter(), schema)
        for app in apps:
            writer.append(app)
        writer.close()


write_read(bucket_name="steamed-bunz-extract", blob_name="steam-catalogue/apps.avro")


