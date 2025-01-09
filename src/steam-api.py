import httpx
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
from pathlib import Path

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
with open(CATALOGUE_AVRO_PATH, 'wb') as out:
    writer = DataFileWriter(out, DatumWriter(), schema)
    for app in apps:
        writer.append(app)
    writer.close()