import httpx
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
from pathlib import Path
from google.cloud import storage
from datetime import datetime
import functions_framework
from flask import jsonify

@functions_framework.http
def steam_catalogue_to_avro(request):
    # Set up paths and constants
    DATA_PATH = Path("/tmp")
    CATALOGUE_AVRO_PATH = DATA_PATH / "catalogue.avro"

    BUCKET_NAME = "steamed-bunz-extract"
    DEST_BLOB_NAME = "steam-catalogue/current.avro"

    # Fetch Steam app list
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

    try:
        response = httpx.get(url)
        response.raise_for_status()
        apps = response.json()["applist"]["apps"]
    # Add extraction date
        current_date = datetime.now().date().isoformat()
        for app in apps:
            app["data_extr_date"] = current_date

    except httpx.HTTPError as e:
        return jsonify({
            "message": "Error fetching data from Steam API",
            "error": str(e)
        }), 500

    # Define Avro schema
    schema = avro.schema.parse(json.dumps({
        "type": "record",
        "name": "App",
        "fields": [
            {"name": "appid", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "data_extr_date", "type": "string"}
        ]
    }))

    # Write directly to Avro
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    with open(CATALOGUE_AVRO_PATH, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for app in apps:
            writer.append(app)
        writer.close()

    # Upload to GCS
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(DEST_BLOB_NAME)
        blob.upload_from_filename(str(CATALOGUE_AVRO_PATH))

    except Exception as e:
        return jsonify({
            "message": "File upload failed",
            "error": str(e)
        }), 500

    return jsonify({
        "message": "File uploaded successfully",
        "bucket": BUCKET_NAME,
        "file": DEST_BLOB_NAME
    }), 200