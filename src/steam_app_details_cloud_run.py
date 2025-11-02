import os
import httpx
import json
from pathlib import Path
from datetime import datetime
import time
import random
import functions_framework
from flask import jsonify
import logging

from google.cloud import storage
from google.cloud import bigquery
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
from google.oauth2 import service_account as ga_service_account

# configure module logger - Cloud Run captures stdout/stderr
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BUCKET_NAME = os.environ.get("STEAM_APP_DETAILS_BUCKET", "steamed-bunz-extract")
# normalize prefix: remove any leading/trailing slashes so blob paths are consistent
DEST_PREFIX = os.environ.get("STEAM_APP_DETAILS_PREFIX", "steam-app-details-basic").strip("/")


# Build Avro schema for the flattened record once at module import
_AVRO_SCHEMA = avro.schema.parse(json.dumps({
    "type": "record",
    "name": "SteamAppBasic",
    "fields": [
        {"name": "appid", "type": "string"},
        {"name": "type", "type": ["null", "string"], "default": None},
        {"name": "release_date_coming_soon", "type": ["null", "boolean"], "default": None},
        {"name": "release_date", "type": ["null", "string"], "default": None},
        {"name": "developers", "type": ["null", {"type": "array", "items": "string"}], "default": None},
        {"name": "publishers", "type": ["null", {"type": "array", "items": "string"}], "default": None},
        {"name": "genres", "type": ["null", {"type": "array", "items": {"type": "record", "name": "Genre", "fields": [{"name": "id", "type": ["null","string"], "default": None}, {"name": "description", "type": ["null","string"], "default": None}]}}], "default": None},
        {"name": "genres_count", "type": ["null","long"], "default": None},
        {"name": "categories", "type": ["null", {"type": "array", "items": {"type": "record", "name": "Category", "fields": [{"name": "id", "type": ["null","long"], "default": None}, {"name": "description", "type": ["null","string"], "default": None}]}}], "default": None},
        {"name": "categories_count", "type": ["null","long"], "default": None},
        {"name": "content_descriptors_notes", "type": ["null","string"], "default": None},
        {"name": "content_descriptors_ids", "type": ["null", {"type": "array", "items": "long"}], "default": None},
        {"name": "content_descriptors_ids_count", "type": ["null","long"], "default": None},
        {"name": "required_age", "type": ["null","long"], "default": None},
        {"name": "platform_windows", "type": ["null","boolean"], "default": None},
        {"name": "platform_mac", "type": ["null","boolean"], "default": None},
        {"name": "platform_linux", "type": ["null","boolean"], "default": None},
        {"name": "achievements_total_count", "type": ["null","long"], "default": None}
    ]
}))


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


# Rate limiting / retry configuration (can be tuned via env vars)
BATCH_DELAY_SECONDS = float(os.environ.get("STEAM_BATCH_DELAY_SECONDS", "0.2"))
MAX_RETRIES = int(os.environ.get("STEAM_MAX_RETRIES", "2"))
BACKOFF_BASE = float(os.environ.get("STEAM_BACKOFF_BASE", "1.0"))
BACKOFF_MAX = float(os.environ.get("STEAM_BACKOFF_MAX", "5.0"))
HTTPX_TIMEOUT = float(os.environ.get("HTTPX_TIMEOUT", "5.0"))


def fetch_with_retries(url, params=None, max_retries=MAX_RETRIES):
    """Fetch URL with retries, exponential backoff + jitter, and respect Retry-After on 429.

    Returns parsed JSON on success, or raises the last exception on failure.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = httpx.get(url, params=params, timeout=HTTPX_TIMEOUT)
            # if rate-limited, check Retry-After header
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        wait = float(ra)
                    except Exception:
                        # header may be a HTTP-date; fallback to base backoff
                        wait = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
                else:
                    wait = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
                jitter = random.uniform(0, wait * 0.1)
                total_wait = wait + jitter
                logger.warning("Received 429 from %s; waiting %.1fs before retry (attempt %d)", url, total_wait, attempt)
                time.sleep(total_wait)
                if attempt >= max_retries:
                    resp.raise_for_status()
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError as e:
            # non-429 HTTP error (4xx/5xx) - don't always retry for client errors
            status = getattr(e.response, 'status_code', None)
            if status and 500 <= status < 600 and attempt < max_retries:
                backoff = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
                jitter = random.uniform(0, backoff * 0.1)
                wait = backoff + jitter
                logger.warning("Server error %s, retrying in %.1fs (attempt %d)", status, wait, attempt)
                time.sleep(wait)
                continue
            raise

        except Exception as e:
            # network errors, timeouts, etc.
            if attempt >= max_retries:
                logger.exception("Failed to fetch %s after %d attempts", url, attempt)
                raise
            backoff = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
            jitter = random.uniform(0, backoff * 0.1)
            wait = backoff + jitter
            logger.warning("Error fetching %s: %s; retrying in %.1fs (attempt %d)", url, e, wait, attempt)
            time.sleep(wait)
            continue


def get_clients():
    """Return (bigquery_client, storage_client).

    Behavior:
    - When running on Windows (os.name == 'nt'), if a service account file exists at
      ../Credentials/bitwisebakery-sa.json it will be used to construct clients.
    - Otherwise, use default application credentials (Cloud Run / GCP environment).
    """
    # prefer explicit service account when running locally on Windows
    try:
        if os.name == 'nt':
            sa_path = Path(__file__).resolve().parent.parent / 'Credentials' / 'bitwisebakery-sa.json'
            if sa_path.exists():
                logger.info("Using local service account credentials from %s", sa_path)
                creds = ga_service_account.Credentials.from_service_account_file(str(sa_path))
                bq_client = bigquery.Client(credentials=creds, project=creds.project_id)
                storage_client = storage.Client(credentials=creds, project=creds.project_id)
                return bq_client, storage_client
    except Exception as e:
        logger.exception("Failed to create clients from local service account: %s", e)

    # default: let libraries use ADC (Cloud Run service account)
    logger.info("Using Application Default Credentials (ADC)")
    return bigquery.Client(), storage.Client()


def extract_fields(appid, app_payload):
    """Return a flattened dict with only the required fields for a single app payload.

    This flattens shallow, non-repeating dicts into top-level fields suitable for
    loading directly into BigQuery as columns. Arrays that naturally repeat (like
    developers or genres) are kept as arrays of simple values/objects.
    """
    # default empty result when no data available
    if not app_payload:
        return {"appid": appid, "type": None}

    d = app_payload.get("data")
    if d is None:
        return {"appid": appid, "type": None}

    out = {"appid": appid}

    # simple scalar
    out["type"] = d.get("type")

    # release_date -> flatten into two columns
    release = d.get("release_date") or {}
    out["release_date_coming_soon"] = release.get("coming_soon")
    out["release_date"] = release.get("date")

    # developers / publishers (keep arrays)
    out["developers"] = d.get("developers")
    out["publishers"] = d.get("publishers")

    # genres and categories (keep as arrays of objects, but add counts)
    genres = d.get("genres") or []
    out["genres"] = [{"id": g.get("id"), "description": g.get("description")} for g in genres]
    out["genres_count"] = len(genres)

    categories = d.get("categories") or []
    out["categories"] = [{"id": c.get("id"), "description": c.get("description")} for c in categories]
    out["categories_count"] = len(categories)

    # content_descriptors: flatten notes and keep ids array + count
    cd = d.get("content_descriptors") or {}
    out["content_descriptors_ids"] = cd.get("ids")
    out["content_descriptors_ids_count"] = len(cd.get("ids") or [])

    # required age
    out["required_age"] = d.get("required_age")

    # platforms -> flatten into platform_* boolean columns
    platforms = d.get("platforms") or {}
    out["platform_windows"] = platforms.get("windows")
    out["platform_mac"] = platforms.get("mac")
    out["platform_linux"] = platforms.get("linux")

    # achievements -> flatten total into a top-level column
    achievements = d.get("achievements") or {}
    out["achievements_total_count"] = achievements.get("total")

    return out


def upload_json_blob(bucket, blob_name, local_path, content_type=None):
    """Upload a local file to GCS with optional content_type."""
    blob = bucket.blob(blob_name)
    if content_type:
        blob.upload_from_filename(str(local_path), content_type=content_type)
    else:
        blob.upload_from_filename(str(local_path))


@functions_framework.http
def steam_app_details_worker(request):
    """Cloud Run HTTP function to fetch per-app Steam details and store one file per app in GCS.

    This function is intended to be invoked by Cloud Scheduler (HTTP) on a schedule after
    the catalogue table has been populated.
    """
    # Initialize clients (local SA on Windows will be used if present, otherwise ADC)
    bq, storage_client = get_clients()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Allow the caller to specify a date in one of three ways (priority order):
    # 1) trailing path: POST https://.../2025-09-13
    # 2) query param: ?date=2025-09-13 or ?backfill_date=2025-09-13
    # 3) JSON body with key 'date' or 'backfill_date'
    # If none provided, default to today's UTC date.

    # Extract trailing path segment (Flask's request.path gives the full path)
    path = request.path or ''
    trailing = None
    try:
        # path may be like '/' or '/2025-09-13'
        if path and path.strip('/'):
            trailing = path.strip('/').split('/')[-1]
    except Exception:
        trailing = None

    # Check query params
    q_date = request.args.get('date') or request.args.get('backfill_date')

    # Check JSON body if present
    json_body = None
    try:
        json_body = request.get_json(silent=True) or {}
    except Exception:
        json_body = {}
    body_date = json_body.get('date') or json_body.get('backfill_date') if isinstance(json_body, dict) else None

    candidate = q_date or body_date or trailing

    if candidate:
        # validate YYYY-MM-DD
        try:
            parsed = datetime.strptime(candidate, "%Y-%m-%d").date()
            today_str = parsed.isoformat()
        except Exception:
            logger.exception("Invalid date format provided: %s", candidate)
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    else:
        today_str = datetime.utcnow().date().isoformat()

    logger.info("steam_app_details_worker invoked for date=%s (candidate=%s)", today_str, candidate)
    # Query BigQuery for appids with today's extraction date
    sql = (
        "SELECT DISTINCT appid FROM `bitwisebakery.prep_steam.catalogue` "
        "WHERE data_extr_date = @today"
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("today", "STRING", today_str)]
    )

    query_job = bq.query(sql, job_config=job_config)
    rows = list(query_job.result())
    appids = [str(r["appid"]) for r in rows]
    logger.info("Query returned %d appids for date=%s", len(appids), today_str)

    if not appids:
        logger.info("No appids found for date %s - exiting", today_str)
        return jsonify({"message": "No appids found for today", "date": today_str, "count": 0}), 200

    uploaded = []
    errors = []

    # Steam API: one request per appid (Steam no longer allows multiple appids per request)
    for idx, aid in enumerate(appids):
        logger.info("Processing app %d/%d: %s", idx + 1, len(appids), aid)
        params = {"appids": str(aid)}
        try:
            data = fetch_with_retries("https://store.steampowered.com/api/appdetails", params=params)
        except Exception as e:
            # If Steam returns a 400 Bad Request for this app, stop the whole run immediately
            if isinstance(e, httpx.HTTPStatusError):
                resp = getattr(e, "response", None)
                status = resp.status_code if resp is not None else None
                if status == 400:
                    logger.error("Steam API returned 400 Bad Request for appid %s. Stopping run.", aid)
                    return jsonify({
                        "error": "Steam API returned 400 Bad Request for an appid",
                        "appid": aid,
                        "status": 400,
                        "message": str(e)
                    }), 502

            logger.exception("Steam API request failed for appid %s: %s", aid, e)
            errors.append({"appid": aid, "error": str(e)})
            # small delay before continuing to avoid tight failure loops
            time.sleep(min(BATCH_DELAY_SECONDS * 2, BACKOFF_MAX))
            continue

        # Each response returns a mapping keyed by the appid string
        app_payload = data.get(str(aid)) or data.get(aid)
        reduced = extract_fields(aid, app_payload)

        # write Avro file to /tmp and upload
        local_dir = Path("/tmp/steam_app_details")
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / f"basic_data_app_{aid}.avro"
        try:
            # Avro requires the record to match the schema types; convert None where appropriate
            record = {
                "appid": str(reduced.get("appid")),
                "type": reduced.get("type"),
                "release_date_coming_soon": reduced.get("release_date_coming_soon"),
                "release_date": reduced.get("release_date"),
                "developers": reduced.get("developers"),
                "publishers": reduced.get("publishers"),
                "genres": reduced.get("genres"),
                "genres_count": reduced.get("genres_count"),
                "categories": reduced.get("categories"),
                "categories_count": reduced.get("categories_count"),
                "content_descriptors_notes": reduced.get("content_descriptors_notes"),
                "content_descriptors_ids": reduced.get("content_descriptors_ids"),
                "content_descriptors_ids_count": reduced.get("content_descriptors_ids_count"),
                "required_age": reduced.get("required_age"),
                "platform_windows": reduced.get("platform_windows"),
                "platform_mac": reduced.get("platform_mac"),
                "platform_linux": reduced.get("platform_linux"),
                "achievements_total_count": reduced.get("achievements_total_count"),
            }

            with open(local_file, "wb") as out:
                writer = DataFileWriter(out, DatumWriter(), _AVRO_SCHEMA)
                writer.append(record)
                writer.close()

            blob_name = f"{DEST_PREFIX}/basic_data_app_{aid}.avro" if DEST_PREFIX else f"basic_data_app_{aid}.avro"
            # Upload Avro with a binary content type
            upload_json_blob(bucket, blob_name, local_file, content_type="application/octet-stream")
            uploaded.append(blob_name)
            logger.info("Uploaded blob %s", blob_name)

            # remove local file to keep /tmp small
            try:
                local_file.unlink()
            except Exception:
                pass

        except Exception as e:
            logger.exception("Failed processing appid %s: %s", aid, e)
            errors.append({"appid": aid, "error": str(e)})

        # pause a small amount between requests to avoid rate-limiting
        try:
            time.sleep(BATCH_DELAY_SECONDS)
        except Exception:
            pass
    logger.info("Completed run for date=%s uploaded=%d errors=%d", today_str, len(uploaded), len(errors))
    return jsonify({"message": "done", "date": today_str, "uploaded_count": len(uploaded), "errors": errors}), 200
