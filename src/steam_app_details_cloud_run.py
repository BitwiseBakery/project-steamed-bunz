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
BATCH_DELAY_SECONDS = float(os.environ.get("STEAM_BATCH_DELAY_SECONDS", "1.0"))
MAX_RETRIES = int(os.environ.get("STEAM_MAX_RETRIES", "3"))
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
    # Ensure types match Avro schema: Genre.id -> string|null, Category.id -> long|null
    genres = d.get("genres") or []
    genres_out = []
    for g in genres:
        gid = g.get("id")
        # normalize genre id to string if present
        gid_norm = None
        if gid is not None:
            try:
                gid_norm = str(gid)
            except Exception:
                gid_norm = None
        genres_out.append({"id": gid_norm, "description": g.get("description")})
    out["genres"] = genres_out
    out["genres_count"] = len(genres)

    categories = d.get("categories") or []
    categories_out = []
    for c in categories:
        cid = c.get("id")
        cid_norm = None
        if cid is not None:
            try:
                # Avro expects a long (int) for category id
                cid_norm = int(cid)
            except Exception:
                cid_norm = None
        categories_out.append({"id": cid_norm, "description": c.get("description")})
    out["categories"] = categories_out
    out["categories_count"] = len(categories)

    # content_descriptors: flatten notes and keep ids array + count
    cd = d.get("content_descriptors") or {}
    # normalize content descriptor ids to integers
    cd_ids = cd.get("ids") or []
    cd_ids_norm = []
    for cid in cd_ids:
        try:
            cd_ids_norm.append(int(cid))
        except Exception:
            # skip ids that can't be parsed as int
            continue
    out["content_descriptors_ids"] = cd_ids_norm
    out["content_descriptors_ids_count"] = len(cd_ids_norm)

    # required age
    # required_age should be an integer (or None)
    req_age = d.get("required_age")
    if req_age is None:
        out["required_age"] = None
    else:
        try:
            out["required_age"] = int(req_age)
        except Exception:
            # sometimes the field is a non-numeric string; coerce to None
            out["required_age"] = None

    # platforms -> flatten into platform_* boolean columns
    platforms = d.get("platforms") or {}
    out["platform_windows"] = platforms.get("windows")
    out["platform_mac"] = platforms.get("mac")
    out["platform_linux"] = platforms.get("linux")

    # achievements -> flatten total into a top-level column
    achievements = d.get("achievements") or {}
    # ensure achievements total is an int or None
    ach_total = achievements.get("total")
    if ach_total is None:
        out["achievements_total_count"] = None
    else:
        try:
            out["achievements_total_count"] = int(ach_total)
        except Exception:
            out["achievements_total_count"] = None

    return out


def upload_json_blob(bucket, blob_name, local_path, content_type=None):
    """Upload a local file to GCS with optional content_type."""
    blob = bucket.blob(blob_name)
    if content_type:
        blob.upload_from_filename(str(local_path), content_type=content_type)
    else:
        blob.upload_from_filename(str(local_path))


def _sanitize_record_for_avro(rec):
    """Return a copy of rec where fields are coerced to types matching _AVRO_SCHEMA.

    Coercions applied:
    - appid -> str
    - type -> str or None
    - release_date_coming_soon -> bool or None
    - release_date -> str or None
    - developers/publishers -> list of str or None
    - genres -> list of {id: str|null, description: str|null}
    - categories -> list of {id: int|null, description: str|null}
    - content_descriptors_ids -> list of int
    - required_age -> int or None
    - platform_* -> bool or None
    - achievements_total_count -> int or None
    """
    out = {}
    # appid
    out["appid"] = str(rec.get("appid")) if rec.get("appid") is not None else ""

    # simple scalar nullable string
    out["type"] = rec.get("type") if isinstance(rec.get("type"), str) else (str(rec.get("type")) if rec.get("type") is not None else None)

    # release date fields
    out["release_date_coming_soon"] = rec.get("release_date_coming_soon") if isinstance(rec.get("release_date_coming_soon"), bool) else None
    out["release_date"] = rec.get("release_date") if isinstance(rec.get("release_date"), str) else (str(rec.get("release_date")) if rec.get("release_date") is not None else None)

    # developers / publishers: ensure lists of strings or None
    def _norm_str_list(val):
        if val is None:
            return None
        try:
            return [str(x) for x in val]
        except Exception:
            return None

    out["developers"] = _norm_str_list(rec.get("developers"))
    out["publishers"] = _norm_str_list(rec.get("publishers"))

    # genres and categories already normalized by extract_fields but be defensive
    def _norm_genres(genres):
        if not genres:
            return None
        out_g = []
        for g in genres:
            gid = g.get("id") if isinstance(g, dict) else None
            gid_norm = str(gid) if gid is not None else None
            desc = g.get("description") if isinstance(g, dict) else None
            out_g.append({"id": gid_norm, "description": desc})
        return out_g

    def _norm_categories(cats):
        if not cats:
            return None
        out_c = []
        for c in cats:
            cid = c.get("id") if isinstance(c, dict) else None
            try:
                cid_norm = int(cid) if cid is not None else None
            except Exception:
                cid_norm = None
            desc = c.get("description") if isinstance(c, dict) else None
            out_c.append({"id": cid_norm, "description": desc})
        return out_c

    out["genres"] = _norm_genres(rec.get("genres"))
    out["genres_count"] = rec.get("genres_count") if isinstance(rec.get("genres_count"), int) else (int(rec.get("genres_count")) if rec.get("genres_count") is not None else None)
    out["categories"] = _norm_categories(rec.get("categories"))
    out["categories_count"] = rec.get("categories_count") if isinstance(rec.get("categories_count"), int) else (int(rec.get("categories_count")) if rec.get("categories_count") is not None else None)

    # content descriptors ids -> list of ints
    cd_ids = rec.get("content_descriptors_ids") or []
    cd_norm = []
    for cid in cd_ids:
        try:
            cd_norm.append(int(cid))
        except Exception:
            continue
    out["content_descriptors_notes"] = rec.get("content_descriptors_notes") if isinstance(rec.get("content_descriptors_notes"), str) else None
    out["content_descriptors_ids"] = cd_norm if cd_norm else []
    out["content_descriptors_ids_count"] = len(cd_norm)

    # required_age
    req = rec.get("required_age")
    try:
        out["required_age"] = int(req) if req is not None else None
    except Exception:
        out["required_age"] = None

    # platforms
    out["platform_windows"] = bool(rec.get("platform_windows")) if rec.get("platform_windows") is not None else None
    out["platform_mac"] = bool(rec.get("platform_mac")) if rec.get("platform_mac") is not None else None
    out["platform_linux"] = bool(rec.get("platform_linux")) if rec.get("platform_linux") is not None else None

    # achievements
    ach = rec.get("achievements_total_count")
    try:
        out["achievements_total_count"] = int(ach) if ach is not None else None
    except Exception:
        out["achievements_total_count"] = None

    return out


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

            # sanitize the record to match Avro types and write defensively
            record_sanitized = _sanitize_record_for_avro(record)
            try:
                with open(local_file, "wb") as out:
                    writer = DataFileWriter(out, DatumWriter(), _AVRO_SCHEMA)
                    writer.append(record_sanitized)
                    writer.close()
            except Exception as e:
                # Catch Avro type errors and log the problematic record for debugging
                logger.exception("Avro write failed for appid %s: %s. Record: %s", aid, e, record_sanitized)
                errors.append({"appid": aid, "error": f"avro_write_failed: {e}", "record": record_sanitized})
                # skip uploading this file and continue
                try:
                    if local_file.exists():
                        local_file.unlink()
                except Exception:
                    pass
                continue

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
