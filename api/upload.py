import base64
import binascii
import gzip
import hashlib
import re

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from vercel.blob import BlobClient

app = FastAPI()

BLOB_READ_WRITE_TOKEN = "vercel_blob_rw_ncDq5ecCANAxTUAc_J5OksLjXSnxOk5uyX58Ld2wzosKTdd"


def json_error(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"stored": False, "error": message},
    )


def clean_path_part(value: object, fallback: str) -> str:
    text = str(fallback if value is None else value).strip()
    text = re.sub(r"[^A-Za-z0-9._=-]+", "_", text)
    return text.strip("._") or fallback


@app.post("/api/upload")
async def upload(request: Request):
    try:
        data = await request.json()

        job_name = data.get("jobName")
        start_after = data.get("startAfter")
        end_key = data.get("endKey")
        row_count = data.get("rowCount")
        payload_sha = data.get("payloadSha256")
        payload_b64 = data.get("payloadGzipBase64")

        if not job_name:
            return json_error(400, "missing jobName")

        if end_key is None:
            return json_error(400, "missing endKey")

        if not payload_sha:
            return json_error(400, "missing payloadSha256")

        if not payload_b64:
            return json_error(400, "missing payloadGzipBase64")

        try:
            payload_gzip = base64.b64decode(payload_b64, validate=True)
        except (binascii.Error, ValueError):
            return json_error(400, "payloadGzipBase64 is not valid base64")

        try:
            csv_bytes = gzip.decompress(payload_gzip)
        except (OSError, EOFError):
            return json_error(400, "payloadGzipBase64 is not valid gzip data")

        actual_sha = hashlib.sha256(csv_bytes).hexdigest()
        if actual_sha.lower() != str(payload_sha).lower():
            return json_error(
                400,
                f"payloadSha256 mismatch: expected {payload_sha}, got {actual_sha}",
            )

        safe_job_name = clean_path_part(job_name, "export")
        safe_end_key = clean_path_part(end_key, "end")
        safe_start_after = clean_path_part(start_after, "start")
        safe_sha = clean_path_part(payload_sha, "sha")
        filename = (
            f"{safe_job_name}/"
            f"start-{safe_start_after}_end-{safe_end_key}_{safe_sha}.csv.gz"
        )

        if BLOB_READ_WRITE_TOKEN == "PASTE_YOUR_VERCEL_BLOB_READ_WRITE_TOKEN_HERE":
            return json_error(500, "blob token is not configured")

        client = BlobClient(token=BLOB_READ_WRITE_TOKEN)
        blob = client.put(
            filename,
            payload_gzip,
            access="private",
            content_type="application/gzip",
            overwrite=True,
        )

        return {
            "stored": True,
            "lastCustId": end_key,
            "blobUrl": blob.url,
            "pathname": blob.pathname,
            "jobName": job_name,
            "startAfter": start_after,
            "rowCount": row_count,
            "payloadSha256": actual_sha,
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "stored": False,
                "error": str(e),
            },
        )
