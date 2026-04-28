import base64
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from vercel.blob import BlobClient

app = FastAPI()


@app.post("/api/upload")
async def upload(request: Request):

    try:
        data = await request.json()

        job_name = data.get("jobName")
        end_key = data.get("endKey")
        payload_sha = data.get("payloadSha256")
        payload_b64 = data.get("payloadGzipBase64")

        if not payload_b64:
            return JSONResponse(
                status_code=400,
                content={"stored": False, "error": "missing payload"}
            )

        payload_bytes = base64.b64decode(payload_b64)

        filename = f"{job_name}/{end_key}_{payload_sha}.gz"

        client = BlobClient()
        blob = client.put(filename, payload_bytes, access="private")

        return {
            "stored": True,
            "lastCustId": end_key,
            "blobUrl": blob.url
        }

    except Exception as e:

        return JSONResponse(
            status_code=500,
            content={
                "stored": False,
                "error": str(e)
            }
        )
