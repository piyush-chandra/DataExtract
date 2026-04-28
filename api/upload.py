import json
import base64
from http.server import BaseHTTPRequestHandler
from vercel_blob import put


class handler(BaseHTTPRequestHandler):

    def do_POST(self):

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)

            data = json.loads(body)

            job_name = data.get("jobName")
            end_key = data.get("endKey")
            payload_sha = data.get("payloadSha256")
            payload_b64 = data.get("payloadGzipBase64")

            if not payload_b64:
                raise Exception("missing payload")

            # decode gzip payload
            payload_bytes = base64.b64decode(payload_b64)

            # blob filename
            filename = f"{job_name}/{end_key}_{payload_sha}.gz"

            blob = put(
                filename,
                payload_bytes,
                access="private"
            )

            response = {
                "stored": True,
                "lastCustId": end_key,
                "blobUrl": blob["url"]
            }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            self.wfile.write(json.dumps(response).encode())

        except Exception as e:

            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            self.wfile.write(json.dumps({
                "stored": False,
                "error": str(e)
            }).encode())