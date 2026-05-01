# Blob Ingest Service

Downloads `.csv.gz` files from Cloudflare R2, stores the raw files in:

```text
/Volumes/T7/BankData/rawFiles
```

Then bulk inserts them into:

```text
/Volumes/T7/BankData/bankdata.sqlite
```

After a file is committed to SQLite, the service deletes that object from R2.

Object processing order:

1. List `.csv.gz` objects under the prefix.
2. Sort matching objects by `uploaded_at` ascending.
3. Pick the oldest file first.
4. Download the raw gzip file to `/Volumes/T7/BankData/rawFiles`.
5. Insert rows into SQLite inside one transaction.
6. Commit the transaction.
7. Delete the R2 object.

Required environment variables:

```bash
export R2_BUCKET_NAME="your-bucket"
export R2_ACCESS_KEY_ID="your-access-key-id"
export R2_SECRET_ACCESS_KEY="your-secret-access-key"
export R2_ACCOUNT_ID="your-cloudflare-account-id"
```

You can set `R2_ENDPOINT_URL` instead of `R2_ACCOUNT_ID`. Set `R2_PUBLIC_BASE_URL`
only if you want upload responses to return public HTTPS URLs instead of
`s3://bucket/key` object references.

The `ci_custmast` table is created with one SQLite column per exported customer
field. `COD_CUST_ID` is the primary key, and each CSV row is inserted by column
name.

Run one local sample:

```bash
.venv/bin/python -m blob_ingest_service.ingest_blobs --local-file /Users/piyush/Downloads/start-0_end-21152320_e8df31520c599733d469754d6787a856d5c3e27fabd1910922d73ee67703379b.csv.gz
```

Recreate the SQLite tables and reload:

```bash
.venv/bin/python -m blob_ingest_service.ingest_blobs --rebuild --local-file /Users/piyush/Downloads/start-0_end-21152320_e8df31520c599733d469754d6787a856d5c3e27fabd1910922d73ee67703379b.csv.gz
```

Run against Cloudflare R2:

```bash
.venv/bin/python -m blob_ingest_service.ingest_blobs --prefix CI_CUST_EXPORT/
```

Run with debug logging:

```bash
.venv/bin/python -m blob_ingest_service.ingest_blobs --prefix CI_CUST_EXPORT/ --log-level DEBUG
```

Process only one blob:

```bash
.venv/bin/python -m blob_ingest_service.ingest_blobs --prefix CI_CUST_EXPORT/ --limit 1 --log-level DEBUG
```
