import argparse
import csv
import gzip
import logging
import shutil
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.r2_storage import R2StorageClient  # noqa: E402

BASE_DIR = Path("/Volumes/T7/BankData")
RAW_DIR = BASE_DIR / "rawFiles"
DB_PATH = BASE_DIR / "bankdata.sqlite"
TABLE_NAME = "ci_custmast"
INGEST_TABLE = "blob_ingest_files"
DEFAULT_PREFIX = "CI_CUST_EXPORT/"
INSERT_CHUNK_SIZE = 10_000
CUSTOMER_COLUMNS = [
    ("COD_CUST_ID", "INTEGER PRIMARY KEY"),
    ("DAT_CUST_OPEN", "TEXT"),
    ("FLG_CUST_TYP", "TEXT"),
    ("FLG_STAFF", "TEXT"),
    ("COD_CC_HOMEBRN", "TEXT"),
    ("NAM_CUST_SHRT", "TEXT"),
    ("TXT_CUST_PREFIX", "TEXT"),
    ("NAM_CUST_FULL", "TEXT"),
    ("FLG_IC_TYP", "TEXT"),
    ("COD_CUST_NATL_ID", "TEXT"),
    ("COD_CUST_LANG", "TEXT"),
    ("COD_INCOME_CAT", "TEXT"),
    ("TXT_PROFESS_CAT", "TEXT"),
    ("COD_TDS", "TEXT"),
    ("REF_CUST_IT_NUM", "TEXT"),
    ("TXT_CUSTADR_ADD1", "TEXT"),
    ("TXT_CUSTADR_ADD2", "TEXT"),
    ("TXT_CUSTADR_ADD3", "TEXT"),
    ("NAM_CUSTADR_CITY", "TEXT"),
    ("NAM_CUSTADR_STATE", "TEXT"),
    ("NAM_CUSTADR_CNTRY", "TEXT"),
    ("TXT_CUSTADR_ZIP", "TEXT"),
    ("REF_CUST_PHONE", "TEXT"),
    ("REF_CUST_PHONE_OFF", "TEXT"),
    ("REF_CUST_EMAIL", "TEXT"),
    ("TXT_PERMADR_ADD1", "TEXT"),
    ("TXT_PERMADR_ADD2", "TEXT"),
    ("TXT_PERMADR_ADD3", "TEXT"),
    ("NAM_PERMADR_CITY", "TEXT"),
    ("NAM_PERMADR_CNTRY", "TEXT"),
    ("NAM_PERMADR_STATE", "TEXT"),
    ("TXT_PERMADR_ZIP", "TEXT"),
    ("DAT_BIRTH_CUST", "TEXT"),
    ("TXT_CUST_SEX", "TEXT"),
    ("COD_CUST_MARSTAT", "TEXT"),
    ("CTR_CUST_SPOUSES", "TEXT"),
    ("NAM_CUST_SPOUSE", "TEXT"),
    ("TXT_SPOUSE_OCCPN", "TEXT"),
    ("COD_TYP_ACCOM", "TEXT"),
    ("TXT_CUST_NATNLTY", "TEXT"),
    ("COD_CUST_BLDGRP", "TEXT"),
    ("TXT_CUST_EDUCN", "TEXT"),
    ("FLG_CUST_MEMO", "TEXT"),
    ("DAT_INCORPORATED", "TEXT"),
    ("NAM_CNTRY_INCORP", "TEXT"),
    ("COD_BUSINESS_REGNO", "TEXT"),
    ("COD_BUSINESS_CAT", "TEXT"),
    ("TXT_BUSINESS_TYP", "TEXT"),
    ("COD_KYC_STATUS", "TEXT"),
    ("COD_EMPLOYEE_ID", "TEXT"),
    ("FLG_MINOR", "TEXT"),
    ("TXT_HOLDADR_ADD1", "TEXT"),
    ("TXT_HOLDADR_ADD2", "TEXT"),
    ("TXT_HOLDADR_ADD3", "TEXT"),
    ("NAM_HOLDADR_STATE", "TEXT"),
    ("NAM_HOLDADR_CNTRY", "TEXT"),
    ("TXT_HOLDADR_ZIP", "TEXT"),
    ("REF_PHONE_MOBILE", "TEXT"),
    ("NAM_MOTHER_MAIDEN", "TEXT"),
    ("COD_AADHAAR_NO", "TEXT"),
    ("DAT_AADHAAR_UPDATED_ON", "TEXT"),
    ("COD_AADHAAR_LINK_ACCT", "TEXT"),
    ("FLG_PAN_INOPERATIVE", "TEXT"),
]
CUSTOMER_COLUMN_NAMES = [column for column, _ in CUSTOMER_COLUMNS]
LOGGER = logging.getLogger("blob_ingest")


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def connect_db(db_path: Path) -> sqlite3.Connection:
    LOGGER.debug("Opening SQLite database path=%s", db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-262144")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_ingest_table(conn: sqlite3.Connection) -> None:
    LOGGER.debug("Ensuring ingest tracking table=%s", INGEST_TABLE)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ident(INGEST_TABLE)} (
            pathname TEXT PRIMARY KEY,
            local_path TEXT NOT NULL,
            blob_url TEXT,
            blob_size INTEGER,
            row_count INTEGER NOT NULL,
            inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def is_already_ingested(conn: sqlite3.Connection, pathname: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {quote_ident(INGEST_TABLE)} WHERE pathname = ?",
        (pathname,),
    ).fetchone()
    return row is not None


def checkpoint_db(conn: sqlite3.Connection) -> None:
    LOGGER.debug("Running WAL checkpoint")
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def ensure_data_table(conn: sqlite3.Connection) -> None:
    LOGGER.debug("Ensuring customer table=%s column_count=%s", TABLE_NAME, len(CUSTOMER_COLUMNS))
    column_defs = [
        f"{quote_ident(column)} {column_type}"
        for column, column_type in CUSTOMER_COLUMNS
    ]
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ident(TABLE_NAME)} (
            {", ".join(column_defs)}
        )
        """
    )

    existing = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({quote_ident(TABLE_NAME)})")
    }
    for column, column_type in CUSTOMER_COLUMNS:
        if column not in existing:
            LOGGER.info("Adding missing SQLite column table=%s column=%s", TABLE_NAME, column)
            conn.execute(
                f"ALTER TABLE {quote_ident(TABLE_NAME)} "
                f"ADD COLUMN {quote_ident(column)} {column_type}"
            )


def rebuild_tables(conn: sqlite3.Connection) -> None:
    LOGGER.warning("Rebuilding SQLite tables table=%s tracking_table=%s", TABLE_NAME, INGEST_TABLE)
    conn.execute(f"DROP TABLE IF EXISTS {quote_ident(TABLE_NAME)}")
    conn.execute(f"DROP TABLE IF EXISTS {quote_ident(INGEST_TABLE)}")
    ensure_data_table(conn)
    ensure_ingest_table(conn)


def make_row_mapper(csv_columns: list[str]):
    LOGGER.debug("Mapping CSV columns csv_column_count=%s", len(csv_columns))
    index_by_column = {column: index for index, column in enumerate(csv_columns)}
    missing_columns = [
        column for column in CUSTOMER_COLUMN_NAMES if column not in index_by_column
    ]
    if missing_columns:
        LOGGER.error("CSV missing required columns missing=%s", missing_columns)
        raise ValueError(f"CSV is missing required columns: {', '.join(missing_columns)}")
    extra_columns = [
        column for column in csv_columns if column not in set(CUSTOMER_COLUMN_NAMES)
    ]
    if extra_columns:
        LOGGER.warning("CSV contains extra columns that will be ignored extra=%s", extra_columns)

    indexes = [index_by_column[column] for column in CUSTOMER_COLUMN_NAMES]

    def map_row(row: list[str]) -> list[str | int | None]:
        mapped = []
        for column, index in zip(CUSTOMER_COLUMN_NAMES, indexes):
            value = row[index] if index < len(row) else ""
            if value == "":
                mapped.append(None)
            elif column == "COD_CUST_ID":
                mapped.append(int(value))
            else:
                mapped.append(value)
        return mapped

    return map_row


def insert_csv_gzip(
    conn: sqlite3.Connection,
    gzip_path: Path,
    pathname: str,
    blob_url: str | None = None,
    blob_size: int | None = None,
) -> int:
    LOGGER.info("Starting SQLite insert file=%s pathname=%s", gzip_path, pathname)
    with gzip.open(gzip_path, "rt", encoding="utf-8", newline="") as file_obj:
        reader = csv.reader(file_obj)
        csv_columns = next(reader)
        ensure_data_table(conn)
        map_row = make_row_mapper(csv_columns)
        LOGGER.debug("Prepared insert sql table=%s columns=%s", TABLE_NAME, len(CUSTOMER_COLUMN_NAMES))

        quoted_columns = ", ".join(quote_ident(column) for column in CUSTOMER_COLUMN_NAMES)
        placeholders = ", ".join("?" for _ in CUSTOMER_COLUMN_NAMES)
        sql = (
            f"INSERT OR REPLACE INTO {quote_ident(TABLE_NAME)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )

        row_count = 0
        batch = []
        for row in reader:
            batch.append(map_row(row))
            if len(batch) >= INSERT_CHUNK_SIZE:
                conn.executemany(sql, batch)
                row_count += len(batch)
                LOGGER.debug("Inserted batch pathname=%s total_rows=%s", pathname, row_count)
                batch.clear()

        if batch:
            conn.executemany(sql, batch)
            row_count += len(batch)
            LOGGER.debug("Inserted final batch pathname=%s total_rows=%s", pathname, row_count)

    conn.execute(
        f"""
        INSERT OR REPLACE INTO {quote_ident(INGEST_TABLE)}
            (pathname, local_path, blob_url, blob_size, row_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (pathname, str(gzip_path), blob_url, blob_size, row_count),
    )
    LOGGER.info("Finished SQLite insert pathname=%s rows=%s", pathname, row_count)
    return row_count


def ingest_local_file(conn: sqlite3.Connection, source_path: Path) -> int:
    LOGGER.info("Local ingest requested source=%s raw_dir=%s db=%s", source_path, RAW_DIR, DB_PATH)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    target_path = RAW_DIR / source_path.name
    pathname = source_path.name
    if is_already_ingested(conn, pathname):
        LOGGER.info("Skipping already ingested local file pathname=%s", pathname)
        return 0

    if source_path.resolve() != target_path.resolve():
        LOGGER.info("Copying raw file source=%s target=%s", source_path, target_path)
        shutil.copy2(source_path, target_path)
    else:
        LOGGER.debug("Source file already in raw directory target=%s", target_path)

    start = time.perf_counter()
    with conn:
        rows = insert_csv_gzip(conn, target_path, pathname)
    elapsed = time.perf_counter() - start
    LOGGER.info("Local ingest committed pathname=%s rows=%s elapsed=%.2fs", pathname, rows, elapsed)
    return rows


def iter_blob_items(client, prefix: str, limit: int | None):
    LOGGER.info("Listing Cloudflare R2 objects prefix=%s limit=%s", prefix, limit)
    candidates = []
    for item in client.iter_objects(prefix=prefix, batch_size=100):
        LOGGER.debug(
            "Found blob pathname=%s size=%s uploaded_at=%s",
            item.pathname,
            item.size,
            item.uploaded_at,
        )
        if not item.pathname.endswith(".csv.gz"):
            LOGGER.debug("Skipping non-csv-gzip blob pathname=%s", item.pathname)
            continue
        candidates.append(item)

    candidates.sort(key=lambda item: (item.uploaded_at, item.pathname))
    LOGGER.info("Found %s candidate CSV gzip blobs; processing oldest first", len(candidates))

    selected = candidates[:limit] if limit else candidates
    for index, item in enumerate(selected, start=1):
        LOGGER.info(
            "Queue position=%s pathname=%s uploaded_at=%s size=%s",
            index,
            item.pathname,
            item.uploaded_at,
            item.size,
        )
        yield item


def ingest_blob_item(conn: sqlite3.Connection, client, item) -> int:
    LOGGER.info(
        "Processing blob pathname=%s size=%s url=%s",
        item.pathname,
        item.size,
        item.url,
    )
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    local_path = RAW_DIR / Path(item.pathname).name

    if is_already_ingested(conn, item.pathname):
        LOGGER.info("Blob already ingested; deleting remote copy pathname=%s", item.pathname)
        client.delete(item.pathname)
        LOGGER.info("Deleted already-ingested blob pathname=%s", item.pathname)
        return 0

    download_start = time.perf_counter()
    LOGGER.info("Downloading blob pathname=%s local_path=%s", item.pathname, local_path)
    client.download_file(item.pathname, local_path, overwrite=True, create_parents=True)
    download_elapsed = time.perf_counter() - download_start
    local_size = local_path.stat().st_size
    LOGGER.info(
        "Downloaded blob pathname=%s local_path=%s bytes=%s elapsed=%.2fs",
        item.pathname,
        local_path,
        local_size,
        download_elapsed,
    )

    start = time.perf_counter()
    with conn:
        rows = insert_csv_gzip(
            conn,
            local_path,
            item.pathname,
            blob_url=item.url,
            blob_size=item.size,
        )
    elapsed = time.perf_counter() - start
    LOGGER.info(
        "SQLite commit complete pathname=%s rows=%s elapsed=%.2fs",
        item.pathname,
        rows,
        elapsed,
    )

    delete_start = time.perf_counter()
    LOGGER.info("Deleting Cloudflare R2 object pathname=%s", item.pathname)
    client.delete(item.pathname)
    LOGGER.info(
        "Deleted Cloudflare R2 object pathname=%s elapsed=%.2fs",
        item.pathname,
        time.perf_counter() - delete_start,
    )
    return rows


def ingest_from_blob(prefix: str, limit: int | None) -> int:
    client = R2StorageClient()
    total_rows = 0
    processed = 0
    start = time.perf_counter()
    with connect_db(DB_PATH) as conn:
        ensure_data_table(conn)
        ensure_ingest_table(conn)
        for item in iter_blob_items(client, prefix=prefix, limit=limit):
            total_rows += ingest_blob_item(conn, client, item)
            processed += 1
        checkpoint_db(conn)

    LOGGER.info(
        "Blob ingest complete processed=%s inserted_rows=%s elapsed=%.2fs",
        processed,
        total_rows,
        time.perf_counter() - start,
    )
    return total_rows


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Cloudflare R2 CSV gzip files into local SQLite."
    )
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--local-file", type=Path, default=None)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop and recreate the customer and ingest tracking tables first.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    global DB_PATH
    DB_PATH = args.db_path
    LOGGER.info("Service starting db=%s raw_dir=%s", DB_PATH, RAW_DIR)

    if args.local_file:
        with connect_db(DB_PATH) as conn:
            if args.rebuild:
                rebuild_tables(conn)
            else:
                ensure_data_table(conn)
                ensure_ingest_table(conn)
            ingest_local_file(conn, args.local_file)
            checkpoint_db(conn)
        LOGGER.info("Service finished local-file mode")
        return

    if args.rebuild:
        with connect_db(DB_PATH) as conn:
            rebuild_tables(conn)
            checkpoint_db(conn)

    ingest_from_blob(prefix=args.prefix, limit=args.limit)
    LOGGER.info("Service finished blob mode")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger("blob_ingest").exception("Service failed")
        raise
