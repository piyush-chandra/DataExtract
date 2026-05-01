from dataclasses import dataclass
from datetime import datetime, timezone

from blob_ingest_service.ingest_blobs import iter_blob_items


@dataclass
class BlobItem:
    pathname: str
    uploaded_at: datetime
    size: int = 1
    url: str = "https://example.test/blob"


class FakeStorageClient:
    def iter_objects(self, prefix: str, batch_size: int):
        assert prefix == "CI_CUST_EXPORT/"
        assert batch_size == 100
        return iter(
            [
                BlobItem(
                    pathname="CI_CUST_EXPORT/new.csv.gz",
                    uploaded_at=datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
                ),
                BlobItem(
                    pathname="CI_CUST_EXPORT/not-data.txt",
                    uploaded_at=datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc),
                ),
                BlobItem(
                    pathname="CI_CUST_EXPORT/old.csv.gz",
                    uploaded_at=datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
                ),
                BlobItem(
                    pathname="CI_CUST_EXPORT/middle.csv.gz",
                    uploaded_at=datetime(2026, 4, 28, 11, 0, tzinfo=timezone.utc),
                ),
            ]
        )


def test_iter_blob_items_processes_oldest_csv_gzip_first():
    items = list(iter_blob_items(FakeStorageClient(), "CI_CUST_EXPORT/", limit=None))

    assert [item.pathname for item in items] == [
        "CI_CUST_EXPORT/old.csv.gz",
        "CI_CUST_EXPORT/middle.csv.gz",
        "CI_CUST_EXPORT/new.csv.gz",
    ]


def test_iter_blob_items_applies_limit_after_oldest_sort():
    items = list(iter_blob_items(FakeStorageClient(), "CI_CUST_EXPORT/", limit=1))

    assert [item.pathname for item in items] == ["CI_CUST_EXPORT/old.csv.gz"]
