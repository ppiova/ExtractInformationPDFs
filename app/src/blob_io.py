"""Blob storage interactions for fetching Annual Report PDFs."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import get_blob_service_client, get_settings
from .utils import log_timing

LOGGER = logging.getLogger(__name__)


def list_pdf_blobs(prefix: Optional[str] = None) -> List[str]:
    """List PDF blobs available in the configured container."""

    settings = get_settings()
    client = get_blob_service_client().get_container_client(settings.azure_storage_container)
    LOGGER.info("Listing blobs under container %s", settings.azure_storage_container)
    blobs = client.list_blobs(name_starts_with=prefix)
    pdfs = [blob.name for blob in blobs if blob.name.lower().endswith(".pdf")]
    LOGGER.info("Found %d PDF blobs", len(pdfs))
    return sorted(pdfs)


@retry(
    wait=wait_exponential(multiplier=2, min=1, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception),
)
def download_blob_to_path(blob_name: str, target_path: Path) -> Path:
    settings = get_settings()
    container_client = get_blob_service_client().get_container_client(settings.azure_storage_container)
    LOGGER.info("Downloading blob %s -> %s", blob_name, target_path)
    with log_timing(f"download:{blob_name}"):
        with open(target_path, "wb") as handle:
            download_stream = container_client.download_blob(blob_name)
            handle.write(download_stream.readall())
    return target_path


def download_blob_to_tempfile(blob_name: str, suffix: str = ".pdf") -> Path:
    fd, path = tempfile.mkstemp(suffix=suffix)
    target = Path(path)
    try:
        download_blob_to_path(blob_name, target)
    finally:
        try:
            import os

            os.close(fd)
        except OSError:
            pass
    return target


def iter_blob_bytes(blob_name: str, chunk_size: int = 4 * 1024 * 1024) -> Iterator[bytes]:
    settings = get_settings()
    container_client = get_blob_service_client().get_container_client(settings.azure_storage_container)
    stream = container_client.download_blob(blob_name)
    LOGGER.debug("Streaming blob %s in chunks of %d", blob_name, chunk_size)
    for chunk in stream.chunks(chunk_size=chunk_size):
        yield chunk
