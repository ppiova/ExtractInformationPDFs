"""Configuration and client factories for the Azure extractor project."""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, BaseSettings, Field, validator

load_dotenv()

LOGGER = logging.getLogger(__name__)


class AzureSettings(BaseSettings):
    """Settings loaded from environment variables or .env file."""

    azure_tenant_id: Optional[str] = Field(default=None, alias="AZURE_TENANT_ID")
    azure_client_id: Optional[str] = Field(default=None, alias="AZURE_CLIENT_ID")
    azure_client_secret: Optional[str] = Field(default=None, alias="AZURE_CLIENT_SECRET")

    azure_storage_account: str = Field(alias="AZURE_STORAGE_ACCOUNT")
    azure_storage_container: str = Field(alias="AZURE_STORAGE_CONTAINER")

    azure_formreco_endpoint: str = Field(alias="AZURE_FORMRECO_ENDPOINT")
    azure_formreco_key: Optional[str] = Field(default=None, alias="AZURE_FORMRECO_KEY")

    azure_search_endpoint: str = Field(alias="AZURE_SEARCH_ENDPOINT")
    azure_search_api_key: Optional[str] = Field(default=None, alias="AZURE_SEARCH_API_KEY")
    azure_search_index_narrative: str = Field(alias="AZURE_SEARCH_INDEX_NARRATIVE")
    azure_search_index_tables: str = Field(alias="AZURE_SEARCH_INDEX_TABLES")

    class Config:
        env_file = ".env"
        case_sensitive = False
        allow_population_by_field_name = True

    @validator(
        "azure_storage_account",
        "azure_storage_container",
        "azure_formreco_endpoint",
        "azure_search_endpoint",
        "azure_search_index_narrative",
        "azure_search_index_tables",
    )
    def non_empty(cls, value: str, field):  # type: ignore[override]
        if not value:
            raise ValueError(f"Environment variable {field.alias} must be provided")
        return value


class ClientBundle(BaseModel):
    """Aggregated Azure clients to be shared across modules."""

    blob_service_client: "BlobServiceClient"
    document_intelligence_client: "DocumentIntelligenceClient"
    search_index_client: "SearchIndexClient"


@lru_cache()
def get_settings() -> AzureSettings:
    """Return cached settings instance."""

    settings = AzureSettings()  # type: ignore[call-arg]
    LOGGER.debug("Loaded Azure settings: %s", settings.dict())
    return settings


def _get_default_credential():
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(exclude_shared_token_cache_credential=True)


def get_blob_service_client():
    """Return a BlobServiceClient using managed identity or service principal."""

    from azure.storage.blob import BlobServiceClient

    settings = get_settings()

    account_url = f"https://{settings.azure_storage_account}.blob.core.windows.net"
    credential = _get_default_credential()
    LOGGER.debug("Instantiating BlobServiceClient with DefaultAzureCredential")
    return BlobServiceClient(account_url=account_url, credential=credential)


def get_document_intelligence_client():
    """Return a DocumentIntelligenceClient."""

    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential

    settings = get_settings()

    if settings.azure_formreco_key:
        credential = AzureKeyCredential(settings.azure_formreco_key)
    else:
        credential = _get_default_credential()

    return DocumentIntelligenceClient(settings.azure_formreco_endpoint, credential)


def get_search_index_client():
    """Return a SearchIndexClient for index management."""

    from azure.search.documents.indexes import SearchIndexClient
    from azure.core.credentials import AzureKeyCredential

    settings = get_settings()

    if settings.azure_search_api_key:
        credential = AzureKeyCredential(settings.azure_search_api_key)
    else:
        credential = _get_default_credential()

    return SearchIndexClient(endpoint=settings.azure_search_endpoint, credential=credential)


def get_search_client(index_name: str):
    """Return a SearchClient for data ingestion."""

    from azure.search.documents import SearchClient
    from azure.core.credentials import AzureKeyCredential

    settings = get_settings()

    if settings.azure_search_api_key:
        credential = AzureKeyCredential(settings.azure_search_api_key)
    else:
        credential = _get_default_credential()

    return SearchClient(endpoint=settings.azure_search_endpoint, index_name=index_name, credential=credential)
