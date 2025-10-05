"""Create Azure Cognitive Search indexes for narrative and tables."""
from __future__ import annotations

import logging

from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchableField,
    SimpleField,
)

from .config import get_search_index_client, get_settings

LOGGER = logging.getLogger(__name__)


def narrative_index(name: str) -> SearchIndex:
    fields = [
        SimpleField(name="id", type="Edm.String", key=True),
        SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
        SimpleField(name="year", type="Edm.String", filterable=True, facetable=True),
        SimpleField(name="section", type="Edm.String", filterable=True, facetable=True),
        SimpleField(name="sourceFile", type="Edm.String", filterable=True),
        SimpleField(name="pageStart", type="Edm.Int32", filterable=True),
        SimpleField(name="pageEnd", type="Edm.Int32", filterable=True),
    ]
    return SearchIndex(name=name, fields=fields)


def tables_index(name: str) -> SearchIndex:
    fields = [
        SimpleField(name="id", type="Edm.String", key=True),
        SimpleField(name="year", type="Edm.String", filterable=True, facetable=True),
        SimpleField(name="statementType", type="Edm.String", filterable=True, facetable=True),
        SearchableField(name="metric", type="Edm.String", analyzer_name="en.microsoft", filterable=True),
        SimpleField(name="value", type="Edm.Double"),
        SimpleField(name="unit", type="Edm.String"),
        SimpleField(name="sourceFile", type="Edm.String", filterable=True),
        SimpleField(name="page", type="Edm.Int32", filterable=True),
    ]
    return SearchIndex(name=name, fields=fields)


def ensure_index(client, index: SearchIndex) -> None:
    try:
        existing = client.get_index(index.name)
        LOGGER.info("Index %s exists; updating schema", index.name)
        index.etag = existing.etag
    except ResourceNotFoundError:
        LOGGER.info("Index %s does not exist; creating", index.name)
    client.create_or_update_index(index)
    LOGGER.info("Index %s ready", index.name)


def main() -> None:
    settings = get_settings()
    client = get_search_index_client()
    ensure_index(client, narrative_index(settings.azure_search_index_narrative))
    ensure_index(client, tables_index(settings.azure_search_index_tables))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
