import logging
from typing import Any, List, Mapping, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("airbyte")

RETRY_TOTAL = 3
RETRY_BACKOFF_FACTOR = 1.0  # 1s, 2s, 4s
RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]


class DustClient:
    """HTTP client for the Dust document and table upsert APIs."""

    def __init__(self, config: Mapping[str, Any]):
        self.api_key = config["api_key"]
        self.workspace_id = config["workspace_id"]
        self.space_id = config["space_id"]
        self.data_source_id = config["data_source_id"]
        self.base_url = config.get("base_url", "https://dust.tt").rstrip("/")

        self._documents_base = (
            f"{self.base_url}/api/v1/w/{self.workspace_id}"
            f"/spaces/{self.space_id}"
            f"/data_sources/{self.data_source_id}"
            f"/documents"
        )

        # Tables API uses underscores in path (data_sources not data-sources)
        self._tables_base = (
            f"{self.base_url}/api/v1/w/{self.workspace_id}"
            f"/spaces/{self.space_id}"
            f"/data_sources/{self.data_source_id}"
            f"/tables"
        )

        self._session = requests.Session()
        retry = Retry(
            total=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_FORCELIST,
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

    def check_connection(self, data_format: str = "documents") -> None:
        """
        Verify credentials and data source existence.

        For documents: GETs /documents?limit=1
        For tables: GETs /tables (list tables)
        """
        if data_format == "tables":
            url = self._tables_base
            response = self._session.get(url, timeout=30)
        else:
            url = f"{self._documents_base}?limit=1"
            response = self._session.get(url, timeout=30)

        if response.status_code == 401:
            raise ConnectionError("Authentication failed. Check your API key.")
        if response.status_code == 404:
            raise ConnectionError(
                f"Data source not found. Verify workspace_id='{self.workspace_id}', "
                f"space_id='{self.space_id}', data_source_id='{self.data_source_id}'."
            )
        if not response.ok:
            raise ConnectionError(
                f"Dust API returned status {response.status_code}: "
                f"{response.text[:500]}"
            )

        logger.info("Dust connection check succeeded.")

    def upsert_document(
        self,
        document_id: str,
        title: str,
        text: str,
        source_url: str = "",
        tags: Optional[List[str]] = None,
        timestamp: Optional[int] = None,
    ) -> dict:
        """
        Upsert a document into the configured Dust data source.

        Raises RuntimeError on API errors after retries are exhausted.
        """
        url = f"{self._documents_base}/{document_id}"
        payload: dict[str, Any] = {
            "title": title,
            "mime_type": "application/json",
            "text": text,
            "source_url": source_url,
            "tags": tags or [],
            "light_document_output": True,
            "async": False,
        }
        if timestamp is not None:
            payload["timestamp"] = timestamp

        response = self._session.post(url, json=payload, timeout=60)

        if response.status_code == 429:
            raise RuntimeError(
                f"Rate limited by Dust API after {RETRY_TOTAL} retries. "
                "Consider reducing sync frequency."
            )

        if not response.ok:
            raise RuntimeError(
                f"Failed to upsert document '{document_id}': "
                f"status={response.status_code}, body={response.text[:500]}"
            )

        return response.json()

    def upsert_table(
        self,
        table_id: str,
        name: str,
        description: str = "",
        columns: Optional[List[dict[str, Any]]] = None,
    ) -> dict:
        """
        Create or update a table definition in the configured Dust data source.

        Args:
            table_id: Unique identifier for the table
            name: Human-readable table name
            description: Optional table description
            columns: List of column definitions, each with 'name' and 'type'

        Returns:
            API response containing table metadata including table ID

        Raises RuntimeError on API errors after retries are exhausted.
        """
        url = self._tables_base
        payload: dict[str, Any] = {
            "id": table_id,
            "name": name,
        }
        if description:
            payload["description"] = description
        if columns:
            payload["columns"] = columns

        response = self._session.post(url, json=payload, timeout=60)

        if response.status_code == 429:
            raise RuntimeError(
                f"Rate limited by Dust API after {RETRY_TOTAL} retries. "
                "Consider reducing sync frequency."
            )

        if not response.ok:
            raise RuntimeError(
                f"Failed to upsert table '{table_id}': "
                f"status={response.status_code}, body={response.text[:500]}"
            )

        return response.json()

    def upsert_rows(
        self,
        table_id: str,
        rows: List[dict[str, Any]],
    ) -> dict:
        """
        Upsert rows into an existing table.

        Args:
            table_id: The table ID to upsert rows into
            rows: List of row objects (dicts with column names as keys)

        Returns:
            API response

        Raises RuntimeError on API errors after retries are exhausted.
        """
        url = f"{self._tables_base}/{table_id}/rows"
        payload = {"rows": rows}

        response = self._session.post(url, json=payload, timeout=60)

        if response.status_code == 429:
            raise RuntimeError(
                f"Rate limited by Dust API after {RETRY_TOTAL} retries. "
                "Consider reducing sync frequency."
            )

        if not response.ok:
            raise RuntimeError(
                f"Failed to upsert rows into table '{table_id}': "
                f"status={response.status_code}, body={response.text[:500]}"
            )

        return response.json()
