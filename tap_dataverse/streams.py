"""Stream type classes for tap-dataverse."""

from __future__ import annotations

import requests
import sys
from typing import Any, Dict, Generator, Iterable, Optional, Union

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_dataverse.client import DataverseStream
from tap_dataverse.auth import DataverseAuthenticator

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

class DataverseTableStream(DataverseStream):
    """Customised stream for any Dataverse Table."""
    def __init__(
        self,
        tap: Any,
        name: str,
        records_path: str,
        path: str,
        schema: Optional[dict] = None,
    ) -> None:
        
        super().__init__(tap=tap, name=tap.name, schema=schema)

        self.name = name
        self.path = path
        self.records_path = records_path
    
    @property
    def authenticator(self) -> DataverseAuthenticator:
        return DataverseAuthenticator(stream=self)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: required - the requests.Response given by the api call.

        Yields:
              Parsed records.

        """
        yield from extract_jsonpath(self.records_path, input=response.json())


