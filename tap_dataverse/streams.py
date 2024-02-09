"""Stream type classes for tap-dataverse."""

from __future__ import annotations

import requests
import sys
from typing import Any, Dict, Generator, Iterable, Optional, Union
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from urllib.parse import parse_qsl

from tap_dataverse.client import DataverseBaseStream
from tap_dataverse.auth import DataverseAuthenticator
from tap_dataverse.utils import sql_attribute_name

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_TToken = t.TypeVar("_TToken")

class DataverseTableStream(DataverseBaseStream):
    """Customised stream for any Dataverse Table."""
    def __init__(
        self,
        tap: Any,
        name: str,
        path: str,
        schema: Optional[dict] = None,
        replication_key: str = None,
        start_date: str = None,
    ) -> None:
        
        super().__init__(tap=tap, name=tap.name, schema=schema,)
        
        self.tap = tap
        self.name = name
        self.path = path
        self.records_path = "$.value[*]"
        # TODO: Properly implement replication keys and start dates
        self.replication_key = replication_key
        self.start_date = start_date
    
    @property
    def http_headers(self) -> dict:
        # TODO: Make configurable
        # TODO: Add default headers
        headers = super().http_headers
        if self.config.get('annotations'):
            headers["Prefer"] = 'odata.include-annotations="*"'
        return headers

    @property
    def authenticator(self) -> DataverseAuthenticator:
        return DataverseAuthenticator(stream=self)
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
        # Initialise Starting Values
        # TODO: start_date not being picked up from config
        try:
            last_run_date=self.get_starting_timestamp(context).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except (ValueError, AttributeError):
            last_run_date=self.get_starting_replication_key_value(context)

        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        
        if self.replication_key and last_run_date:
            params["$orderby"] = f"{self.replication_key} asc"
            params["$filter"] = f"{self.replication_key} ge {last_run_date}"
        
        if next_page_token:
            # Only provide the skiptoken on subsequent requests
            self.logger.info(next_page_token.query)
            params = dict(parse_qsl(next_page_token.query))

        self.logger.info(params)
        return params


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: required - the requests.Response given by the api call.

        Yields:
              Parsed records.

        """
        yield from extract_jsonpath(self.records_path, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        if self.config.get('sql_attribute_names'):
            """
            SQL identifiers and key words must begin with a letter (a-z, but 
            also letters with diacritical marks and non-Latin letters) or an 
            underscore (_). Subsequent characters in an identifier or key word 
            can be letters, underscores, digits (0-9), or dollar signs ($). Note 
            that dollar signs are not allowed in identifiers according to the 
            letter of the SQL standard, so their use might render applications 
            less portable
            """
            return {sql_attribute_name(k):v for k,v in row.items()}
        else:
            return row
