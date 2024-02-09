"""REST client handling, including DataverseStream base class."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable
from urllib.parse import parse_qsl

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_dataverse.auth import DataverseAuthenticator

if sys.version_info >= (3, 9):
    pass
else:
    pass

if TYPE_CHECKING:
    from urllib.parse import ParseResult


_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class DataversePaginator(BaseHATEOASPaginator):
    """Pagination class for Dataverse.

    Retrieves the @odata.nextLink response element
    """

    def get_next_url(self, response: requests.Response) -> str:
        """Return the URL for next page."""
        return response.json().get("@odata.nextLink")


class DataverseStream(RESTStream):
    """Dataverse stream class."""

    """This stream is not actually synced, it is used initially for discovery ONLY."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"""{self.config["api_url"]}/api/data/v{self.config["api_version"]}"""

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        # https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/compose-http-requests-handle-errors#http-headers
        headers["Accept"] = "application/json"
        headers["OData-MaxVersion"] = "4.0"
        headers["OData-Version"] = "4.0"
        headers["If-None-Match"] = None
        return headers

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return DataverseAuthenticator.create_for_stream(
            self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token",
            oauth_scopes=f"{self.config['api_url']}/.default",
        )

    def get_new_paginator(self) -> DataversePaginator:
        """Return a paginator for next page links."""
        return DataversePaginator()

    records_jsonpath = "$.value[*]"  # Or override `parse_response`.

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v

        if next_page_token:
            # Only provide the skiptoken on subsequent requests
            self.logger.info(next_page_token.query)
            params = dict(parse_qsl(next_page_token.query))

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
