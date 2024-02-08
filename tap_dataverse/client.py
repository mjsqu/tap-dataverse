"""REST client handling, including DataverseStream base class."""

from __future__ import annotations
from xml.etree import ElementTree as ET

import sys
from functools import cached_property
from typing import Any, Callable, Iterable
from singer_sdk import typing as th  # JSON Schema typing helpers


import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseHATEOASPaginator

from tap_dataverse.auth import DataverseAuthenticator
from singer_sdk.authenticators import OAuthAuthenticator

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


NS = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm"
}

class DataversePaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        data = response.json()
        return data.get("@odata.nextLink")

class DataverseBaseStream(RESTStream):
    def __init__(self, params = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = params
    
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"""{self.config["api_url"]}/api/data/v{self.config["api_version"]}"""
    
    @property
    def http_headers(self) -> dict:
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
    
    def get_new_paginator(self):
        return DataversePaginator()
    

class DataverseStream(DataverseBaseStream):
    """Dataverse stream class."""

    """This stream is not actually synced, it is used initially for discovery ONLY."""      
    records_jsonpath = "$.value[*]"  # Or override `parse_response`.

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token

        if self.params:
            params = params | self.params
        
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

class MetadataStream(DataverseStream):
    """Metadata stream."""

    name = "metadata"
    path = "/api/data/v9.2/$metadata"
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        tree = ET.fromstring(response.text)

        data_service = tree.find("edmx:DataServices", NS)
        entities = data_service.find("edm:Schema", NS)

        entity_def = {}
        for elem in entities.findall("edm:EntityType", NS):
            # if an Entity doesn't have elements or a `Key` skip over it
            if len(elem) and elem.find("edm:Key", NS):
                entity_key = elem.find("edm:Key", NS).find("edm:PropertyRef", NS).get("Name")
                entity_name = elem.get("Name")

                props = []
                for prop in elem.findall("edm:Property", NS):
                    prop_name = prop.get("Name")
                    prop_type = prop.get("Type")
                    props.append({"LogicalName": prop_name, "PropertyType": prop_type})

                entity_def.update({entity_name: {"Key": entity_key, "Properties": props}})

        yield entity_def
