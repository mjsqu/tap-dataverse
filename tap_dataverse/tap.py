"""Dataverse tap class."""

from __future__ import annotations

import requests

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from typing import Any, Dict, Generator, Iterable, Optional, Union

from tap_dataverse.streams import DataverseTableStream
from tap_dataverse.client import DataverseStream
from tap_dataverse.auth import DataverseAuthenticator

class TapDataverse(Tap):
    """Dataverse tap class."""

    name = "tap-dataverse"
    dynamic_catalog = True

    tap_properties = th.PropertiesList(
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client secret to authenticate against the API service",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="Client (application) ID",
        ),
        th.Property(
            "tenant_id",
            th.StringType,
            required=True,
            description="Tenant ID",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="The url for the API service",
        ),
    )

    config_jsonschema = tap_properties.to_dict()

 
    def discover_streams(self) -> list[DataverseTableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        streams = []

        for stream in self.config["streams"]:
            pass

    def get_schema(self, logical_name: str) -> Any:
        """Retrieve the schema from the /api/v{}/EntitityDefinition(LogicalName={})/Attributes endpoint."""
        # https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/query-metadata-web-api#querying-entitymetadata-attributes
        
        

        pass

    


if __name__ == "__main__":
    TapDataverse.cli()
