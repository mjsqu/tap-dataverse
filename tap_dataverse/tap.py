"""Dataverse tap class."""

from __future__ import annotations

import requests

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from typing import Any, Dict, Generator, Iterable, Optional, Union

from tap_dataverse.streams import DataverseTableStream
from tap_dataverse.client import DataverseStream
from tap_dataverse.utils import attribute_to_properties


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
        th.Property(
            "streams",
            th.ArrayType(th.StringType),
            required=True,
            description="The list of streams to extract",
        ),
        th.Property(
            "api_version",
            th.StringType,
            default="9.2",
            description="The API version found in the /api/data/v{x.y} of URLs",
        ),
    )

    config_jsonschema = tap_properties.to_dict()

    def discover_streams(self) -> list[DataverseTableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams = []
        logical_names = self.config["streams"]

        # TODO: Add replication-key of modifiedon - allow configurable
        # TODO: Refactor into a separate function
        # TODO: Work out how to add http parameters to a Stream rather than string appends
        for logical_name in logical_names:
            endpoint_root = f"/EntityDefinitions(LogicalName='{logical_name}')"
            discovery_stream = DataverseStream(
                tap=self,
                name="discovery",
                schema=th.PropertiesList(  # type: ignore
                    th.Property("LogicalName", th.IntegerType),
                    th.Property("AttributeType", th.StringType),
                ).to_dict(),
                path=f"{endpoint_root}/Attributes",
                params={
                    "$select": "LogicalName,AttributeType"
                }
            )

            attributes = discovery_stream.get_records(context=None)

            properties = th.PropertiesList()

            for attribute in attributes:
                # TODO: Need to work out how to append a PropertiesList to a PropertiesList
                for property in attribute_to_properties(attribute):
                    properties.append(property)
                

            # Repoint the discovery stream to find the EntitySetName required in the url
            # which accesses the table
            discovery_stream.path = f"{endpoint_root}"
            discovery_stream.params = {"$select":"EntitySetName"}
            discovery_stream.records_jsonpath = "$.[*]"

            entity_definitions = discovery_stream.get_records(context=None)

            for entity_definition in entity_definitions:
                entity_set_name = entity_definition["EntitySetName"]

            stream = DataverseTableStream(
                    tap=self,
                    name=logical_name,
                    path=f"/{entity_set_name}",
                    schema=properties.to_dict(),
                )
            
            streams.append(stream)

        return streams


if __name__ == "__main__":
    TapDataverse.cli()
