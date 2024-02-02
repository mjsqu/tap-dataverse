"""Dataverse tap class."""

from __future__ import annotations

import requests

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from typing import Any, Dict, Generator, Iterable, Optional, Union

from tap_dataverse.streams import DataverseTableStream
from tap_dataverse.client import DataverseStream
from tap_dataverse.utils import attribute_type_to_jsonschema_type


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
    )

    config_jsonschema = tap_properties.to_dict()

    def discover_streams(self) -> list[DataverseTableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams = []
        logical_names = self.config["streams"]
        
        for logical_name in logical_names:
            discovery_stream = DataverseStream(
                tap=self,
                name="discovery",
                schema=th.PropertiesList(  # type: ignore
                    th.Property("LogicalName", th.IntegerType),
                    th.Property("AttributeType", th.StringType),
                ).to_dict(),
                path=f"/api/data/v9.2/EntityDefinitions(LogicalName='{logical_name}')/Attributes?$select=LogicalName,AttributeType",
            )

            attributes = discovery_stream.get_records(context=None)
            
            properties = th.PropertiesList()
            
            for attribute in attributes:
                self.logger.info(attribute)
                # Build a schema and translate datatypes from Dynamics to JSONSchema
                properties.append(th.Property(attribute["LogicalName"],attribute_type_to_jsonschema_type(attribute["AttributeType"])))
            
            discovery_stream.path = f"/api/data/v9.2/EntityDefinitions(LogicalName='{logical_name}')?$select=EntitySetName"
            discovery_stream.records_jsonpath = "$.[*]"
            
            entity_definitions = discovery_stream.get_records(context=None)
            
            for entity_definition in entity_definitions:
                entity_set_name = entity_definition['EntitySetName']

            streams.append(DataverseTableStream(tap=self, name=logical_name, path=f"/api/data/v9.2/{entity_set_name}",schema=properties.to_dict()))
        
        return streams

if __name__ == "__main__":
    TapDataverse.cli()
