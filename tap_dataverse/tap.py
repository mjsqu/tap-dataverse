"""Dataverse tap class."""

from __future__ import annotations

import copy

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

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
            "api_version",
            th.StringType,
            default="9.2",
            description="The API version found in the /api/data/v{x.y} of URLs",
        ),
    )
    
    _stream_properties = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=True,
            description="the path appended to the `api_url`. Stream-level path will "
            "overwrite top-level path",
        ),
        # TODO: Instead of generic "params" - add select/count/filter etc.
        # TODO: Instead of generic "headers" - check what can be added to the Dataverse API
        # TODO: Find out how to infer the primary key from the EntityDetails response
        th.Property(
            "replication_key",
            th.StringType,
            required=False,
            description="the json response field representing the replication key."
            "Note that this should be an incrementing integer or datetime object.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="An optional field. Normally required when using the"
            "replication_key. This is the initial starting date when using a"
            "date based replication key and there is no state available.",
        ),
    )

        # add common properties to top-level properties
    for prop in tap_properties.wrapped.values():
        tap_properties.append(prop)

    # add common properties to the stream schema
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(_stream_properties.wrapped)
    stream_properties.append( 
        th.Property(
            "name", th.StringType, required=True, description="name of the stream"
        ),
    )

    # add streams schema to top-level properties
    tap_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
            description="An array of streams, designed for separate paths using the"
            "same base url.",
        ),
    )

    config_jsonschema = tap_properties.to_dict()

    def discover_streams(self) -> list[DataverseTableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        discovered_streams = []
        streams = self.config["streams"]

        # TODO: Add replication-key of modifiedon - allow configurable
        # TODO: Refactor into a separate function
        for stream in streams:
            logical_name = stream.get('path')
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

            discovered_stream = DataverseTableStream(
                    tap=self,
                    name=logical_name,
                    path=f"/{entity_set_name}",
                    schema=properties.to_dict(),
                    replication_key=stream.get("replication_key"),
                    start_date=stream.get("start_date"),
                )
            
            discovered_streams.append(discovered_stream)

        return discovered_streams


if __name__ == "__main__":
    TapDataverse.cli()
