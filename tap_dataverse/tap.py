"""Dataverse tap class."""

from __future__ import annotations

import copy

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_dataverse.streams import DataverseTableStream
from tap_dataverse.client import DataverseStream
from tap_dataverse.utils import attribute_type_to_jsonschema_type, sql_attribute_name


class TapDataverse(Tap):
    """Dataverse tap class."""

    name = "tap-dataverse"
    #dynamic_catalog = True

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
        th.Property(
            "sql_attribute_names",
            th.BooleanType,
            default=False,
            description="Uses the Snowflake column name rules to translate any"
            "characters outside the standard to an underscore. Particularly helpful"
            "when annotations are turned on",
        ),
        th.Property(
            "annotations",
            th.BooleanType,
            default=False,
            description="Turns on annotations",
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

        # TODO: Refactor into a separate function
        for stream in streams:
            logical_name = stream.get("path")
            endpoint_root = f"/EntityDefinitions(LogicalName='{logical_name}')"
            discovery_stream = DataverseStream(
                tap=self,
                name="discovery",
                schema=th.PropertiesList(  # type: ignore
                    th.Property("LogicalName", th.IntegerType),
                    th.Property("AttributeType", th.StringType),
                ).to_dict(),
                path=f"{endpoint_root}/Attributes",
                params={"$select": "LogicalName,AttributeType"},
            )
            self.logger.info(discovery_stream.get_starting_replication_key_value(None))

            attributes = discovery_stream.get_records(context=None)

            properties = th.PropertiesList()

            for attribute in attributes:
                for property in self.attribute_to_properties(attribute):
                    properties.append(property)

            # Repoint the discovery stream to find the EntitySetName required in the url
            # which accesses the table
            discovery_stream.path = f"{endpoint_root}"
            discovery_stream.params = {"$select": "EntitySetName"}
            discovery_stream.records_jsonpath = "$.[*]"

            entity_definitions = discovery_stream.get_records(context=None)

            for entity_definition in entity_definitions:
                entity_set_name = entity_definition["EntitySetName"]

            discovered_stream = DataverseTableStream(
                tap=self,
                name=logical_name,
                path=f"/{entity_set_name}",
                schema=properties.to_dict(),
                start_date=stream.get("start_date", self.config.get("start_date", "")),
                replication_key=stream.get(
                    "replication_key", self.config.get("replication_key", "")
                ),
            )

            discovered_streams.append(discovered_stream)

        return discovered_streams
    
    def annotation(self, original_annotation: str):
        if self.config.get('sql_attribute_names'):
            return sql_attribute_name(original_annotation)
        else:
            return original_annotation
    
    def attribute_to_properties(self, attribute: dict) -> list:
        """
        TODO: Handle this:
        "_owningbusinessunit_value@OData.Community.Display.V1.FormattedValue": "ipmhadev",
        "_owningbusinessunit_value@Microsoft.Dynamics.CRM.associatednavigationproperty": "owningbusinessunit",
        "_owningbusinessunit_value@Microsoft.Dynamics.CRM.lookuplogicalname": "businessunit",
        """
        """
        Special cases:
            - Money - has an extra 'base' column
            - UniqueIdentifier - is a uuid type
        """

        FORMATTED = [
            "BigInt",
            "DateTime",
            "Decimal",
            "Double",
            "Integer",
            "Money",
            "Picklist",
            "State",
            "Status",
        ]
        FORMATTED_NAV_LKUP = ["Lookup", "Owner"]

        properties = []

        if attribute["AttributeType"] in FORMATTED:
            base_property = th.Property(
                attribute["LogicalName"],
                attribute_type_to_jsonschema_type(attribute["AttributeType"])
            )

            properties.append(base_property)

            formatted_property = th.Property(
                f"""{attribute["LogicalName"]}{self.annotation("@OData.Community.Display.V1.FormattedValue")}""",
                th.StringType,
            )
            properties.append(formatted_property)

            return properties

        elif attribute["AttributeType"] in FORMATTED_NAV_LKUP:
            modified_name = f"""_{attribute["LogicalName"]}_value"""
            """
            Sample: 
                "_ownerid_value@OData.Community.Display.V1.FormattedValue": "sa_BIDWScheduling #",
                "_ownerid_value@Microsoft.Dynamics.CRM.associatednavigationproperty": "ownerid",
                "_ownerid_value@Microsoft.Dynamics.CRM.lookuplogicalname": "systemuser",
                "_ownerid_value": "0ae8c9a0-923b-ed11-bba3-0022481563ba",
            """
            
            properties.append(th.Property(
                modified_name,
                th.UUIDType,
            ))

            # If sql_attribute_names is set, these names should be cleaned up
            # this should be paired with post_process in the DataverseTableStream
            properties.append(th.Property(
                f"""{modified_name}{self.annotation("@OData.Community.Display.V1.FormattedValue")}""",
                th.StringType,
            ))
            properties.append(th.Property(
                f"""{modified_name}{self.annotation("@Microsoft.Dynamics.CRM.associatednavigationproperty")}""",
                th.StringType,
            ))
            properties.append(th.Property(
                f"""{modified_name}{self.annotation("@Microsoft.Dynamics.CRM.lookuplogicalname")}""",
                th.StringType,
            ))
            return properties

        else:
            properties.append(th.Property(
                attribute["LogicalName"],
                attribute_type_to_jsonschema_type(attribute["AttributeType"])
            ))

            if attribute["AttributeType"] == "Money":
                properties.append(th.Property(
                    f"""{attribute["LogicalName"]}_base""",
                    attribute_type_to_jsonschema_type(attribute["AttributeType"])
                ))

            return properties


if __name__ == "__main__":
    TapDataverse.cli()
