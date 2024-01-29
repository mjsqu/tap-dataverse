"""Dataverse tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

# TODO: Import your custom stream types here:
from tap_dataverse import streams


class TapDataverse(Tap):
    """Dataverse tap class."""

    name = "tap-dataverse"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
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
            "resource",
            th.StringType,
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
    ).to_dict()

 
    def discover_streams(self) -> list[streams.DataverseStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        """The MetadataStream class is used only to get the list of other classes """
        """It's a kind of dummy class that doesn't end up being synced but is used purely to generate the catalog """
        metadata = streams.MetadataStream(self)
        for record in metadata.get_records(None):
            print(record)
        
        return []


if __name__ == "__main__":
    TapDataverse.cli()
