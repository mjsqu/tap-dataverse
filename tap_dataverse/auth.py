"""Dataverse Authentication."""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

if TYPE_CHECKING:
    from tap_dataverse.client import DataverseStream


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class DataverseAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Dataverse."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        return {
            "resource": self.config["api_url"],
            "scope": self.oauth_scopes,
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
        }

    @classmethod
    def create_for_stream(
        cls,
        stream: DataverseStream,
        auth_endpoint: str,
        oauth_scopes: str,
    ) -> DataverseAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_endpoint: endpoint for authentication
            oauth_scopes: OAuth scopes

        Returns:
            APIKeyAuthenticator: A new
                :class:`singer_sdk.authenticators.APIKeyAuthenticator` instance.
        """
        return cls(
            stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes
        )
