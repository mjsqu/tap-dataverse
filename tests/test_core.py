"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json

from singer_sdk.testing import get_tap_test_class

from tap_dataverse.tap import TapDataverse

with open(r".secrets/config.json") as f:
    sample_config = json.load(f)


# Run standard built-in tap tests from the SDK:
TestTapDataverse = get_tap_test_class(
    tap_class=TapDataverse,
    config=sample_config,
)


# TODO: Create additional tests as appropriate for your tap.

