"""Tests standard tap features using the built-in SDK tests library."""

import json
from pathlib import Path

from singer_sdk.testing import get_tap_test_class

from tap_dataverse.tap import TapDataverse

config_file = Path(r".secrets/config.json")

with config_file.open() as f:
    sample_config = json.load(f)


# Run standard built-in tap tests from the SDK:
TestTapDataverse = get_tap_test_class(
    tap_class=TapDataverse,
    config=sample_config,
)
