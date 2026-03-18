"""Integration test configuration.

When ``AZURE_COSMOS_EMULATOR=true`` is set, disable SSL verification so the
tests can connect to the Cosmos DB Emulator's self-signed certificate.

Note: This globally disables SSL verification for the test process.
This is acceptable for integration tests running against a local emulator
but should not be used when connecting to production Cosmos DB accounts.
"""

import os
import ssl


def pytest_configure(config):
    """Disable SSL verification when running against the Cosmos DB Emulator."""
    if os.environ.get("AZURE_COSMOS_EMULATOR", "").lower() == "true":
        ssl._create_default_https_context = ssl._create_unverified_context
