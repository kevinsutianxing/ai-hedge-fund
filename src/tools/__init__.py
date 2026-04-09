"""Data provider router. Set DATA_PROVIDER=a_share to use A-share data."""

import os

PROVIDER = os.environ.get("DATA_PROVIDER", "us").lower()

if PROVIDER == "a_share":
    from src.tools.api_a_share import *  # noqa: F401,F403
else:
    from src.tools import *  # noqa: F401,F403
