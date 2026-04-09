"""Data provider router. Set DATA_PROVIDER=a_share to use A-share data."""

import os

PROVIDER = os.environ.get("DATA_PROVIDER", "us").lower()

if PROVIDER == "a_share":
    from src.tools.api_a_share import *  # noqa: F401,F403
    # prices_to_df only exists in api.py, re-implement for a_share
    import pandas as pd
    def prices_to_df(prices):
        if not prices:
            return pd.DataFrame()
        return pd.DataFrame([p.model_dump() for p in prices])
else:
    from src.tools.api import *  # noqa: F401,F403
