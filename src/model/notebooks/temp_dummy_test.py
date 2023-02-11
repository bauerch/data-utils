import pandas as pd
from src.model.notebooks import scd


pd.set_option('display.max_columns', None)
param_scd1_columns = [
    "name"
]
param_scd2_columns = [
    "ip_address"
]

scd.apply_scd_updates(
    "dim_user.csv",
    "staging_user.csv",
    "id",
    param_scd1_columns,
    param_scd2_columns
)
