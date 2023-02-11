import pandas as pd
from src.model.notebooks import scd


pd.set_option('display.max_columns', None)

param_columns = [
    "sk",
    "id",
    "first_name",
    "last_name",
    "email",
    "gender",
    "ip_address",
    "effective_from",
    "effective_till",
    "active_flag"
]
param_scd1_columns = [
    "first_name",
    "last_name",
    "email",
    "gender"
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
