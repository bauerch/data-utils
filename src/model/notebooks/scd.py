import datetime
import hashlib
import pandas as pd
import numpy as np
import uuid
from typing import (
    Optional,
    Iterable
)


def calculate_md5_hash(row: Iterable[str]) -> str:
    """
    Creates a MD5 hash from an iterable.
    """
    # Convert all values to string, concatenate and encode before hashing
    hashable_str = "".join(map(str, row)).encode("utf-8")

    # Create a md5 hash from the resulting string
    md5_hash = hashlib.md5(hashable_str).hexdigest()

    return md5_hash


def add_scd2_checksum_column(
        dataframe: pd.DataFrame,
        column_name: str,
        scd2_columns: Optional[list[str]] = None
) -> pd.DataFrame:
    """
    Adds a column to a dataframe with a MD5 hash for every row.
    """
    # If SCD2 columns are specified, filter to just these columns,
    # otherwise all columns are considered SCD2 columns
    scd2_dataframe = dataframe.loc[:, list(scd2_columns)] if scd2_columns else dataframe

    # Calculate checksum (md5 hash) for SCD2 columns per row
    md5_hashes_series = scd2_dataframe.apply(lambda row: calculate_md5_hash(row), axis=1)

    # Copy the input dataframe and add checksum series as a new column
    result = dataframe.copy()
    result[column_name] = md5_hashes_series

    return result


def scd1(
        df_dim: pd.DataFrame,
        df_stg: pd.DataFrame,
        key: str,
        scd1_columns: Optional[list[str]] = None
) -> Optional[pd.DataFrame]:
    """
    """
    try:
        df_result_merged = pd.merge(
            df_dim,
            df_stg,
            how="left",
            on=key,
            suffixes=("", "_stg")
        )

        # Update SCD1 columns in dimension table
        for column in scd1_columns:
            dim_column_name = f"{column}"
            stg_column_name = f"{column}_stg"
            dim_column = df_result_merged[dim_column_name]
            stg_column = df_result_merged[stg_column_name]

            df_result_merged.loc[dim_column != stg_column, dim_column_name] = stg_column

        # Create new dataframe with updated table
        columns = [column for column in df_result_merged.columns if column.endswith("_stg")]
        df_result_merged = df_result_merged.drop(columns, axis=1)
    except Exception as error:
        print(error)
        df_result_merged = None

    return df_result_merged


def scd2(
        df_dim: pd.DataFrame,
        df_stg: pd.DataFrame,
        key: str,
        scd2_columns: Optional[list[str]] = None
) -> Optional[pd.DataFrame]:
    """
    """
    default_date = datetime.datetime.strptime('9999-12-31', '%Y-%m-%d').date()
    current_date = datetime.datetime.today().date()

    # Prepare dimension table for merge by filtering active records
    df_dim_merge = df_dim[df_dim["active_flag"] == 1]

    # Prepare staging table for merge by adding SCD2 columns effective_from, effective_till
    # and active_flag to the dataframe
    df_stg_merge = df_stg.copy()
    df_stg_merge["effective_from"] = current_date
    df_stg_merge["effective_till"] = default_date
    df_stg_merge["active_flag"] = 1

    # Calculate checksums for SCD2 columns in both the dimension and staging table
    df_dim_merge = add_scd2_checksum_column(
        df_dim_merge,
        column_name="scd2_checksum",
        scd2_columns=scd2_columns
    )

    df_stg_merge = add_scd2_checksum_column(
        df_stg_merge,
        column_name="scd2_checksum",
        scd2_columns=scd2_columns
    )

    try:
        df_result_merged = pd.merge(
            df_dim_merge,
            df_stg_merge,
            how="outer",
            on=key,
            suffixes=("", "_stg")
        )

        # Flag each row with SCD2 actions to be performed
        hash_dim = df_result_merged["scd2_checksum"]
        hash_stg = df_result_merged["scd2_checksum_stg"]

        conditions = [
            (hash_dim != hash_stg) & hash_dim.notnull() & hash_stg.notnull(),
            (hash_dim != hash_stg) & hash_dim.isnull() & hash_stg.notnull()
        ]
        choices = [
            "UPSERT",
            "INSERT"
        ]

        df_result_merged["scd2_action_stg"] = np.select(conditions, choices, default=None)

        # TBD.
        df_filter_no_action = df_result_merged[df_result_merged["scd2_action_stg"].isnull()]
        df_result_no_action = no_action_handler(df_filter_no_action, key)

        # TBD.
        df_filter_insert = df_result_merged[df_result_merged["scd2_action_stg"] == "INSERT"]
        df_result_insert = insert_handler(df_filter_insert, key)

        # TBD.
        df_filter_upsert = df_result_merged[df_result_merged["scd2_action_stg"] == "UPSERT"]
        df_result_upsert = upsert_handler(df_filter_upsert, key)

        # TODO: Ensure consistent columns in all dataframes, remove technical columns
        # TODO: Add active_flag == 0 records from current dimension table
        print("NO ACTION:", df_result_no_action.columns)
        print("INSERT:", df_result_insert.columns)
        print("UPSERT:", df_result_upsert.columns)

        result = None
    except Exception as error:
        print(error)
        result = None

    return result


def no_action_handler(
    dataframe: pd.DataFrame,
    key: str
) -> pd.DataFrame:
    """
    """
    return pd.DataFrame()


def insert_handler(
        dataframe: pd.DataFrame,
        key: str
) -> pd.DataFrame:
    """
    """
    # Create a new dataframe which only contains staging data
    columns = [column for column in dataframe.columns if column.endswith("_stg")]
    columns.append(key)
    result = dataframe[columns]

    # Create a surrogate key for each row added
    result.insert(0, "sk", [str(uuid.uuid4()) for _ in range(len(result.index))])

    # Remove staging suffix from column names in the dataframe
    result = result.copy()
    result.rename(
        columns={column: column[:-4] for column in columns if column.endswith("_stg")},
        inplace=True
    )

    return result


def upsert_handler(
        dataframe: pd.DataFrame,
        key: str,
        current_date: datetime.date = datetime.datetime.today().date()
) -> pd.DataFrame:
    """
    """
    # Insert a new row in the dimension table with the latest data
    result_insert = insert_handler(dataframe, key)

    # Create a new dataframe which only contains dimension table data.
    columns = [column for column in dataframe.columns if not column.endswith("_stg")]
    columns.append(key)
    result_update = dataframe[columns]

    # Update the active_flag, effective_till for existing rows
    result_update = result_update.copy()
    result_update.loc[:, ("effective_till", "active_flag")] = (current_date, 0)

    # TODO: Ensure consistent columns in all dataframes, remove technical columns
    print(result_insert.columns)
    print(result_update.columns)

    return pd.concat([result_insert, result_update], axis=0)


def apply_scd_updates(
        dim_file: str,
        stg_file: str,
        key: str,
        scd1_columns: list[str],
        scd2_columns: list[str]
) -> None:
    """
    """
    try:
        # Read CSVs files into dataframe
        df_dim = pd.read_csv(dim_file, delimiter=",")
        df_stg = pd.read_csv(stg_file, delimiter=",")

        # Perform SCD Type 1 updates
        result = scd1(
            df_dim,
            df_stg,
            key=key,
            scd1_columns=scd1_columns
        )

        # Perform SCD Type 2 updates
        result = scd2(
            result,
            df_stg,
            key=key,
            scd2_columns=scd2_columns
        )
    except Exception as error:
        print(error)
    else:
        print(result)
