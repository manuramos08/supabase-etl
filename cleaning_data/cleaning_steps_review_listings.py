import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step
from logging_setup import make_logger
df_l = pd.read_csv(r"D:\Docs\Práctica pipeline\data\clean\listings_cleaned.csv", encoding='UTF-8')
df_r_s = pd.read_csv(r"D:\Docs\Práctica pipeline\data\clean\reviews_summary.csv", encoding='UTF-8')

logger_r_l = make_logger ("cleaning_online_retail", r"D:\Docs\Práctica pipeline\data\logs\merge_review_listings.log")
def merge_listings_review (
             *,
             logger
             ) -> pd.DataFrame:
    before_df_r_s = deepcopy(df_r_s)
    before_df_l = deepcopy(df_l)
    reviews_temp = df_r_s.rename(columns={"listing_id": "id"})
    df_merged = pd.merge(df_l, reviews_temp, on='id', how='left')
    log_step(logger, before_df_r_s, df_merged, step_desc="Merging listings with reviews", extra=f"{before_df_l.shape}")
    assert df_merged.shape[0] == df_l.shape[0], "The number of rows in the merged DataFrame does not match the listings DataFrame."
    return df_merged

def drop_nulls(df:pd.DataFrame, 
               *,
               cols : str = 'review_scores_rating',
               logger) -> pd.DataFrame:
    before_df = deepcopy(df)
    df = df.dropna(subset=cols)
    log_step(logger, before_df, df, step_desc="Dropping nulls in review_scores_rating") 
    assert  df.shape[0] < before_df.shape[0], "The number of rows in the DataFrame did not decrease after dropping nulls."
    return df

def change_name (df:pd.DataFrame,
                 *,
                 col: str = 'last_date',
                 new_name: str = 'last_review_date', 
                 logger) -> pd.DataFrame:
    before_df=deepcopy(df)
    df = df.rename(columns={col:new_name})
    log_step(logger, before_df, df, step_desc=f'Change {col} name to: {new_name}')
    assert df.columns.isin([new_name]).any(), f'No existe {new_name} entre las columnas'
    return df