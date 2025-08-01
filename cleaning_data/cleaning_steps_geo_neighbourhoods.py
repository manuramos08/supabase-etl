import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step
from logging_setup import make_logger
import geopandas as gpd
from shapely import to_wkt  
from shapely.validation import explain_validity 

def drop_column(
                    df: gpd.GeoDataFrame,
                    *,
                    col: str = "neighbourhood_group",
                    logger,
                ) -> gpd.GeoDataFrame:
    before = deepcopy(df)
    df = df.drop(columns=col)
    log_step(logger, before, df, step_desc=f"Drop column: {col}")
    assert col not in df.columns    
    return df


def clean_names(
                df: gpd.GeoDataFrame,
                *,
                col: str = "neighbourhood",
                logger,
            ) -> gpd.GeoDataFrame:
    before = deepcopy(df)
    df[col] = (
        df[col]
          .str.replace(" ", "_", regex=False)   # espacio → _
          .str.replace(".", "", regex=False)    # punto   → «»
    )
    log_step(logger, before, df,
             step_desc=f'Clean " " → "_" and remove "." from: {col}')
    assert not df[col].str.contains(r"\.").any()
    return df