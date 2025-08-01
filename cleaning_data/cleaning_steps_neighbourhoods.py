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

def drop_column (df:pd.DataFrame,
                 *,
                 col: str='neighbourhood_group',
                 logger) -> pd.DataFrame:
    before = deepcopy(df)
    df = df.drop(columns=col)
    log_step(logger, before, df, step_desc=f'Drop column: {col}')
    assert set(df.columns).isdisjoint(col)
    return df
    
def clean_names (df:pd.DataFrame,
                 *,
                 col: str='neighbourhood',
                 logger) -> pd.DataFrame:
    before = deepcopy(df)
    df[col] = df[col].str.replace(' ', '_').str.replace('.', '')
    log_step(logger, before, df, step_desc=f'Clean " " and "_" from: {col}')
    assert not df[col].str.contains(r'\.').any()
    return df

