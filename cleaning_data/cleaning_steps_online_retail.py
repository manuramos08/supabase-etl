import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step

def drop_customer_id_na(df: pd.DataFrame,
                        *,
                        logger,               
                        col: str = "Customer ID") -> pd.DataFrame:
    before = deepcopy(df)
    df = df.dropna(subset=[col])
    log_step(logger, before, df,
             step_desc=f"Drop {col} nulos")
    assert df[col].isna().sum() == 0, \
           f"Quedaron nulos en '{col}' tras dropna"
    return df

def drop_price_bajo(df: pd.DataFrame,
                    *,
                    logger,
                    col: str = 'Price') -> pd.DataFrame:
    before = deepcopy(df)
    price_bajo = df[col] <= 0
    filas_bajas = price_bajo.sum()
    index_price_bajo = df[price_bajo].index
    df = df.drop(index=index_price_bajo)
    log_step(logger, before, df, step_desc="Drop Price <= 0", extra=f"filas quitadas: {filas_bajas}")
    assert (df[col] <= 0).sum() == 0, "Hay precios <= 0 tras drop"
    return df 

def convert_to_datetime(df: pd.DataFrame,
                        *,
                        logger,
                        col: str = 'InvoiceDate') -> pd.DataFrame:
    before = deepcopy(df)
    df[col] = pd.to_datetime(df[col], format = '%Y-%m-%d %H:%M:%S')
    log_step(logger, before, df, step_desc="Convert InvoiceDate to datetime")
    assert pd.api.types.is_datetime64_any_dtype(df[col]), \
       "'InvoiceDate' NO es datetime64 después de la conversión"
    return df

def create_month_day(df: pd.DataFrame,
                     *,
                     logger,
                     col: str = 'InvoiceDate') -> pd.DataFrame:
    before = deepcopy(df)
    df['Month'] = df[col].dt.month
    df['Year'] = df[col].dt.year
    log_step(logger, before, df, step_desc="Create Year and Month from InvoiceDate")
    assert {"Month", "Year"}.issubset(df.columns), \
       "Las columnas 'Month' y/o 'Year' no existen"
    assert df[["Month", "Year"]].isna().sum().sum() == 0, \
       "'Month' o 'Year' contienen valores nulos"
    return df

def drop_retail_dupes (df: pd.DataFrame,
                       *,
                       logger,
                       subset: tuple[str, ...] = ('Invoice', 'StockCode', 'Customer ID')
                      ) -> pd.DataFrame:
    before = deepcopy(df)
    duplicados = df.duplicated(subset=subset).sum()
    df = df.drop_duplicates(subset=subset)
    log_step(logger, before, df, step_desc=f"Drop duplicates {' + '.join(subset)}", extra=f"filas quitadas: {duplicados}")
    assert df.duplicated(subset=subset).sum() == 0, \
       "Hay filas duplicadas tras drop_duplicates"
    assert (len(before) - len(df)) == duplicados, \
       "El conteo de duplicados eliminados no coincide con 'duplicados'"
    return df
 

 


    

