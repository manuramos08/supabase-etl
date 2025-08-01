import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step

def mod_nombres_columnas(df: pd.DataFrame,
                         *,
                         logger) -> pd.DataFrame:
    before = deepcopy(df)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    log_step(logger, before, df, step_desc="Modificar nombres de columnas")
    assert all(
        col == col.lower().replace(' ', '_') 
        for col in df.columns
    ), "Quedan columnas sin normalizar"
    return df

def pasar_a_fecha(df: pd.DataFrame,
                  *,
                  subset: tuple[str, ...] = ('order_date', 'ship_date'),
                  logger) -> pd.DataFrame:
    before = deepcopy(df)
    for col in subset:
        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
    log_step(logger, before, df, step_desc="Pasar a formato fecha")
    assert all(
        pd.api.types.is_datetime64_any_dtype(df[col]) 
        for col in subset
    ), "No se han convertido las columnas a tipo fecha"
    return df

def profit_margin(df: pd.DataFrame,
                  *,
                  subset: tuple[str, str] = ('profit', 'sales'),
                  logger) -> pd.DataFrame:
    before = deepcopy(df)
    df['profit_margin'] = (df[subset[0]] / df[subset[1]] * 100).round(2)
    log_step(logger, before, df, step_desc="Crear columna profit_margin")
    assert 'profit_margin' in df.columns, "No se ha creado la columna profit_margin"
    return df

def drop_order_dupes(df: pd.DataFrame,
               *,
               subset: tuple[str, str] = ('order_id', 'customer_id'),
               logger) -> pd.DataFrame:
    before = deepcopy(df)
    df = df.drop_duplicates(subset=subset)
    log_step(logger, before, df, step_desc="Eliminar duplicados por order_id y customer_id")
    assert df.duplicated(subset=subset).sum() == 0, "Quedan duplicados en el DataFrame"
    return df

def drop_column_nulls (df: pd.DataFrame, 
                *, 
                columns: str = 'postal_code',
                logger) -> pd.DataFrame:
    before = deepcopy(df)
    df = df.drop(columns=columns)
    log_step(logger, before, df, step_desc=f"Eliminar columna {columns} con muchos nulos")
    assert columns not in df.columns, f"La columna {columns} no se ha eliminado correctamente"
    return df

    
