import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step
from logging_setup import make_logger

df_r = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\airbnb\reviews.csv.gz", encoding='UTF-8')
df_l = pd.read_csv(r"D:\Docs\Práctica pipeline\data\clean\listings_cleaned.csv", encoding='UTF-8')

def drop_columns(df: pd.DataFrame,
                 *,
                 subset: list = ['id', 'reviewer_id', 'reviewer_name', 'comments'],
                 logger) -> pd.DataFrame:
    before = deepcopy(df)
    df = df.drop(columns=subset)
    log_step(logger, before, df, step_desc=f'Eliminar columnas {subset}')
    assert set(df.columns).isdisjoint(subset), "Hay columnas no deseadas en el DataFrame"
    return df

def date_type(df : pd.DataFrame, 
              *, 
              col: str = 'date',
              logger) -> pd.DataFrame:
    before = deepcopy(df)
    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
    log_step(logger, before, df, step_desc=f'Convertir {col} a tipo fecha')
    assert pd.api.types.is_datetime64_any_dtype(df[col]), f'La columna {col} no es de tipo fecha'
    return df

def not_in_listings(df: pd.DataFrame,
                    *,
                    df_2: pd.DataFrame,
                    logger) -> pd.DataFrame:
    before = deepcopy(df)
    reviews_ren = df.rename(columns={"listing_id": "id"})
    mask = reviews_ren['id'].isin(df_2['id'])
    index = df[~mask].index
    df = df.drop(index=index)
    log_step(logger, before, df, step_desc='Eliminar reviews sin listing_id válido')
    assert df['listing_id'].isin(df_2['id']).all(), "Hay listing_id no válidos en el DataFrame de reviews"
    return df

def create_reviews_summary(df: pd.DataFrame,
                           *,
                           logger) -> pd.DataFrame:
    before = deepcopy(df)
    df_r_resumen = df.groupby('listing_id').size().reset_index(name='num_reviews')
    df_r_resumen['last_date'] = df.groupby('listing_id')['date'].max().values
    log_step(logger, before, df_r_resumen, step_desc='Crear resumen de reviews por listing_id')
    assert 'num_reviews' in df_r_resumen.columns, "No se ha creado la columna num_reviews"
    assert 'last_date' in df_r_resumen.columns, "No se ha creado la columna last_date"
    return df_r_resumen

