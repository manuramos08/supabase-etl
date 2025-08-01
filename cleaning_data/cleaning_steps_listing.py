import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import log_step

def limpiar_columnas(df: pd.DataFrame, 
                     *, 
                     logger) -> pd.DataFrame:
    before = deepcopy(df)
    columns_mantain = ['id', 'neighbourhood_cleansed', 'latitude', 'longitude', 'property_type', 'accommodates', 'bedrooms', 'beds', 'price', 'minimum_nights', 'maximum_nights', 'availability_365', 'number_of_reviews', 'reviews_per_month', 'review_scores_rating']

    cols_drop = set(df.columns) - set(columns_mantain)
    df = df.drop(columns=cols_drop)
    log_step(logger, before, df, step_desc='Eliminar columnas innecesarias')
    assert all(
            cols in columns_mantain
            for cols in df.columns), f'Columnas no deseadas encontradas'  
    return df    

def drop_nulls(df: pd.DataFrame,
               *,
               cols: str = 'price',
               logger) -> pd.DataFrame:
    before = deepcopy(df)
    df = df.dropna(subset=[cols])
    log_step(logger, before, df, step_desc=f'Eliminar filas con valores nulos en {cols} específicas')
    assert df['price'].isnull().sum() == 0, f'Valores nulos encontrados en {cols}'
    return df

def clean_price(df: pd.DataFrame, 
                *, 
                cols: str='price',
                logger) -> pd.DataFrame:
    before=deepcopy(df)
    df[cols] = (df[cols].str.replace('$', '').str.replace(',', '').astype(float)).round(2)
    log_step(logger, before, df, step_desc=f'Limpiar {cols} (elimina $ y ,) y se convierte a float')
    assert df[cols].dtype == 'float64', f'Error al convertir {cols} a float'
    return df


def clean_neighbourhood(df: pd.DataFrame,
                        *,
                        cols: str='neighbourhood_cleansed',
                        logger) -> pd.DataFrame:
    before = deepcopy(df)
    df[cols] = df[cols].str.replace('.', '').str.replace('NuÃ±ez', 'Nuñez').str.replace(' ', '_')
    log_step(logger, before, df, step_desc=f'Limpia {cols} (elimina . y corrige NuÃ±ez)')
    assert not df[cols].str.contains(r'\.').any(), "Aún hay '.' en neighbourhood"
    return df

def check_availability(df: pd.DataFrame,
                        *,
                        cols: str='availability_365',
                        logger) -> pd.DataFrame:
    before = deepcopy(df)
    mask = (df[cols] == 0) | (df[cols].isna())
    idx = df[mask].index
    df = df.drop(index=idx)
    log_step(logger, before, df, step_desc=f'Eliminar filas con {cols} = 0 o nulos')
    assert df[cols].min() >= 0, f'Valores negativos encontrados en {cols}'
    return df
             
