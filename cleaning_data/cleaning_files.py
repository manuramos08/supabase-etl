import pandas as pd
import numpy as np
import datetime as dt
import logging
from copy import deepcopy
from pathlib import Path
from logging_setup import make_logger
import cleaning_steps_online_retail as csor
import cleaning_steps_global_superstore as csgs
import cleaning_steps_listing as csl
import cleaning_steps_neighbourhoods as csn
import geopandas as gpd
import cleaning_steps_geo_neighbourhoods as csgn


df_online_retail = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\online_retail_II.csv")

logger_or = make_logger ("cleaning_online_retail", r"D:\Docs\Práctica pipeline\data\logs\cleaning_online_retail.log")
df_online_retail = (
    df_online_retail
      .pipe(csor.drop_customer_id_na, logger=logger_or)
      .pipe(csor.drop_price_bajo, logger=logger_or)
      .pipe(csor.convert_to_datetime, logger=logger_or)
      .pipe(csor.create_month_day, logger=logger_or)
      .pipe(csor.drop_retail_dupes, logger=logger_or)
)
outfile_or = Path(r"D:\Docs\Práctica pipeline\data\clean\online_retail_II_cleaned.csv")
df_online_retail.to_csv(outfile_or, index=False, encoding='utf-8-sig')


df_global_superstore = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\Global_Superstore2.csv", encoding='latin1')
logger_gs = make_logger ("cleaning_global_superstore", r"D:\Docs\Práctica pipeline\data\logs\cleaning_global_superstore.log")

df_global_superstore = (
  df_global_superstore
    .pipe(csgs.mod_nombres_columnas, logger=logger_gs)
    .pipe(csgs.pasar_a_fecha, logger=logger_gs)
    .pipe(csgs.profit_margin, logger=logger_gs)
    .pipe(csgs.drop_order_dupes, logger=logger_gs)
    .pipe(csgs.drop_column_nulls, logger=logger_gs)
)
outfile_gs = Path(r"D:\Docs\Práctica pipeline\data\clean\Global_Superstore2_cleaned.csv")
df_global_superstore.to_csv(outfile_gs, index=False, encoding='utf-8-sig')


df_listings = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\airbnb\listings.csv.gz", encoding='latin1')
logger_ls = make_logger ("cleaning_listings", r"D:\Docs\Práctica pipeline\data\logs\cleaning_listings.log")

df_listings = (
  df_listings
    .pipe(csl.limpiar_columnas, logger=logger_ls)
    .pipe(csl.drop_nulls, logger=logger_ls) 
    .pipe(csl.clean_price, logger=logger_ls)
    .pipe(csl.clean_neighbourhood, logger=logger_ls) 
    .pipe(csl.check_availability, logger=logger_ls) 
)
outfile_l = Path(r"D:\Docs\Práctica pipeline\data\clean\listings_cleaned.csv")
df_listings.to_csv(outfile_l, index=False, encoding='utf-8-sig')

import cleaning_reviews as cr


df_r = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\airbnb\reviews.csv.gz", encoding='UTF-8')
df_l = pd.read_csv(r"D:\Docs\Práctica pipeline\data\clean\listings_cleaned.csv", encoding='UTF-8')
logger_r = make_logger ("cleaning_reviews", r"D:\Docs\Práctica pipeline\data\logs\cleaning_reviews.log")

df_r_r = (
    df_r 
    .pipe(cr.drop_columns, logger=logger_r)
    .pipe(cr.date_type, logger=logger_r)
    .pipe(cr.not_in_listings, df_2=df_l, logger=logger_r)
)
df_r_resumen = cr.create_reviews_summary(df_r_r, logger=logger_r)

outfile_r = Path(r"D:\Docs\Práctica pipeline\data\clean\reviews_cleaned.csv")
df_r.to_csv(outfile_r, index=False, encoding='utf-8-sig')
outfile_r_resumen = Path(r"D:\Docs\Práctica pipeline\data\clean\reviews_summary.csv")
df_r_resumen.to_csv(outfile_r_resumen, index=False, encoding='utf-8-sig')

import cleaning_steps_review_listings as csrl

df_r_s = pd.read_csv(r"D:\Docs\Práctica pipeline\data\clean\reviews_summary.csv", encoding='UTF-8')
logger_r_l = make_logger ("merge_review_listings", r"D:\Docs\Práctica pipeline\data\logs\merge_review_listings.log")

df_r_l = csrl.merge_listings_review(logger=logger_r_l)
df_r_l = (
    df_r_l
    .pipe(csrl.drop_nulls, logger=logger_r_l) 
    .pipe(csrl.change_name, logger=logger_r_l)
)

outfile_r_l = Path(r"D:\Docs\Práctica pipeline\data\clean\reviews_+_listings.csv")
df_r_l.to_csv(outfile_r_l, index=False, encoding='utf-8-sig')

df_n = pd.read_csv(r"D:\Docs\Práctica pipeline\data\raw\airbnb\neighbourhoods.csv")
logger_n = make_logger('cleaning_neighbourhoods', r'D:\Docs\Práctica pipeline\data\logs\cleaning_neighbourhoods.log')

df_n = (
  df_n
  .pipe(csn.drop_column, logger=logger_n)
  .pipe(csn.clean_names, logger=logger_n)
)

outfile_c_n = Path(r"D:\Docs\Práctica pipeline\data\clean\cleaned_neighbourhoods.csv")
df_n.to_csv(outfile_c_n, index=False, encoding='utf-8-sig')


gdf_n: gpd.GeoDataFrame = gpd.read_file(r"D:\Docs\Práctica pipeline\data\raw\airbnb\neighbourhoods.geojson", encoding='UTF-8')
logger_gdf_n = make_logger('cleaning_geometry_neighbourhoods', r'D:\Docs\Práctica pipeline\data\logs\cleaning_geometry_neighbourhoods.log')

gdf_n = (
  gdf_n
  .pipe(csgn.drop_column, logger=logger_gdf_n)
  .pipe(csgn.clean_names, logger=logger_gdf_n)
)


outfile_gdf_n = Path(r"D:\Docs\Práctica pipeline\data\clean\cleaned_geometry_neighbourhoods.geojson")
gdf_n.to_file(outfile_gdf_n, driver="GeoJSON")   


