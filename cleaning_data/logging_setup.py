import pandas as pd
import logging
from logging import FileHandler, Formatter
from copy import deepcopy
from pathlib import Path

def make_logger(name: str, path: str) -> logging.Logger:
    handler = FileHandler(path, encoding="utf-8")
    handler.setFormatter(Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
    handler.setLevel(logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(handler)
    return logger

def log_step(logger, df_before: pd.DataFrame,
             df_after:  pd.DataFrame,
             *,
             step_desc: str,
             extra: str = "") -> None:
                n_before = len(df_before)   # filas previas
                n_after  = len(df_after)    # filas posteriores
                delta    = n_after - n_before
                
                c_before = df_before.shape[1]
                c_after  = df_after.shape[1]
                delta_cols = c_after - c_before
                
                logger.info(
                    f"{step_desc:<60} | "
                    f"filas {n_before:>12} → {n_after:>12} (Δ={delta:+9}) |  "
                    f"cols {c_before:>7} → {c_after:>7} (Δ={delta_cols:+5}) | "
                    f"{extra}"
                )

        

        