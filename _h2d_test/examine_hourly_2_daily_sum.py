
"""
This script just verifies that the daily rainfall time series retrieved from AQ should be
literally offset (backwards) by 1 day: i.e., say `ts` is the daily time series from AQ,
its real daily rainfall time series should be ts.index = ts.index - pd.Timedelta('1D')
"""


import datetime
from pathlib import Path

import polars as pl
import _tools.fun_s_pl as fpl



p = Path('.')
pp = p / '_h2d_test'
ppp = pp / 'data'


hwl = pl.read_parquet(ppp / 'hRain_wide.parquet')
hwl = hwl.with_columns(pl.col('Time').str.to_datetime('%Y-%m-%d %H:%M:%S'))

dwl = pl.read_parquet(ppp / 'dRain_wide.parquet')
dwl = dwl.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))



# After the fix, their time sequence should be matched
dwl_fixed = dwl.with_columns(pl.col('Date').sub(datetime.timedelta(days=1)).alias('Date'))
dwl_f = fpl.hourly_2_daily(hwl, agg=pl.sum).drop('Site')