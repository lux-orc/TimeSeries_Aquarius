
"""
This script just verifies that the daily rainfall time series retrieved from AQ should be
literally offset (backwards) by 1 day: i.e., say `ts` is the daily time series from AQ,
its real daily rainfall time series should be ts.index = ts.index - pd.Timedelta('1D')
"""


import datetime
from pathlib import Path

import duckdb
import polars as pl
import polars.selectors as cs
import _tools.fun_s_pl as fpl



p = Path('.')
pp = p / '_hourly_2_daily_test'
ppp = pp / 'data'


hwl = pl.read_parquet(ppp / 'hRain_wide.parquet')
hwl = hwl.with_columns(pl.col('Time').str.to_datetime('%Y-%m-%d %H:%M:%S'))

dwl_f = fpl.hourly_2_daily(hwl, agg=pl.sum).drop('Site')



# After the fix, their time sequence should be matched
dwl = pl.read_parquet(ppp / 'dRain_wide.parquet')
dwl = dwl.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
dwl_fixed = dwl.with_columns(pl.col('Date').sub(datetime.timedelta(days=1)).alias('Date'))



# # Export the calculated daily rainfall (from the hourly)
# dwl_f.write_csv(pp / 'daily_rain_pl.csv')




hts = (
    pl.read_parquet(ppp / 'hRain_wide.parquet')
    .with_columns(pl.col('Time').str.to_datetime('%Y-%m-%d %H:%M:%S'))
)

col_dt = hts.select(cs.temporal()).columns[0]
col_site = hts.select(pl.exclude(col_dt)).columns[0]

day_starts_at = 0
agg = 'sum'
prop = 1.

q_str = f"""
    with tmp as (
        select
            *,
            date_trunc('day', "{col_dt}" - interval {day_starts_at+1} hour) as Date
            -- time_bucket(
            --    interval 1 day, "{col_dt}" - interval {day_starts_at+1} hour
            -- )::date as Date
        from hts
    )
    select
        Date,
        -- This is where the aggregate function can be set up
        {agg}("{col_site}") as Agg_{agg}
    from tmp
    group by Date
    -- This is where you can set the prop value
    having count("{col_site}") / 24 >= {prop}
    order by Date
"""


print(fpl.cp(r := duckdb.sql(q_str), fg=33))





from _tools.fun_s_pl import na_ts_insert

def hourly_2_daily(
        hts: pl.DataFrame,
        day_starts_at: int = 0,
        agg: str = 'avg',
        prop: float = 1.,
    ) -> pl.DataFrame:
    """
    Aggregate the hourly time series to daily time series using customised function

    Parameters
    ----------
    hts : pl.DataFrame
        An hourly time series (for a single site)
    day_starts_at : int, optional, default=0
        What time (hour) a day starts - 0 o'clock by default.
        e.g., 9 means the output of daily time series by 9 o'clock!
    agg : str, optional, default='avg'
        Aggregate function (from DuckDB) - avg by default (`avg`)
    prop : float, optional, default=1
        The ratio of the available data (within a day range)

    Returns
    -------
    pl.DataFrame
        A daily time series (pl.DataFrame) with an extra column of site name

    Raises
    ------
    ValueError
        `day_starts_at` should be an integer between 0 and 23. Error raised otherwise.
    ValueError
        `prop` should be a float in [0, 1]. Error raised otherwise.
    """
    if not isinstance(day_starts_at, int) or day_starts_at < 0 or day_starts_at > 23:
        raise ValueError('`day_starts_at` must be an integer in [0, 23]!\n')
    if prop < 0 or prop > 1:
        raise ValueError('`prop` must be in [0, 1]!\n')
    col_dt = hts.select(cs.temporal()).columns[0]
    col_v = hts.select(cs.numeric()).columns[0]
    r = duckdb.sql(f"""
        with tmp as (
            select
                *,
                date_trunc('day', "{col_dt}" - interval {day_starts_at+1} hour) as Date
                -- time_bucket(
                --     interval 1 day, "{col_dt}" - interval {day_starts_at+1} hour
                -- )::date as Date
            from hts
        )
        select
            Date,
            {agg}("{col_v}") as Agg_{agg}
        from tmp
        group by Date
        having count("{col_v}") / 24 >= {prop}
        order by Date
    """)
    return r.pl().pipe(na_ts_insert).with_columns(pl.lit(col_v).alias('Site'))


# Run the above function

import time

n = 40

s = time.perf_counter()
for _ in range(n):
    # fpl.hourly_2_daily(hts, agg=pl.sum).drop('Site')  # Still run faster
    hourly_2_daily(hts, agg='sum').drop('Site')
print(time.perf_counter() - s)
