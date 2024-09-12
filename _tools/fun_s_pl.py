# -*- coding: utf-8 -*-
import datetime
import json
from functools import reduce
from typing import Any, Callable
from urllib import parse

import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs
import urllib3

# Some display settings for numpy Array, Pandas and Polars DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)
pl.Config.set_tbl_cols(-1)


def cp(s: Any = '', /, display: int = 0, fg: int = 39, bg: int = 48) -> str:
    """
    Return the string for color print in the (IPython) console

    Parameters
    ----------
    s : Any, default=''
    display (显示方式) : int, default=0
        - 0: 默认值
        - 1: 高亮
        - 2: 模糊效果
        - 3: 斜体
        - 4: 下划线
        - 5: 闪烁
        - 7: 反显
        - 8: 不显示（隐藏效果）
        - 9: 划掉字体
        - 22: 非粗体
        - 24: 非下划线
        - 25: 非闪烁
        - 27: 非反显
    fg (前景色) : int, default=39
        - 30: 黑色
        - 31: 红色
        - 32: 绿色
        - 33: 黄色
        - 34: 蓝色
        - 35: 洋红
        - 36: 青色
        - 37: 白色
        - 38: 删除效果并终止
    bg (背景色) : int, default=48
        - 40: 黑色
        - 41: 红色
        - 42: 绿色
        - 43: 黄色
        - 44: 蓝色
        - 45: 洋红
        - 46: 青色
        - 47: 白色

    Returns
    -------
    str
        A string for color print in the console

    Notes
    -----
    stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
    """
    return f'\033[{display};{fg};{bg}m{s}\033[0m'


def print_dict(d: dict, /) -> None:
    """
    Customised function for printing a dictionary nicely on the console

    Parameters
    ----------
    d : dict
        A python dictionary object.

    Returns
    -------
    None
    """
    for k, v in d.items():
        print(cp(cp(f'\n{k}: {type(v)}\n', fg=34, display=4), display=1) + cp(f'\n{v}\n'))


def is_numeric(x: Any, /) -> bool:
    """
    Is `x` numeric?

    Parameters
    ----------
    x : Any
        An object.

    Returns
    -------
    bool
        `True` (is numeric) or `False` (not numeric).
    """
    return isinstance(x, (int, float, complex)) and not isinstance(x, bool)


def _ts_valid_pd(ts: Any, /) -> 'str | None':
    """Validate the input time series: `None` returned as passed"""
    if not isinstance(ts, (pd.Series, pd.DataFrame)):
        return '`ts` must be either pandas.Series or pandas.DataFrame!'
    if not (
        all(isinstance(i, (datetime.datetime, datetime.date)) for i in ts.index)
        or pd.api.types.is_datetime64_any_dtype(ts.index)
    ):
        return f'Wrong dtype in the index: `{ts.index.dtype}` detected!'
    if not (ts.index.size == ts.index.unique().size):
        return '`ts.index` must be unique!'
    if not all(ts.index == ts.index.sort_values(ascending=True)):
        return '`ts.index` must be in chronicle order!'
    if isinstance(ts, pd.DataFrame):
        if ts.shape[1] < 1:
            return 'No column exists in the DataFrame `ts`!'
        df = ts.select_dtypes(include=np.number)
        if not (df.shape[1] == ts.shape[1]):
            return 'All columns in `ts` must be numeric!'
        return None
    if not pd.api.types.is_any_real_numeric_dtype(ts):
        return 'The Series must contain real numbers!'


def _ts_valid_pl(ts: Any, /) -> 'str | None':
    """Validate the input time series: `None` returned as valid"""
    if isinstance(ts, pl.DataFrame):
        if ts.width < 2:
            return '`ts` must have one datetime column and the rest of numeric column(s)!'
        if len(col_dt := ts.select(cs.temporal()).columns) != 1:
            return '`ts` must have one datetime column!'
        if ts[col_dt[0]].unique().len() != ts[col_dt[0]].len():
            return f'The values in the temporal column {col_dt} must be unique!'
        if not ts.sort(by=col_dt, descending=False).equals(ts):
            return f'Column {col_dt} must be sorted in chronicle order!'
        if ts.width != ts.select(cs.numeric()).width + 1:
            return f'Apart from column {col_dt}, the rest column(s) must be numeric!'
        return None
    return '`ts` must be a polars.DataFrame!'


def ts_step(ts: pl.DataFrame, minimum_time_step_in_second: int = 60) -> 'int | None':
    """
    Identify the temporal resolution (in seconds) for a time series

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars time series - 1st column as date/datetime, and other column(s) as numeric
    minimum_time_step_in_second : int, default=60
        The minimum threshold of the time step that can be identified.

    Raises
    ------
    TypeError
        When `_ts_valid_pd(ts)` returns a string.

    Returns
    -------
    int | None
        * **`-1`**: time series is not in a regular time step.
        * Any integer **above `0`**: time series is regular (step in secs).
        * **`None`**: contains no values or a single value.
    """
    if err_str := _ts_valid_pl(ts):
        raise TypeError(err_str)
    col_dt = ts.select(cs.temporal()).columns[0]
    col_v = ts.select(cs.numeric()).columns
    x = ts.fill_nan(None).filter(~pl.all_horizontal(pl.col(col_v).is_null()))
    if len(x) in {0, 1}:
        return None
    diff_in_second = x.select(pl.col(col_dt)).to_series().diff(1).dt.total_seconds()[1:]
    step_min = diff_in_second.filter(diff_in_second >= minimum_time_step_in_second).min()
    return int(step_min) if (diff_in_second % step_min == 0).all() else -1


def is_ts_daily(ts: pl.DataFrame, /) -> bool:
    """Check if a time series (in Polars DataFrame) is daily (day starts at 0 o'clock)"""
    if err_str := _ts_valid_pl(ts):
        raise TypeError(err_str)
    col_dt = ts.select(cs.temporal()).columns[0]
    if not pl.Date.is_(ts[col_dt].dtype):
        time_no_hms = all([
            ts[col_dt].dt.hour().eq(0).all(),
            ts[col_dt].dt.minute().eq(0).all(),
            ts[col_dt].dt.second().eq(0).all(),
        ])
        return (ts_step(ts) == 86400) and time_no_hms
    return True


def ts_pd2pl(ts: 'pd.Series | pd.DataFrame') -> pl.DataFrame:
    """Convert the timeseries from Pandas DataFrame to Polars DataFrame"""
    if (err_str := _ts_valid_pd(ts)) is None:
        print('TimeSeries: Pandas DataFrame -> Polars DataFrame!')
        ts_pl = pl.DataFrame(pd.DataFrame(ts).reset_index()).fill_nan(None)
        col_dt = ts_pl.select(cs.temporal()).columns[0]
        if is_ts_daily(ts_pl):
            ts_pl = ts_pl.with_columns(pl.col(col_dt).cast(pl.Date).alias(col_dt))
        return ts_pl.sort(col_dt)
    raise TypeError(err_str)


def ts_pl2pd(ts: pl.DataFrame) -> pd.DataFrame:
    """Convert the timeseries from Polars DataFrame to Pandas DataFrame"""
    if (err_str := _ts_valid_pl(ts)) is None:
        print('TimeSeries: Polars DataFrame -> Pandas DataFrame!')
        return ts.to_pandas().set_index(ts.select(cs.temporal()).columns[0])
    raise TypeError(err_str)


def na_ts_insert(ts: pl.DataFrame) -> pl.DataFrame:
    """
    Pad Null value into a Polars DataFrame of a valid time series

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars DataFrame - 1st column as date/datetime, and rest column(s) as numeric

    Returns
    -------
    pl.DataFrame
        The Null-padded DataFrame.

    Notes
    -----
        As for irregular time series, The empty-numeric-row-removed DataFrame returned.
    """
    col_dt = ts.select(cs.temporal()).columns[0]
    col_v = ts.select(cs.numeric()).columns
    r = ts.fill_nan(None).filter(~pl.all_horizontal(pl.col(col_v).is_null()))
    if (step := ts_step(ts)) in {-1, None}:
        return r
    return r.sort(col_dt).upsample(time_column=col_dt, every=f'{step}s')


def hourly_2_daily(
        hts: pl.DataFrame,
        day_starts_at: int = 0,
        agg: Callable = pl.mean,
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
    agg : Callable, optional, default=pl.mean
        Customised aggregation function (from Polars) - mean by default (`pl.mean`)
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
    r = (
        hts.lazy()
        .select([col_dt, col_v])
        .fill_nan(None)
        .select(
            pl.col(col_dt)
            .sub(datetime.timedelta(seconds=3600 * (1+day_starts_at)))
            .dt.date()
            .alias('Date'),
            pl.col(col_v),
        )
        .with_columns(pl.col(col_v).count().over('Date').truediv(24).alias('Prop'))
        .filter(pl.col('Prop').ge(prop))
        .drop_nulls(subset=col_v)
        .group_by('Date', maintain_order=True)
        .agg(agg(col_v).alias(f'Agg_{agg.__name__}'))
    )
    return r.collect().pipe(na_ts_insert).with_columns(pl.lit(col_v).alias('Site'))


def ts_info(ts: pl.DataFrame) -> 'pl.DataFrame | None':
    """
    Obtain the data availability of the input time series (Polars DataFrame)

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars input time series.

    Returns
    -------
    pl.DataFrame | None
        * Info on ['Site', 'Start', 'End', 'Length_yr', 'Completion_%'].
        * As for time series of the irregular time steps, 'Completion_%' is ignored.
        * `None` returned when there is no data in the input time series.
    """
    if (con := ts_step(ts)) is None:
        return None
    col_dt = ts.select(cs.temporal()).columns
    col_rest = ts.select(pl.exclude(col_dt)).columns
    col_rest_ = [f'{i}_' for i in col_rest]
    seconds_year = (days_year := 365.2422) * 24 * 3600
    info = (
        ts.lazy()
        .rename(dict(zip(col_rest, col_rest_)))
        .unpivot(on=col_rest_, index=col_dt, variable_name='Site', value_name='V')
        .filter(pl.col('V').fill_nan(None).is_not_null())
        .group_by('Site', maintain_order=True).agg(
            pl.col(col_dt).min().alias('Start'),
            pl.col(col_dt).max().alias('End'),
            pl.col('V').len().alias('n'),
        )
        .with_columns(
            pl.col('End')
            .sub(pl.col('Start'))
            .dt.total_seconds()
            .truediv(seconds_year)
            .alias('Length_yr')
        )
    )
    info = (
        pl.LazyFrame({'Site': col_rest_})
        .join(info, on='Site', how='left', coalesce=True)
        .with_columns(Site=pl.Series(col_rest))
    )
    if con == -1:
        return info.drop('n').collect()
    step_day = con / 86400
    return (
        info.with_columns(
            (pl.col('Length_yr') * days_year + step_day).alias('N'),
            (pl.col('Length_yr') + step_day / days_year),
        )
        .with_columns((pl.col('n') * step_day / pl.col('N') * 100).alias('Completion_%'))
        .drop(['n', 'N'])
        .collect()
    )


def get_AQ(
        url: str,
        basic_auth: str = 'api-read:PR98U3SKOczINoPHo7WM',
        **kwargs
    ) -> urllib3.response.HTTPResponse:
    """Connect ORC's AQ using 'GET' verb"""
    http = urllib3.PoolManager()
    hdr = urllib3.util.make_headers(basic_auth=basic_auth)
    return http.request('GET', url=url, headers=hdr, **kwargs)


def get_uid(measurement: str, site: str) -> 'str | None':
    """
    Get UniqueId <- f'{measurement}@{site}'

    Parameters
    ----------
    measurement : str
        The format of {Parameter}.{Label}, such as:
            * Flow.WMHourlyMean
            * Discharge.MasterDailyMean
    site : str
        The {LocationIdentifier} behind a site name, such as:
            * WM0062
            * FA780

    Returns
    -------
    str | None
        * str: UniqueId str used for requesting time series (Aquarius)
        * `None`: the UniqueId cannot be located
    """
    if not site.strip():
        raise ValueError("Provide a correct string value for 'Site'!\n")
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url_desc = f'{end_point}/GetTimeSeriesDescriptionList'
    ms = f'{measurement}@{site}'
    parameter, _ = measurement.split('.')
    query_dict = {'LocationIdentifier': site, 'Parameter': parameter}
    r = get_AQ(url=url_desc, fields=query_dict)
    if not (ld := json.loads(r.data.decode('utf-8')).get('TimeSeriesDescriptions')):
        return None
    j_list = [i for i, v in enumerate(ld) if v['Identifier'] == ms]
    return ld[j_list[0]].get('UniqueId', None) if j_list else None


def get_url_AQ(
        measurement: str,
        site: str,
        date_start: int = None,
        date_end: int = None
    ) -> 'str | None':
    """
    Generate the url for requesting time series

    Parameters
    ----------
    measurement : str
        The format of {Parameter}.{Label}, such as:
            * Flow.WMHourlyMean
            * Discharge.MasterDailyMean
    site : str
        The {LocationIdentifier} behind a site name, such as:
            * WM0062
            * FA780
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.

    Returns
    -------
    str | None
        A string of the url for requesting time series.
    """
    if (uid := get_uid(measurement, site)) is None:
        return None
    fmt = '%Y-%m-%dT00:00:00.0000000+12:00'
    ds = '1800-01-01T00:00:00.0000000+12:00' if date_start is None else (
        datetime.datetime.strptime(f'{date_start}', '%Y%m%d').strftime(fmt))
    de = (
        datetime.datetime.now() + datetime.timedelta(days=1) if date_end is None else
        datetime.datetime.strptime(f'{date_end}', '%Y%m%d') + datetime.timedelta(days=1)
    ).strftime(fmt)
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    query_dict = {
        'TimeSeriesUniqueId': uid,
        'QueryFrom': ds,
        'QueryTo': de,
        'GetParts': 'PointsOnly',
    }
    q_str = parse.urlencode(query_dict)
    return f'{end_point}/GetTimeSeriesCorrectedData?{q_str}'


def get_ts_AQ(
        measurement: str,
        site: str,
        date_start: int = None,
        date_end: int = None
    ) -> pl.DataFrame:
    """Get the time series for a single site specified by those defined in `get_url_AQ`"""
    empty_df = pl.DataFrame(schema={'Timestamp': str, 'Value': float})
    if (url := get_url_AQ(measurement, site, date_start, date_end)) is None:
        print(f'\n[{measurement}@{site}] -> No data! An empty column [{site}] added!\n')
        return empty_df
    r = get_AQ(url=url)
    if not (ld := json.loads(r.data.decode('utf-8')).get('Points', None)):
        print(f'[{measurement}@{site}] -> No data over the chosen period!\n')
        return empty_df
    return pl.DataFrame(ld).unnest('Value').rename({'Numeric': 'Value'}).with_columns([
        pl.col('Timestamp').cast(str),
        pl.col('Value').cast(float),
    ])


def clean_24h_datetime(shit_datetime: str) -> str:
    """
    Clean the 24:00:00 in a datetime string to a normal datetime string

    Parameters
    ----------
    shit_datetime : str
        The first 19 characters for the input follow a format of '%Y-%m-%dT%H:%M:%S',
        and it is supposed to have shit (24:MM:SS) itself, like '2020-12-31T24:00:00'

    Returns
    -------
    str (length of 19)
        A normal datetime string:
            such as '2021-01-01T00:00:00' converted from the shit one mentioned above.
    """
    if not isinstance(shit_datetime, str):
        return None
    date_str, time_str = (s19 := shit_datetime[:19]).split('T')
    *Ymd, H, M, S = [int(i) for i in (date_str.split('-') + time_str.split(':'))]
    return (
        datetime.datetime(*Ymd, H-1, M, S)
        + datetime.timedelta(hours=1)
    ).strftime('%Y-%m-%dT%H:%M:%S') if H > 23 else s19


def _HWU_AQ(
        site: str,
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False
    ) -> pl.DataFrame:
    """Get hourly rate for a single water meter (from Aquarius)"""
    ts_raw = get_ts_AQ('Flow.WMHourlyMean', site, date_start, date_end)
    if raw_data:
        return ts_raw
    return ts_raw.select(
        pl.col('Timestamp')
        .map_elements(clean_24h_datetime, return_dtype=pl.Utf8)
        .str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S')
        .alias('Time'),
        pl.col('Value').truediv(1e3).alias(site),
    ).pipe(na_ts_insert)


def _DWU_AQ(
        site: str,
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False
    ) -> pl.DataFrame:
    """Get daily rate for a single water meter (from Aquarius)"""
    ts_raw = get_ts_AQ('Abstraction Volume.WMDaily', site, date_start, date_end)
    if raw_data:
        return ts_raw
    return ts_raw.select(
        pl.col('Timestamp')
        .map_elements(clean_24h_datetime, return_dtype=pl.Utf8)
        .str.slice(0, 10)
        .str.strptime(pl.Date, '%Y-%m-%d')
        .alias('Date'),
        pl.col('Value').truediv(86400).alias(site),
    ).pipe(na_ts_insert)


def hourly_WU_AQ(
        siteList: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
    ) -> pl.DataFrame:
    """
    A wrapper of getting hourly rate for multiple water meters (from Aquarius)

    Parameters
    ----------
    siteList : str | list[str]
        A list of water meters' names
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.
    raw_data : bool, optional, default=False
        Raw data (hourly volume in m^3) from Aquarius (extra info). Default is `False`

    Returns
    -------
    pl.DataFrame
        A DataFrame of hourly abstraction
    """
    if isinstance(siteList, str):
        siteList = [siteList]
    siteList = list(dict.fromkeys(siteList))
    if raw_data:
        d = {
            site: _HWU_AQ(site, date_start, date_end, True).with_columns(
                pl.lit(site).alias('Site')
            ) for site in siteList
        }
        return pl.concat(d.values(), how='vertical').select('Site', 'Timestamp', 'Value')
    lst = [_HWU_AQ(site, date_start, date_end, False) for site in siteList]
    return na_ts_insert(
        reduce(lambda a, b: a.join(b, on='Time', how='full', coalesce=True), lst)
        .sort('Time')
    )


def daily_WU_AQ(
        siteList: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
    ) -> pl.DataFrame:
    """
    A wrapper of getting daily rate for multiple water meters (from Aquarius)

    Parameters
    ----------
    siteList : str | list[str]
        A list of water meters' names
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.
    raw_data : bool, optional, default=False
        Raw data (daily volume in m^3) from Aquarius (extra info). Default is `False`

    Returns
    -------
    pl.DataFrame
        A DataFrame of daily abstraction
    """
    if isinstance(siteList, str):
        siteList = [siteList]
    siteList = list(dict.fromkeys(siteList))
    if raw_data:
        d = {
            site: _DWU_AQ(site, date_start, date_end, True).with_columns(
                pl.lit(site).alias('Site')
            ) for site in siteList
        }
        return pl.concat(d.values(), how='vertical').select('Site', 'Timestamp', 'Value')
    lst = [_DWU_AQ(site, date_start, date_end, False) for site in siteList]
    return na_ts_insert(
        reduce(lambda a, b: a.join(b, on='Date', how='full', coalesce=True), lst)
        .sort('Date')
    )
