
duckdb

create or replace table h_rain as
    select
        strptime(Time, '%Y-%m-%d %H:%M:%S') as Time,
        #2 as Rain
    from '_hourly_2_daily_test/data/hRain_wide.parquet'
;

-- Calculate the hourly rainfall as daily rainfall (using sum aggregate)
create or replace table d_rain_sum as
    with tmp as (
        select
            *,
            -- This is where you can set in which hour the day starts
            date_trunc('day', Time - interval 1 hour) as Date  -- recommended
            -- time_bucket(interval 1 day, Time - interval 1 hour)::date as Date
        from h_rain
    )
    select
        Date,
        sum(Rain) as Agg_sum  -- This is where the aggregate function can be set up
    from tmp
    group by Date
    having count(Rain) / 24 >= 1  -- This is where you can set the prop value
    order by Date
;

create or replace table d_rain_agg as
    with d_series as (
        select generate_series::date as Date
        from generate_series(
            date '1979-04-26',  -- constant only here!
            date '2024-04-08',  -- constant only here!
            interval 1 day
        )
    )
    select
        d_series.Date,
        d_rain_sum.Agg_sum
    from d_series
    left join d_rain_sum on d_series.Date = d_rain_sum.Date
    order by d_series.Date
;

-- -- Export to the CSV file
-- copy d_rain_agg to '_hourly_2_daily_test/daily_rain_duckdb.csv';


-- Read the offset daily rainfall time series and fix it
create or replace table d_rain as
    select * replace((strptime(Date, '%Y-%m-%d') - interval 1 day)::date as Date)
    from '_hourly_2_daily_test/data/dRain_wide.parquet'
;
