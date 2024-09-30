
rm(list = ls(all.names = TRUE))

require(data.table)
require(duckdb)

source("_tools/fun_s.R")

fi <- "_hourly_2_daily_test/data/hRain_wide.parquet"
h <- arrow::read_parquet(fi)
setDT(h)

h[, Time := as.POSIXct(Time, tz = "Etc/GMT-12", format = "%Y-%m-%d %H:%M:%S")]
d <- hourly_2_daily(h, agg = sum)

# Register the dataframe (using duckdb)
con <- dbConnect(duckdb())
duckdb_register(con, "h", h)

q_str = r"(
    with tmp as (
        select
            *,
            time_bucket(interval 1 day, "Time" - interval 1 hour)::date as Date
        from h
    )
    select
        Date,
        -- This is where the aggregate function can be set up
        sum("Leith at Pinehill") as Agg_sum
    from tmp
    group by Date
    -- This is where you can set the prop value
    having count("Leith at Pinehill") / 24 >= 1
    order by Date
)"

d_duckdb <- dbGetQuery(con, q_str)
setDT(d_duckdb)


# Load the daily time series from AQ
d_AQ <- arrow::read_parquet("_hourly_2_daily_test/data/dRain_wide.parquet")
setDT(d_AQ)
d_AQ[, Date := as.Date(Date, format = "%Y-%m-%d")]

