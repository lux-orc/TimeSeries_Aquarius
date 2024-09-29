
rm(list = ls(all.names = TRUE))

source("_tools/fun_s.R")

fi <- "_h2d_test/data/hRain_wide.parquet"
h <- arrow::read_parquet(fi)

h$Time <- as.POSIXct(h$Time, tz = "Etc/GMT-12", format = "%Y-%m-%d %H:%M:%S")

d <- hourly_2_daily(h, agg = sum)

# Load the daily time series from AQ
d_AQ <- arrow::read_parquet("_h2d_test/data/dRain_wide.parquet")
d_AQ$Date <- as.Date(d_AQ$Date, format = "%Y-%m-%d")
