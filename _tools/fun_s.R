
# https://adv-r.hadley.nz/functions.html
# https://r-pkgs.org/index.html

library(data.table)  # Fast operations on large data frames
library(httr)  # Useful tools for working with HTTP organised by HTTP verbs

Sys.setenv(TZ = "Etc/GMT-12")  # Sys.unsetenv("TZ"); Sys.timezone()


path_relative <- function(path, to) {
  return(sub(path, replacement = "", pattern = file.path(to, "/"), fixed = TRUE))
}


ok_comma <- function(FUN) {
  # Keep the last comma for `c()`, `list()`, `data.frame()` -> use it in caution!
  #
  # Notes:
  #   https://www.r-bloggers.com/2013/03/r-and-the-last-comma/
  function(...) {
    arg_list <- as.list(sys.call())[-1L]
    len <- length(arg_list)
    if (len > 1L) {
      last <- arg_list[[len]]
      if (missing(last))
        arg_list <- arg_list[-len]
    }
    do.call(FUN, arg_list)
  }
}


cp <- function(s = "", display = 0, fg = 39, bg = 48) {
  # Colour print in the (RStudio) console for an object.
  #
  # Args:
  #   s: An object, default is an empty character ("").
  #   display, default = 0:
  #     - 0: default
  #     - 1: highlight
  #     - 2: blur
  #     - 3: italic
  #     - 4: underline
  #     - 5: flash?
  #     - 7: inverse?
  #     - 8: hide
  #     - 9: strikethrough
  #     - 22: non-bold
  #     - 24: non-underline
  #     - 25: non-flash?
  #     - 27: non-inverse?
  #   fg (foreground), default = 39:
  #     - 30: black
  #     - 31: red
  #     - 32: green
  #     - 33: yellow
  #     - 34: blue
  #     - 35: magenta
  #     - 36: cyan
  #     - 37: white
  #     - 38: delete and pause
  #   bg (background), default = 48:
  #     - 40: black
  #     - 41: red
  #     - 42: green
  #     - 43: yellow
  #     - 44: blue
  #     - 45: magenta
  #     - 46: cyan
  #     - 47: white
  #
  # Return:
  #   A string for color print in the console.
  #
  # Notes:
  #   stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
  x <- paste(utils::capture.output(s), "\n", sep = "")
  x[1] <- paste0(" ", x[1])
  return(paste0("\033[", display, ";", fg, ";", bg, "m", x, "\033[0m"))
}


xts_2_dt <- function(xts_obj) {
  # Convert xts object to a data.table object
  res_dt <- as.data.table(xts_obj, keep.rownames = TRUE)
  colnames(res_dt)[1] <- if (!ts_step(res_dt)) "Date" else "Time"
  return(res_dt)
}


.as_df <- function(DF) {
  # DataFrame-related function (output) from the type (dt/df/tbl) of the input DF.
  if (data.table::is.data.table(DF) || xts::is.xts(DF)) {
    return(data.table::as.data.table)
  } else if (tibble::is_tibble(DF)) {
    return(tibble::as_tibble)
  } else {
    return(as.data.frame)
  }
}


.trim_ts <- function(TS) {
  # Remove the complete empty rows for a time series (TS) data.frame.
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   A time series (TS) data.frame with all completely empty rows removed.
  if (xts::is.xts(TS)) TS <- xts_2_dt(TS)
  n <- dim(TS)[2] - 1
  rm_L <- rowSums(is.na(TS[, -1, drop = FALSE])) == n
  ts_rmd <- if (length(which(rm_L))) TS[-which(rm_L), ] else TS
  res <- as.data.frame(ts_rmd)
  row.names(res) <- NULL
  return(.as_df(TS)(res))
}


ts_step <- function(TS, minimum_time_step_in_second = 60L) {
  # Identify the time step for a time series (can be multiple columns in a wide format)
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #   minimum_time_step_in_second: The threshold for checking time step, default is 60 secs.
  #
  # Returns:
  #   0: daily time step (days start from zero o'clock),
  #   -1: time series is not in a regular time step,
  #   any integer greater than 0: time series is in a regular time step (in secs),
  #   NULL: time series doesn't contain any values (i.e., empty).
  t1 <- .trim_ts(TS)[[1]]
  if (any(c(0, 1) %in% length(t1)))
    return(message("Not enough data to determine steps -> `NULL` returned!"))
  if (is(t1, "Date")) {
    return(0L)
  } else {
    t2 <- as.numeric(difftime(t1[-1], t1[-length(t1)], units = "s"))
    t3 <- t2[t2 >= minimum_time_step_in_second]
    step_minimum <- min(t3)
    if (all(t3 %% step_minimum == 0)) return(step_minimum) else return(-1L)
  }
}


na_ts_insert <- function(TS) {
  # Padding the input time series (TS) by NA.
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   Padded time series for regular time-step TS;
  #   empty-row-removed time series for irregular time-step TS.
  if (xts::is.xts(TS)) TS <- xts_2_dt(TS)
  x <- .trim_ts(data.table(TS))
  if (dim(x)[1]) {
    con <- ts_step(x)
    rng <- range(x[[1]], na.rm = TRUE)
    time_df <-
      if (con == 0) {
        data.table(seq.Date(from = rng[1], to = rng[2], by = 1))
      } else if (con > 0) {
        data.table(seq.POSIXt(from = rng[1], to = rng[2], by = con))
      } else {
        data.table(x[[1]])
      }
    names(time_df) <- names(x)[1]
    res <- merge.data.table(time_df, x, by = names(x)[1], all.x = TRUE)
  } else {
    res <- x
  }
  return(.as_df(TS)(res))
}


hourly_2_daily <- function(hts, day_starts_at = 0L, agg = mean, prop = 1.) {
  # Aggregate the hourly time series to daily time series using customised function.
  #
  # Args:
  #   hts: An hourly time series (for a single site).
  #   day_starts_at: What time (hour) a day starts - 0 o'clock by default.
  #     e.g., 9L means the output of daily time series by 9 o'clock!
  #   agg: Customised aggregation function - mean by default.
  #   prop: The ratio of the available data (within a day range).
  #
  # Returns:
  #   A frame of daily time series with an extra column of site name.
  is_wholenumber <- function(x, tol = sqrt(.Machine$double.eps)) abs(x - round(x)) < tol
  if (!is_wholenumber(day_starts_at) || day_starts_at < 0L || day_starts_at > 23L)
    stop("`day_starts_at` must be an integer in [0L, 23L]!\n")
  if (prop < 0 || prop > 1)
    stop("`prop` must be in [0, 1]!\n")
  hts_c <- as.data.table(na_ts_insert(hts))
  site_name <- names(hts_c)[2]
  setnames(hts_c, old = names(hts_c), new = c("Time", "Value"))
  tmp <- hts_c[, {
    Time_ <- Time - 3600 * (1L + day_starts_at)
    .(Date = as.Date(Time_, tz = "Etc/GMT-12"), Value)
  }]
  tmp[, Prop := sum(!is.na(Value)) / 24, Date]
  r <- tmp[Prop >= prop, .(Agg = agg(Value, na.rm = TRUE)), Date]
  x <- if (any(c(0, 1) %in% dim(r)[1])) r else na_ts_insert(r)
  x[, Site := site_name]
  setnames(x, old = "Agg", new = paste("Agg", deparse(substitute(agg)), sep = "_"))
  return(.as_df(hts)(x))
}


ts_info <- function(TS) {
  # Data dictionary for the input time series (TS).
  #
  # Args:
  #   TS: Single/multiple time series (regular or irregular time step).
  #
  # Returns:
  #   A data.frame on data availability and completion in percentages for each site.
  ts_w <- if (xts::is.xts(TS)) xts_2_dt(TS) else as.data.table(TS)
  names(ts_w)[1] <- "Time"
  con <- ts_step(ts_w)
  if (is.null(con)) return(NULL)
  empty_df <- data.table(Site = names(TS)[-1L])
  ts_l <- melt.data.table(ts_w, id.vars = "Time", variable.name = "Site", value.name = "V")
  info_df <- na.omit(ts_l)[, .(Start = min(Time), End = max(Time), n = .N), Site]
  d_yr <- 365.2422
  info_df[, Length_yr := as.numeric(difftime(End, Start, units = "d")) / d_yr]
  info_df <- merge.data.table(empty_df, info_df, by = "Site", all.x = TRUE, sort = FALSE)
  if (con == -1L) return(.as_df(TS)(info_df[, -"n"]))
  step_day <- if (con == 0L) 1L else con / (3600 * 24)
  info_df[, `:=`(N = Length_yr * d_yr + step_day, Length_yr = Length_yr + step_day / d_yr)]
  info_df[, `:=`(Completion = n * step_day / N * 100, n = NULL, N = NULL)]
  setnames(info_df, old = "Completion", new = "Completion_%")
  return(.as_df(TS)(info_df[]))
}


ts_2_list <- function(TS) {
  # Transform the time series (wide format) to a list of time series (each site).
  #
  # Args:
  #   TS: A time series (this can be time series of both regular or irregular time step).
  #
  # Returns:
  #   A list split by columns in the input TS.
  if (xts::is.xts(TS)) TS <- xts_2_dt(TS)
  x <- as.data.frame(TS)
  L <- vector(mode = "list", length = dim(x)[2] - 1L)
  for (i in seq_len(length(L)))
    L[[i]] <- .as_df(TS)(na_ts_insert(x[, c(1, i + 1)]))
  return(`names<-`(L, names(TS)[-1]))
}


is_datetime <- function(x) {
  # Checks if an object is a date/datetime class
  #
  # Args:
  #   x: An object.
  #
  # Returns:
  #   A single logical value.
  return(all(class(x) %in% c("Date", "POSIXct", "POSIXt")))
}


ts_melt <- function(TS, value_name = "Value") {
  # Get the long format of the time series.
  #
  # Args:
  #   TS: A time series (this can be time series of both regular or irregular time step)
  #   value_name: The name of the numeric column in the output data.frame
  #
  # Returns:
  #   A long format of the time series.
  con <- ts_step(TS)
  if (is.null(con)) return(NULL)
  x <- as.data.table(na_ts_insert(TS))
  dt_name <- names(x[, .SD, .SDcols = is_datetime])
  y <- NULL
  for (ts_site in ts_2_list(x)) {
    site <- names(ts_site)[2]
    ts_site_long <- data.table(
      Site = site,
      To_be_replaced = ts_site[[dt_name]],
      Value = ts_site[[site]]
    )
    setnames(ts_site_long, old = "Value", new = value_name)
    y <- rbind(y, ts_site_long)
  }
  setnames(y, old = "To_be_replaced", new = dt_name)
  return(.as_df(TS)(y))
}


get_AQ <- function(url, ...) {
  # Connect ORC's AQ using 'GET' verb
  return(GET(url, authenticate("api-read", "PR98U3SKOczINoPHo7WM"), ...))
}


get_uid <- function(measurement, site) {
  # Get UniqueId from paste0(measurement, "@", "site").
  #
  # Args:
  #   measurement: char
  #     The format of {Parameter}.{Label}, such as:
  #       * Flow.WMHourlyMean
  #       * Discharge.MasterDailyMean
  #   site: char
  #     The {LocationIdentifier} behind a site name, such as:
  #       * WM0062
  #       * FA780
  #
  # Returns:
  # str | NULL
  #   * str: UniqueId str used for requesting time series (Aquarius)
  #   * `None`: the UniqueId cannot be located
  if (trimws(site) == "")
    stop("Provide a correct string value for 'Site'!", call. = FALSE)
  ts_id_site <- paste0(measurement, "@", site)
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  r <- get_AQ(
    paste0(end_point, "/GetTimeSeriesDescriptionList"),
    query = list(LocationIdentifier = site)
  )
  ts_desc <- content(r)$TimeSeriesDescriptions
  if (is.null(ts_desc)) return(NULL)
  chk <- data.table(
    Identifier = sapply(ts_desc, "[[", 1),
    UniqueId = sapply(ts_desc, "[[", 2)
  )
  uid <- chk[Identifier == ts_id_site, UniqueId]
  return(if (length(uid)) uid else NULL)
}


get_url_AQ <- function(measurement, site, date_start = NA, date_end = NA) {
  # Generate a URL for requesting time series (from Aquarius).
  #
  # Args:
  #   measurement: char
  #     The format of {Parameter}.{Label}, such as:
  #       * Flow.WMHourlyMean
  #       * Discharge.MasterDailyMean
  #   site: char
  #     The {LocationIdentifier} behind a site name, such as:
  #       * WM0062
  #       * FA780
  #   date_start: int, optional (default as NA)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning.
  #   date_end: int, optional (default as NA)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end (4 days from current date on).
  #   raw_data: bool, optional
  #     Raw data (hourly volume in m^3) from Aquarius (extra info). Default is `FALSE`
  #
  # Returns:
  #   A string of a URL
  uid <- get_uid(measurement, site)
  if (is.null(uid)) return(NULL)
  str_ex <- "T00:00:00.0000000+12:00"
  dt_s <- if (is.na(date_start)) paste0("1800-01-01", str_ex) else (
    paste0(as.Date(as.character(date_start), format = "%Y%m%d"), str_ex)
  )
  dt_e <- if (is.na(date_end)) format(Sys.Date() + 1, paste0("%Y-%m-%d", str_ex)) else (
    paste0(as.Date(as.character(date_end), format = "%Y%m%d") + 1, str_ex)
  )
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  url_part <- parse_url(paste0(end_point, "/GetTimeSeriesCorrectedData"))
  url_part$query <- list(
    TimeSeriesUniqueId = uid,
    QueryFrom = dt_s,
    QueryTo = dt_e,
    GetParts = "PointsOnly"
  )
  return(build_url(url_part))
}


get_ts_AQ <- function(measurement, site, date_start = NA, date_end = NA) {
  # Get the time series for a single site specified by those defined in `get_url_AQ`
  url_c <- get_url_AQ(measurement, site, date_start = NA, date_end = NA)
  if (is.null(url_c)) {
    cat(
      "\n[", measurement, "@", site, "] -> No data! An empty column [",
      site, "] added!\n", sep = ""
    )
    return(data.table(Timestamp = character(), Value = numeric()))
  }
  r <- get_AQ(url_c)
  point_list <- content(r)$Points
  if (length(point_list)) {
    ts_raw <- rbindlist(point_list)
    ts_raw[, Value := unlist(Value, use.names = FALSE)]
  } else {
    ts_raw <- data.table(Timestamp = character(), Value = numeric())
  }
  return(ts_raw[])
}


.clean_24h_datetime <- function(shit_datetime) {
  # Clean the 24:00:00 in a datetime string to a normal datetime string.
  #
  # Args:
  #   shit_datetime: char
  #     The first 19 characters for the input follow a format of "%Y-%m-%dT%H:%M:%S",
  #     and it is supposed to have shit (24:MM:SS) itself, like "2020-12-31T24:00:00"
  #
  # Returns:
  #   A normal datetime string:
  #     such as '2021-01-01T00:00:00' converted from the shit one presented above.
  #
  # Example:
  #   > require(data.table)
  #   > bad_dt_str <- "2020-12-31T24:00:00"
  #   > .clean_24h_datetime(bad_dt_str)
  #   [1] "2021-01-01T00:00:00"
  if (length(shit_datetime) > 1L)
    stop("This function accepts a single string ONLY!", call. = FALSE)
  if (is.na(shit_datetime))
    return(as.character(NA))
  str_19 <- substr(shit_datetime, 1L, 19L)
  dt_list <- tstrsplit(str_19, split = "T")
  date_str <- dt_list[[1]]
  time_str <- dt_list[[2]]
  h_ms <- tstrsplit(time_str, split = ":")
  h <- as.integer(h_ms[[1]])
  if (h == 24L) {
    m <- as.character(h_ms[[2]])
    s <- as.character(h_ms[[3]])
    fmt <- "%Y-%m-%dT%H:%M:%S"
    time_str_new <- paste(h - 1L, m, s, sep = ":")
    date_str_new <- paste(date_str, time_str_new, sep = "T")
    return(format(as.POSIXct(date_str_new, tz = "UTC", format = fmt) + 3600L, fmt))
  }
  return(str_19)
}
clean_24h_datetime <- Vectorize(.clean_24h_datetime, SIMPLIFY = TRUE, USE.NAMES = FALSE)


.HWU_AQ <- function(site, date_start = NA, date_end = NA, raw_data = FALSE) {
  # Getting hourly rate for a single water meter (from Aquarius)
  ts_raw <- get_ts_AQ("Flow.WMHourlyMean", site, date_start, date_end)
  if (raw_data) return(ts_raw[])
  if (ts_raw[, .N]) {
    ts_dt <- ts_raw[, .(
      Time = as.POSIXct(
        clean_24h_datetime(Timestamp),
        format = "%Y-%m-%dT%H:%M:%S",
        tz = "Etc/GMT-12"),
      V = Value / 1e3)]
    setnames(ts_dt, "V", site)
    return(na_ts_insert(ts_dt))
  }
  empty_dt <- data.table(
    Time = as.POSIXct(as.character(), tz = "Etc/GMT-12"),
    V = as.numeric()
  )
  setnames(empty_dt, old = "V", new = site)
  return(empty_dt[])
}


.DWU_AQ <- function(site, date_start = NA, date_end = NA, raw_data = FALSE) {
  # Getting daily rate for a single water meter (from Aquarius)
  ts_raw <- get_ts_AQ("Abstraction Volume.WMDaily", site, date_start, date_end)
  if (raw_data) return(ts_raw[])
  if (ts_raw[, .N]) {
    ts_dt <- ts_raw[, .(
      Date = as.Date(clean_24h_datetime(Timestamp), format = "%Y-%m-%d"),
      V = Value / (24 * 60 * 60))]
    setnames(ts_dt, "V", site)
    return(na_ts_insert(ts_dt))
  }
  empty_dt <- data.table(Date = as.Date(as.character()), V = as.numeric())
  setnames(empty_dt, old = "V", new = site)
  return(empty_dt[])
}


hourly_WU_AQ <- function(siteList, date_start = NA, date_end = NA, raw_data = FALSE) {
  # A wrapper of getting hourly rate for multiple water meters (from Aquarius).
  #
  # Args:
  #   siteList: char. A list of water meters' names.
  #   date_start: int, optional (default as NA)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end: int, optional (default as NA)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end (4 days from current date on).
  #   raw_data: bool, optional
  #     Raw data (hourly volume in m^3) from Aquarius (extra info). Default is `FALSE`
  #
  # Returns:
  #   A data.table of hourly abstraction
  siteList <- unique(siteList)
  if (raw_data) {
    ts_raw_list <- lapply(
      siteList, .HWU_AQ, date_start = date_start, date_end = date_end, raw_data = TRUE)
    names(ts_raw_list) <- siteList
    ts_raw <- rbindlist(ts_raw_list, idcol = TRUE)
    setnames(ts_raw, ".id", "Site")
    return(ts_raw[])
  }
  ts_list <- lapply(siteList, .HWU_AQ, date_start = date_start, date_end = date_end)
  ts_dt <- Reduce(function(a, b) merge.data.table(a, b, by = "Time", all = TRUE), ts_list)
  return(na_ts_insert(ts_dt))
}


daily_WU_AQ <- function(siteList, date_start = NA, date_end = NA, raw_data = FALSE) {
  # A wrapper of getting daily rate for multiple water meters (from Aquarius).
  #
  # Args:
  #   siteList: char. A list of water meters' names.
  #   date_start: int, optional (default as NA)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end: int, optional (default as NA)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end.
  #   raw_data: bool, optional
  #     Raw data (daily volume in m^3) from Aquarius (extra info). Default is `FALSE`
  #
  # Returns:
  #   A data.table of daily abstraction
  siteList <- unique(siteList)
  if (raw_data) {
    ts_raw_list <- lapply(
      siteList, .DWU_AQ, date_start = date_start, date_end = date_end, raw_data = TRUE)
    names(ts_raw_list) <- siteList
    ts_raw <- rbindlist(ts_raw_list, idcol = TRUE)
    setnames(ts_raw, ".id", "Site")
    return(ts_raw[])
  }
  ts_list <- lapply(siteList, .DWU_AQ, date_start = date_start, date_end = date_end)
  ts_dt <- Reduce(function(a, b) merge.data.table(a, b, by = "Date", all = TRUE), ts_list)
  return(na_ts_insert(ts_dt))
}
