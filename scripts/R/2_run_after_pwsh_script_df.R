
rm(list = ls(all.names = TRUE))

source("_tools/fun_s.R")

time_start <- Sys.time()  # Start the timer

# Set up the path of the project folder
path <- getwd()
path_out <- file.path(path, "out")
path_csv <- file.path(path_out, "csv")

# Check if folder <out/csv> exists, raise otherwise
if (!dir.exists(path_csv))
  stop(paste0("\n\tFolder <", path_relative(path_csv, path), "> doesn't exist!"))

# Load the reference list between the plate numbers and the site names
path_info <- file.path(path, "info")
plate_info <- read.csv(file.path(path_info, "plate_info.csv"))
param_info <- read.csv(file.path(path_info, "param_info.csv"))


# Detect the folders in '/out/csv' folder
path_folders <- list.dirs(path_csv, recursive = FALSE, full.names = TRUE)


# For each folder, read the csv data files
for (path_folder in path_folders) {
  csv_paths <- list.files(path_folder, recursive = FALSE, full.names = TRUE)
  csv_names <- list.files(path_folder, recursive = FALSE, full.names = FALSE)
  folder_name <- sub(pattern = paste0(path_csv, "/"), replacement = "", x = path_folder)
  pr <- path_relative(path_folder, path)
  if (!length(csv_names)) {
    message("\nNo CSV files in folder <", pr, ">\n", sep = "")
    next
  }
  ts_df <- NULL
  for (csv_path in csv_paths) {
    tmp <-
      read.csv(csv_path, header = FALSE, nrows = 1, skip = 7)[1, 1] |>
      strsplit(": ", fixed = TRUE) |>
      _[[1]][1] |>
      sub(replacement = "", pattern = "# ", fixed = TRUE)
    tmp_1 <- strsplit(tmp, "@", fixed = TRUE)[[1]]
    plate <- rev(tmp_1)[1]
    tmp_2 <- strsplit(paste(tmp_1[tmp_1 != plate], collapse = "@"), ".", fixed = TRUE)[[1]]
    lab <- rev(tmp_2)[1]
    tmp_3 <- strsplit(tmp_2[1], " ", fixed = TRUE)[[1]]
    param <- paste(tmp_3[-1], collapse = " ")
    ts_i <- read.csv(csv_path, skip = 11, colClasses = c("character", "double"))
    names(ts_i)[2] <- "Value"
    ts_i <-
      ts_i |>
      transform(
        Unit = param_info[which(param_info$Param == param), "Unit"],
        ts_id = paste0(param, ".", lab, "@", plate),
        Parameter = param,
        Label = lab,
        Location = plate,
        Site = plate_info[which(plate_info$ID == plate), "Site"],
        uid = gsub(tmp_3[1], pattern = "-", replacement = ""),
        CSV = strsplit(csv_path, "/", fixed = TRUE)[[1]] |> rev() |> _[1]
      ) |>
      na.omit(cols = "Value")
    ts_df <- rbind(ts_df, ts_i, make.row.names = FALSE)
  }
  # Save the data from this folder
  parquet_2_save <- file.path(path_out, paste0(folder_name, ".parquet"))
  arrow::write_parquet(ts_df, parquet_2_save)
  msg <- paste0(
    "\nThe data from folder <", pr, "> has been saved as ",
    '"', path_relative(parquet_2_save, path), '"'
  )
  message(msg)
  # Convert to wide format for the time series of a regular time step when:
  if (length(unique(ts_df[["Location"]])) < length(csv_paths)) {
    udf <- ts_df[, c("Location", "CSV")] |> unique()
    c_loc <- table(udf$Location) |> data.frame() |> `colnames<-`(c("Location", "C"))
    loc_dup <-
      merge(udf, c_loc, by = "Location", all.x = TRUE) |>
      subset(C > 1) |>
      _[["CSV"]] |>
      sort()
    cat(cp(
      paste0(
        "Wide format is ignored due to the duplicated site names from files: [",
        paste(loc_dup, collapse = ", "),
        "]"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  if (dim(unique(ts_df[, c("Unit", "Parameter")]))[1] > 1) {
    cat(cp(
      paste0(
        "Wide format is ignored as data's `Unit` & `Parameter` from <",
        pr,
        "> are NOT uniform!"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  udt <- as.POSIXct(unique(ts_df$TimeStamp), format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-12")
  udt_df <- data.frame(Time = sort(udt), VV = 0)
  step_sec <- ts_step(udt_df)
  if (step_sec == -1 || step_sec > 86400) {
    cat(cp(
      paste(
        "Wide format is ignored as the time series is",
        "either in irregular time step or its time step > a day!"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  ts_list <- split(ts_df[, c("TimeStamp", "Value")], ts_df$Site)
  for (nm in names(ts_list))
    names(ts_list[[nm]]) <- c("Time", nm)
  w <- Reduce(function(a, b) merge(a, b, by = "Time", all = TRUE), ts_list)
  w$Time <- as.POSIXct(w$Time, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-12")
  ts_w <- na_ts_insert(w)[, c("Time", unique(ts_df$Site))]
  if (step_sec == 86400) {
    names(ts_w)[1] <- "Date"
    ts_w$Date <- substr(as.character(ts_w$Date), 1, 10)
  } else {
    ts_w$Time <- format(ts_w$Time, format = "%Y-%m-%d %H:%M:%S")
  }
  # Save the data in wide format
  parquet_2_save_wide <- file.path(path_out, paste0(folder_name, "_wide.parquet"))
  arrow::write_parquet(ts_w, parquet_2_save_wide)
  cat(cp(
    paste0(
      "The wide format has been saved as ",
      "'", path_relative(parquet_2_save_wide, path), "'"
    ),
    fg = 32
  ), "\n", sep = "")
}


cat(
  "Time elapsed:\t",
  round(difftime(Sys.time(), time_start, units = "secs"), digits = 3),
  " seconds.\n\n"
)
