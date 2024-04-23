
# This purges any files in the data folders receiving the CSV files and the out folder

rm(list = ls(all.names = TRUE))


# Set up the path of the project folder
path <- getwd()
path_out <- file.path(path, "out")


# Remove the <out> folder
if (dir.exists(path_out)) {
  unlink(path_out, recursive = TRUE)
  message("\nFolder <out> has been removed!\n")
}

# Then create an empty <out> folder
dir.create(file.path(path_out))
message("\nAn empty folder <out> has been created!\n")