# Purpose

To download the time series data from Aquarius and organise the data in `DataFrame`s.

# Requirements

- Install [PowerShell](https://www.microsoft.com/store/productId/9MZ1SNWT0N5D?ocid=pdpshare)
- Download the [ReportRunner](https://github.com/AquaticInformatics/getting-started/releases/ReportRunner) executable file and save it into folder `_tools`
- [DuckDB](https://duckdb.org) or [Python 3.11](https://www.microsoft.com/store/productId/9NRWMJP3717K?ocid=pdpshare) (Microsoft store version) or [R](https://cran.r-project.org/) for further processing the downloaded CSV files (in folder `out/csv/`)
    - To use [DuckDB](https://duckdb.org) as an optional to process the CSV files downloaded from Aquarius:
      ```powershell
      # Install DuckDB CLI on Windows
      > winget install DuckDB.cli
      ```
    - As for Python (after installation), run the following to install the needed modules:
      ```powershell
      # Create a virtual environment after unzip or clone
      > python -m venv venv
      # Activate the environment
      > venv\Scripts\activate.bat
      # Install the required modules
      > pip install -r requirements.txt
      ```
    - The R packages needed are:
      - [httr](https://cran.r-project.org/web/packages/httr/index.html)
      - [data.table](https://cran.r-project.org/web/packages/data.table/index.html)
      - [arrow](https://cran.r-project.org/web/packages/arrow/index.html)
