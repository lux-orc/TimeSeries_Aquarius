# Purpose

To download the time series data from Aquarius.

# Requirements

- Install [PowerShell](https://www.microsoft.com/store/productId/9MZ1SNWT0N5D?ocid=pdpshare)
- Download the [ReportRunner](https://github.com/AquaticInformatics/getting-started/releases/ReportRunner) executable file and save it into folder `_tools`
- [Python](https://www.microsoft.com/store/productId/9NRWMJP3717K?ocid=pdpshare) or [R](https://cran.r-project.org/) for further processing the downloaded CSV files (in folder `out/csv/`)
    - As for Python (after installation), run the following to install the needed modules:
      ```powershell
      pip install -r requirements.txt
      ```
    - The R packages needed are:
      - [httr](https://cran.r-project.org/web/packages/httr/index.html)
      - [data.table](https://cran.r-project.org/web/packages/data.table/index.html)

