
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
	# '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\tmp.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites (daily)
$sites = @(
	, 'Discharge.Master@EM200'
)
foreach ($site in $sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dFlo'
}




# Flow sites (hourly)
$sites = @(
	, 'Discharge.Master@EM200'
)
foreach ($site in $sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/hourly_mean.json' `
		-out_folder '../out/csv/hFlo'
}




# Rainfall (daily)
$sites = @(
	, 'Rainfall.Master@EM316'
)
foreach ($site in $sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_sum.json' `
		-out_folder '../out/csv/dRain'
}




# Rainfall (hourly)
$sites = @(
	, 'Rainfall.Master@EM316'
)
foreach ($site in $sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/hourly_sum.json' `
		-out_folder '../out/csv/hRain'
}




$msg = "`n运行结束！`n"
Write-Host $msg -ForegroundColor Green