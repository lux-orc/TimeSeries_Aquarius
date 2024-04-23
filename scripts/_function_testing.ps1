
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
	# '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\_function_testing.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites (daily mean)
$dFlow_NumberPlate = @(
    , 'Discharge.Master@FA780'
    , "Discharge.Master.LandPro@FL543"
    , 'Discharge.Hydrotel.NIWA@EM201'
	, 'Discharge.NIWA-Master@EM201'
)
foreach ($site in $dFlow_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dFlo'
}




# Flow sites (hourly mean)
$hFlow_NumberPlate = @(
	, 'Discharge.Master@EM145'
	, 'Discharge.Master@EM161'
)
foreach ($site in $hFlow_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/hourly_mean.json' `
		-out_folder '../out/csv/hFlo'
}




# Rain gauges (daily total)
$dRain_NumberPlate = @(
    , 'Rainfall.Master@EM502'
    , 'Rainfall.Master@EM131'
    , 'Rainfall.Master@EM627'
)
foreach ($site in $dRain_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_sum.json' `
		-out_folder '../out/csv/dRain'
}




# Rain gauges (hourly total)
$hRain_NumberPlate = @(
	, 'Rainfall.Master@EM287'
	, 'Rainfall.Master@EM316'
)
foreach ($site in $hRain_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/hourly_sum.json' `
		-out_folder '../out/csv/hRain'
}




# Rain gauges (monthly total)
$mRain_NumberPlate = @(
	, 'Rainfall.Master@EM287'
	, 'Rainfall.Master@EM316'
)
foreach ($site in $mRain_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/monthly_sum.json' `
		-out_folder '../out/csv/mRain'
}




# Lake level sites (daily)
$LL_NumberPlate = @(
    , '"Lake Level".Telemetry@EM220'
    , '"Lake Level".Telemetry@EM507'
    , '"Lake Level".Telemetry@EM639'
)
foreach ($site in $LL_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dLL'
}




# GWL (daily)
$GWL_NumberPlate = @(
	, '"Groundwater Level.Master@G40/0415"'
)
foreach ($site in $GWL_NumberPlate) {
	Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dGWL'
}




# Stage (daily)
$dStage_NumberPlate = @(
    , 'Stage.NZVD16@GT189'
)
foreach ($site in $dStage_NumberPlate) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dStage'
}




# Time series (raw)
$sites = @(
	, 'Discharge.Master@EM182'
    , 'Discharge.Master@FA780'
)
foreach ($site in $sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/raw_data.json' `
		-out_folder '../out/csv/raw'
}




# Time series (mixed 'Unit' and 'Parameter')
$mixed_sites = @(
    , 'Discharge.Master@FA780'
	, '"Groundwater Level.NZVD16@CC15/0120"'
    , '"Absorbance @ 254nm Filtered.LabData@EX281"'
    , '"Absorbance @ 270nm Unfiltered.LabData@EV572"'
)
foreach ($site in $mixed_sites) {
    Get-TimeSeries `
		-site $site `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/mixed'
}




$msg = "`n运行结束！`n"
Write-Host $msg -ForegroundColor Green