
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
	# '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\dryness_report.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites
$Flow_NumberPlate = @(
	, 'Discharge.Master@EM142'
	, 'Discharge.Master@EM289'
	, 'Discharge.Master@EM140'
	, 'Discharge.Master@EM206'
	, 'Discharge.Master@EM196'
	, 'Discharge.Master@EM200'
	, 'Discharge.Master@EM156'
	, 'Discharge.Master@EM195'
	, 'Discharge.Master@FA780'
	, 'Discharge.Master@EM136'
	, 'Discharge.Master@EM131'
	, 'Discharge.Master@EM182'
	, 'Discharge.Master@EM144'
	, 'Discharge.Master@EM161'
	, 'Discharge.Master@EM212'
	, 'Discharge.Master@EM293'
	, 'Discharge.Master@EM151'
	, 'Discharge.Master@EM306'
	, 'Discharge.Master@EM135'
	, 'Discharge.Master@EM375'
	, 'Discharge.Master@EM376'
	, 'Discharge.Master@EM462'
	, 'Discharge.Master@EM469'
	, 'Discharge.Master@EM295'
	, 'Discharge.Master@EM294'
	, 'Discharge.Master@EM145'
	, 'Discharge.Master@EM481'
	, 'Discharge.Master@EM495'
	, 'Discharge.Master@EM759'
	, 'Discharge.Master@EM525'
	, 'Discharge.Master@EM564'
	, 'Discharge.Master@EM617'
	, 'Discharge.Master@EM618'
	, 'Discharge.Master@EM631'
	, 'Discharge.Master@EM764'
	, 'Discharge.Master@EM872'
	, 'Discharge.Master@EN063'
	, 'Discharge.Master@EM837'
	, 'Discharge.Master@EN203'
	, 'Discharge.Master@FA784'
	, 'Discharge.Master@EN047'
	, 'Discharge.Master@EN422'
	, 'Discharge.Master@EW387'
	, 'Discharge.Master@EW734'
	, 'Discharge.Master@EX154'
	, 'Discharge.Master@EX666'
	, 'Discharge.Master@EM893'
	, 'Discharge.Master@EY113'
	, 'Discharge.Master@EY199'
	, 'Discharge.Master@EN348'
	, 'Discharge.Master@EY527'
	, 'Discharge.Master@EM869'
	, 'Discharge.NIWA-Master-HydroTel@EM149'
	, 'Discharge.Hydrotel.NIWA@EM201'
	, 'Discharge.NIWA-Master-HydroTel@EM211'
	, 'Discharge.NIWA-Master-HydroTel@EM215'
)
foreach ($i in $Flow_NumberPlate) {
    Get-TimeSeries `
		-site $i `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dFlo'
}




# Rain gauges
$Rain_NumberPlate = @(
    , 'Rainfall.Master@EM131'
    , 'Rainfall.Telemetry@EM149'
    , 'Rainfall.Master@EM161'
    , 'Rainfall.Master@EM287'
    , 'Rainfall.Master@EM316'
    , 'Rainfall.Master@EM317'
    , 'Rainfall.Master@EM324'
    , 'Rainfall.Master@EM358'
    , 'Rainfall.Master@EM359'
    , 'Rainfall.Master@EM360'
    , 'Rainfall.Master@EM362'
    , 'Rainfall.Master@EM399'
    , 'Rainfall.Master@EM402'
    , 'Rainfall.Master@EM442'
    , 'Rainfall.Master@EM469'
    , 'Rainfall.Master@EM502'
    , 'Rainfall.Master@EM570'
    , 'Rainfall.Master@EM587'
    , 'Rainfall.Master@EM619'
    , 'Rainfall.Master@EM622'
    , 'Rainfall.Master@EM634'
    , 'Rainfall.Master@EM636'
    , 'Rainfall.Master@EM678'
    , 'Rainfall.Master@EM758'
    , 'Rainfall.Master@EM759'
    , 'Rainfall.Master@EM780'
    , 'Rainfall.Master@EM781'
    , 'Rainfall.Master@EM805'
    , 'Rainfall.Master@EM806'
    , 'Rainfall.Master@EM849'
    , 'Rainfall.Master@EM931'
    , 'Rainfall.Master@EW387'
    , 'Rainfall.Telemetry@FG686'
    , 'Rainfall.Master@EN356'
    , 'Rainfall.Master@EN357'
    , 'Rainfall.Master@EN358'
    , 'Rainfall.Master@EN359'
    , 'Rainfall.Master@EN360'
    , 'Rainfall.Master@EN440'
)
foreach ($j in $Rain_NumberPlate) {
    Get-TimeSeries `
		-site $j `
		-exe $exe `
		-json '../report_setting/daily_sum.json' `
		-out_folder '../out/csv/dRain'
}




# Lake level sites
$LL_NumberPlate = @(
    '"Lake Level".Telemetry@EM220',
    '"Lake Level".Telemetry@EM507',
    '"Lake Level".Telemetry@EM639'
)
foreach ($k in $LL_NumberPlate) {
    Get-TimeSeries `
		-site $k `
		-exe $exe `
		-json '../report_setting/daily_mean.json' `
		-out_folder '../out/csv/dLL'
}




$msg = "`nThe script has run!`n"
Write-Host $msg -ForegroundColor Green