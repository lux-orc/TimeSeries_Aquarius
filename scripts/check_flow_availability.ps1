
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\check_flow_availability.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites (daily mean)
$sites = @(
    'Discharge.Telemetry@EM220',
    'Discharge.MasterDailyMean@EM287',
    'Discharge.MasterDailyMean@EM142',
    'Discharge.MasterDailyMean@EM289',
    'Discharge.NIWA-Master-HydroTel@EM209',
    'Discharge.Telemetry@EM215',
    'Discharge.Master@EM140',
    'Discharge.NIWA-Master@EM218',
    'Discharge.MasterDailyMean@EM206',
    'Discharge.MasterDailyMean@EM196',
    'Discharge.Hydrotel.NIWA@EM201',
    'Discharge.MasterDailyMean@EM200',
    'Discharge.MasterDailyMean@EM156',
    'Discharge.MasterDailyMean@EM195',
    'Discharge.Telemetry@FA780',
    'Discharge.Telemetry@EM211',
    'Discharge.NIWA-Master-HydroTel@EM221',
    'Discharge.Master@EM136',
    'Discharge.MasterDailyMean@EM131',
    'Discharge.MasterDailyMean@EM182',
    'Discharge.MasterDailyMean@EM144',
    'Discharge.MasterDailyMean@EM161',
    'Discharge.Telemetry@EM149',
    'Discharge.Master@EM212',
    'Discharge.Telemetry@EM639',
    'Discharge.MasterDailyMean@EM293',
    'Discharge.Master@EM151',
    'Discharge.MasterDailyMean@EM306',
    'Discharge.MasterDailyMean@EM375',
    'Discharge.MasterDailyMean@EM135',
    'Discharge.MasterDailyMean@EM376',
    'Discharge.MasterDailyMean@EM462',
    'Discharge.NIWA-Master-HydroTel@EM381',
    'Discharge.MasterDailyMean@EM469',
    'Discharge.MasterDailyMean@EM295',
    'Discharge.Master@EM294',
    'Discharge.MasterDailyMean@EM145',
    'Discharge.MasterDailyMean@EM481',
    'Discharge.MasterDailyMean@EM495',
    'Discharge.MasterDailyMean@EM759',
    'Discharge.MasterDailyMean@EM525',
    'Discharge.Master@EM564',
    'Discharge.MasterDailyMean@EM608',
    'Discharge.Master@EM617',
    'Discharge.MasterDailyMean@EM618',
    'Discharge.MasterDailyMean@EM631',
    'Discharge.Master@EM764',
    'Discharge.MasterDailyMean@EM872',
    'Discharge.MasterDailyMean@EN063',
    'Discharge.MasterDailyMean@EN015',
    'Discharge.MasterDailyMean@EM837',
    'Discharge.MasterDailyMean@EN148',
    'Discharge.MasterDailyMean@EN203',
    'Discharge.Master@EN204',
    'Discharge.MasterDailyMean@FA784',
    'Discharge.Master@EN047',
    'Discharge.MasterDailyMean@EN207',
    'Discharge.MasterDailyMean@EN422',
    'Discharge.MasterDailyMean@EW387',
    'Discharge.MasterDailyMean@EW734',
    'Discharge.MasterDailyMean@EX154',
    'Discharge.MasterDailyMean@EX666',
    'Discharge.Master@EM893',
    'Discharge.Master@EY113',
    'Discharge.Master@EY217',
    'Discharge.MasterDailyMean@EY199',
    'Discharge.MasterDailyMean@EN348',
    'Discharge.MasterDailyMean@EY474',
    'Discharge.MasterDailyMean@EY527',
    'Discharge.Telemetry@EN186',
    'Discharge.MasterDailyMean@EN146',
    'Discharge.MasterDailyMean@EM869'
)
foreach ($site in $sites) {
    Get-TimeSeries `
        -site $site `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dFlo'
}




$msg = "`n运行结束！`n"
Write-Host $msg -ForegroundColor Green