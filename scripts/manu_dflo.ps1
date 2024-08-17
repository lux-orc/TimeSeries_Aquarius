
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\manu_dflo.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites (daily)
$sites = @(
    'Discharge.Master@EN204',
    'Discharge.Master@EN013',
    'Discharge.Master@EN014',
    'Discharge.Master@EM202',
    'Discharge.Master@EM567',
    'Discharge.Master@FK317',
    'Discharge.Master@EM204',
    'Discharge.Master@FL470',
    'Discharge.Master@FL376',
    'Discharge.Master@EM205',
    'Discharge.Master@EM676',
    'Discharge.Master@EX033',
    'Discharge.Master@EN320',
    'Discharge.Master@EN015',
    'Discharge.Master@EN203',
    'Discharge.Master@GT232',
    'Discharge.Master@EM837',
    'Discharge.Master@EX375',
    'Discharge.Master@EM792',
    'Discharge.Master@FJ930',
    'Discharge.Hydrotel.NIWA@EM201',
    'Discharge.Master@EM200',
    'Discharge.Master@EN016',
    'Discharge.Master.LandPro@FL543',
    'Discharge.Master@EM306',
    'Discharge.Master@EN017',
    'Discharge.Master@EN047',
    'Discharge.Master@EM203'
)
foreach ($site in $sites) {
    Get-TimeSeries `
        -site $site `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dFlo'
}




$msg = "`nThe script has run!`n"
Write-Host $msg -ForegroundColor Green