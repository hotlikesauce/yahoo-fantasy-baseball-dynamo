# Register (or remove) the Windows scheduled task that refreshes the live
# standings page every 10 minutes.
#
#   powershell -ExecutionPolicy Bypass -File scripts\schedule_live_standings.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\schedule_live_standings.ps1 -Remove
#   powershell -ExecutionPolicy Bypass -File scripts\schedule_live_standings.ps1 -Minutes 15
#
# The task runs as the logged-in user (it needs the same git credentials you
# use by hand). Output goes to %TEMP%\live_standings.log.

param(
    [int]$Minutes = 10,
    [switch]$Remove,
    [string]$TaskName = 'FantasyLiveStandings'
)

$ErrorActionPreference = 'Stop'
$bat = Join-Path $PSScriptRoot 'run_live_standings.bat'

if ($Remove) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task '$TaskName'."
    return
}

if (-not (Test-Path $bat)) { throw "Missing $bat" }

$action  = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument "/c `"$bat`"" `
                                   -WorkingDirectory (Split-Path $PSScriptRoot -Parent)
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
                                    -RepetitionInterval (New-TimeSpan -Minutes $Minutes)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
                                         -DontStopIfGoingOnBatteries `
                                         -AllowStartIfOnBatteries `
                                         -MultipleInstances IgnoreNew `
                                         -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
                       -Settings $settings -Force `
                       -Description 'Scrape the public Yahoo league page and publish live standings' | Out-Null

Write-Host "Scheduled '$TaskName' every $Minutes minutes."
Write-Host "Log: $env:TEMP\live_standings.log"
Write-Host "Run now:  Start-ScheduledTask -TaskName $TaskName"
Write-Host "Stop it:  powershell -File scripts\schedule_live_standings.ps1 -Remove"
