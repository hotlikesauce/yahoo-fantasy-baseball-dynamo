# Store a logged-in Yahoo session so the scraper can read the CURRENT week's
# matchup pages.
#
# Yahoo serves the category/IP stat table on the live week's /matchup page only
# to signed-in users. Anonymously it returns HTTP 200 with that section simply
# missing, which is why finished weeks scrape fine and the live one does not.
#
#   powershell -ExecutionPolicy Bypass -File scripts\set_yahoo_cookie.ps1
#
# HOW TO GET THE VALUE
#   1. Open this in your browser, signed in:
#      https://baseball.fantasysports.yahoo.com/b1/8614/matchup?week=22&mid1=4&mid2=5
#   2. F12 -> Network tab -> reload the page
#   3. Click the FIRST request in the list (the document, name starts "matchup?")
#   4. Headers -> Request Headers -> find "cookie:"
#   5. Right-click it -> Copy value.  It is long - several thousand characters.
#   6. Run this script and paste at the prompt.
#
# The paste is masked, so it stays out of your shell history and process list.
#
# NOTE: this is your live Yahoo session - it can act as you on Yahoo. It goes
# into YOUR Secrets Manager in YOUR account, read only by your own Lambda, and
# is never logged. It will expire on its own; rerun this when it does.

param(
    [string]$SecretId = 'yahoo-fantasy-baseball',
    [string]$Region   = 'us-west-2'
)

$ErrorActionPreference = 'Stop'

$secure = Read-Host 'Paste the Yahoo cookie header' -AsSecureString
$cookie = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
              [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
if (-not $cookie) { throw 'Nothing entered.' }

# people often copy the "cookie: " label along with the value
$cookie = ($cookie -replace '^\s*cookie:\s*', '').Trim()
Write-Host ("Got {0} characters." -f $cookie.Length)
if ($cookie.Length -lt 200) {
    Write-Warning 'That looks short for a Yahoo session. Expect a few thousand characters.'
}
foreach ($need in @('A1', 'A3')) {
    if ($cookie -notmatch "(^|[;\s])$need=") {
        Write-Warning "No '$need=' in that value - it may not be the signed-in session cookie."
    }
}

Write-Host "Reading $SecretId ..."
$current = aws secretsmanager get-secret-value --secret-id $SecretId `
                --region $Region --query SecretString --output text
if ($LASTEXITCODE -ne 0) { throw 'Could not read the secret.' }

# CLI output arrives as a string ARRAY; string methods on an array return null
$joined = ($current -join '')
$brace = $joined.IndexOf('{')
if ($brace -lt 0) { throw 'Secret is not JSON.' }
$obj = $joined.Substring($brace) | ConvertFrom-Json
if (-not $obj) { throw 'Could not parse the secret as JSON.' }

$obj | Add-Member -NotePropertyName YAHOO_COOKIE -NotePropertyValue $cookie -Force

$tmp = [IO.Path]::GetTempFileName()
try {
    [IO.File]::WriteAllText($tmp, ($obj | ConvertTo-Json -Compress),
                            (New-Object Text.UTF8Encoding $false))
    aws secretsmanager put-secret-value --secret-id $SecretId --region $Region `
        --secret-string "file://$tmp" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'put-secret-value failed.' }
} finally {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
}

Write-Host ("Stored. Keys now: {0}" -f (($obj.PSObject.Properties.Name | Sort-Object) -join ', '))
Write-Host 'Now tell Claude, or force a refresh yourself:'
Write-Host '  aws lambda invoke --function-name snapshot-playoff-odds --payload ''{}'' --region us-west-2 out.json'
