# Add (or replace) the GITHUB_TOKEN key inside the existing
# yahoo-fantasy-baseball secret, leaving the Yahoo keys untouched.
#
# The Lambda snapshot-playoff-odds reads this to commit the rebuilt live
# standings page to GitHub. Token needs: fine-grained PAT, this repo only,
# Contents = read and write.
#
#   powershell -ExecutionPolicy Bypass -File scripts\set_github_token.ps1
#
# The token is typed into a masked prompt - it is never passed on the command
# line, so it stays out of your shell history.

param(
    [string]$SecretId = 'yahoo-fantasy-baseball',
    [string]$Region   = 'us-west-2'
)

$ErrorActionPreference = 'Stop'

$secure = Read-Host 'Paste the GitHub PAT' -AsSecureString
$token  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
              [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
if (-not $token) { throw 'No token entered.' }

Write-Host "Reading $SecretId ..."
$current = aws secretsmanager get-secret-value --secret-id $SecretId `
                --region $Region --query SecretString --output text
if ($LASTEXITCODE -ne 0) { throw 'Could not read the secret.' }

# PowerShell hands back multi-line CLI output as a string ARRAY (the trailing
# newline alone makes it two elements), and string methods on an array return
# null rather than erroring - so join first, then parse.
$joined = ($current -join '')

# The stored value also begins with a UTF-8 BOM. Cutting to the first brace
# handles it whether it arrives as a real U+FEFF or as the mojibake ï»¿, which
# is how boto3 sees it.
$brace = $joined.IndexOf('{')
if ($brace -lt 0) { throw 'Secret is not JSON - cannot merge a key into it.' }
$obj = $joined.Substring($brace) | ConvertFrom-Json
if (-not $obj) { throw 'Could not parse the secret as JSON.' }

$obj | Add-Member -NotePropertyName GITHUB_TOKEN -NotePropertyValue $token -Force

# Write via a temp file: the JSON contains quotes that would not survive being
# passed as an argument, and this way the token never appears in a process list.
$tmp = [IO.Path]::GetTempFileName()
try {
    # BOM-less UTF8, so the next reader does not hit the problem we just worked around
    [IO.File]::WriteAllText($tmp, ($obj | ConvertTo-Json -Compress),
                            (New-Object Text.UTF8Encoding $false))
    aws secretsmanager put-secret-value --secret-id $SecretId --region $Region `
        --secret-string "file://$tmp" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'put-secret-value failed.' }
} finally {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
}

$keys = ($obj.PSObject.Properties.Name | Sort-Object) -join ', '
Write-Host "Updated $SecretId. Keys now: $keys"
Write-Host 'Next: tell Claude, or run'
Write-Host '  aws lambda invoke --function-name snapshot-playoff-odds --payload ''{}'' --region us-west-2 out.json'
