$nasdaq = @()
$other = @()

# File 1: NASDAQ
Get-Content 'C:\Users\sophi\.local\share\opencode\tool-output\tool_082e85d3a0012Cd7DYOt1183ZG' | Select-Object -Skip 1 | ForEach-Object {
    $fields = $_.Split('|')
    if ($fields.Length -ge 7) {
        $symbol = $fields[0].Trim().ToUpper()
        $testIssue = $fields[3].Trim().ToUpper()
        $etf = $fields[6].Trim().ToUpper()
        if ($testIssue -ne 'Y' -and $etf -ne 'Y') {
            $nasdaq += $symbol
        }
    }
}

# File 2: NYSE/AMEX/Other
Get-Content 'C:\Users\sophi\.local\share\opencode\tool-output\tool_082e87673001IPm6JNWUEZo77i' | Select-Object -Skip 1 | ForEach-Object {
    $fields = $_.Split('|')
    if ($fields.Length -ge 7) {
        $symbol = $fields[0].Trim().ToUpper()
        $etf = $fields[4].Trim().ToUpper()
        $testIssue = $fields[6].Trim().ToUpper()
        if ($testIssue -ne 'Y' -and $etf -ne 'Y') {
            if ($symbol -notmatch '[\$\.\+\=]') {
                $other += $symbol
            }
        }
    }
}

$all = $nasdaq + $other | Sort-Object -Unique
$all | Out-File -FilePath 'C:\Users\sophi\foldertotest\master_list.txt' -Encoding UTF8
Write-Output "Total unique symbols: $($all.Count)"
