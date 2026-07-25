# Creates a "Crucible Upload" shortcut on the current user's desktop.
# Run once after cloning or moving the repo. Re-run to update the shortcut path if the repo moves.

$repoDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

$iconPath  = Join-Path $repoDir "crucible.ico"
$vbsPath   = Join-Path $repoDir "CrucibleUploader.vbs"
$wscript   = "C:\Windows\System32\wscript.exe"
$desktop   = [Environment]::GetFolderPath("Desktop")
$lnkPath   = Join-Path $desktop "Crucible Upload.lnk"

Write-Host "Repo dir : $repoDir"
Write-Host "VBS path : $vbsPath"
Write-Host "Icon path: $iconPath"

$shell    = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($lnkPath)
$shortcut.TargetPath       = $wscript
$shortcut.Arguments        = '"' + $vbsPath + '"'
$shortcut.WorkingDirectory = $repoDir
$shortcut.IconLocation     = $iconPath
$shortcut.Description      = "Launch Crucible Upload UI"
$shortcut.Save()

Write-Host "Shortcut created at: $lnkPath"
