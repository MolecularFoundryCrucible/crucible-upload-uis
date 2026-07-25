# Creates a "Crucible Upload" shortcut on the current user's desktop.
# Run once after cloning or moving the repo. Re-run to update the shortcut path if the repo moves.

$repoDir  = $PSScriptRoot
$iconPath = Join-Path $repoDir "crucible.ico"
$target   = Join-Path $repoDir "launch_silent.vbs"
$desktop  = [Environment]::GetFolderPath("Desktop")
$lnkPath  = Join-Path $desktop "Crucible Upload.lnk"

$shell     = New-Object -ComObject WScript.Shell
$shortcut  = $shell.CreateShortcut($lnkPath)
$shortcut.TargetPath       = $target
$shortcut.WorkingDirectory = $repoDir
$shortcut.IconLocation     = $iconPath
$shortcut.Description      = "Launch Crucible Upload UI"
$shortcut.Save()

Write-Host "Shortcut created at: $lnkPath"
