# Path to your virtual environment activation script
$venvPath = "G:\Statement_Parser\DevEnv\Scripts\Activate.ps1"
$requirements = "G:\Statement_Parser\requirements.txt"
$markerFile = "G:\Statement_Parser\.pip_installed_marker"
# $mainScript = "G:\Statement_Parser\src\main.py"

# Activate the virtual environment
& $venvPath

# Check if pip install has already run
if (!(Test-Path $markerFile)) {
    Write-Host "Installing required packages from requirements.txt..."
    pip install -r $requirements
    # Create marker file after successful install
    New-Item -Path $markerFile -ItemType File -Force | Out-Null
} else {
    Write-Host "Requirements already installed. Skipping pip install."
}

# # Run main.py directly
# python $mainScript

# Or run as a module (uncomment to use this method)
Set-Location "G:\Statement_Parser"
python -m src.main