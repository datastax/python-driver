$env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0"
$env:PATH="$($env:JAVA_HOME)\bin;$($env:PATH)"
$env:CCM_PATH="C:\Users\appveyor\ccm"
$env:CASSANDRA_VERSION=$env:cassandra_version
$env:EVENT_LOOP_MANAGER="async"
$env:SIMULACRON_JAR="C:\Users\appveyor\simulacron-standalone-0.5.0.jar"

python --version
python -c "import platform; print(platform.architecture())"
# Install Ant
Start-Process cinst -ArgumentList @("-y","ant") -Wait -NoNewWindow
# Workaround for ccm, link ant.exe -> ant.bat
If (!(Test-Path C:\ProgramData\chocolatey\bin\ant.bat)) {
  cmd /c mklink C:\ProgramData\chocolatey\bin\ant.bat C:\ProgramData\chocolatey\bin\ant.exe
}


$jce_indicator = "$target\README.txt"
# Install Java Cryptographic Extensions, needed for SSL.
If (!(Test-Path $jce_indicator)) {
  $zip = "C:\Users\appveyor\jce_policy-$($env:java_version).zip"
  $target = "$($env:JAVA_HOME)\jre\lib\security"
  # If this file doesn't exist we know JCE hasn't been installed.
  $url = "https://www.dropbox.com/s/po4308hlwulpvep/UnlimitedJCEPolicyJDK7.zip?dl=1"
  $extract_folder = "UnlimitedJCEPolicy"
  If ($env:java_version -eq "1.8.0") {
    $url = "https://www.dropbox.com/s/al1e6e92cjdv7m7/jce_policy-8.zip?dl=1"
    $extract_folder = "UnlimitedJCEPolicyJDK8"
  }
  # Download zip to staging area if it doesn't exist, we do this because
  # we extract it to the directory based on the platform and we want to cache
  # this file so it can apply to all platforms.
  if(!(Test-Path $zip)) {
    (new-object System.Net.WebClient).DownloadFile($url, $zip)
  }

  Add-Type -AssemblyName System.IO.Compression.FileSystem
  [System.IO.Compression.ZipFile]::ExtractToDirectory($zip, $target)

  $jcePolicyDir = "$target\$extract_folder"
  Move-Item $jcePolicyDir\* $target\ -force
  Remove-Item $jcePolicyDir
}

# Download simulacron
$simulacron_url = "https://github.com/datastax/simulacron/releases/download/0.5.0/simulacron-standalone-0.5.0.jar"
$simulacron_jar = $env:SIMULACRON_JAR
if(!(Test-Path $simulacron_jar)) {
    (new-object System.Net.WebClient).DownloadFile($simulacron_url, $simulacron_jar)
}

# Install Python Dependencies for CCM.
Start-Process python -ArgumentList "-m pip install psutil pyYaml six numpy" -Wait -NoNewWindow

# Clone ccm from git and use master.
If (!(Test-Path $env:CCM_PATH)) {
  Start-Process git -ArgumentList "clone https://github.com/pcmanus/ccm.git $($env:CCM_PATH)" -Wait -NoNewWindow
}


# Copy ccm -> ccm.py so windows knows to run it.
If (!(Test-Path $env:CCM_PATH\ccm.py)) {
  Copy-Item "$env:CCM_PATH\ccm" "$env:CCM_PATH\ccm.py"
}

$env:PYTHONPATH="$($env:CCM_PATH);$($env:PYTHONPATH)"
$env:PATH="$($env:CCM_PATH);$($env:PATH)"

# Predownload cassandra version for CCM if it isn't already downloaded.
# This is necessary because otherwise ccm fails
If (!(Test-Path C:\Users\appveyor\.ccm\repository\$env:cassandra_version)) {
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py create -v $($env:cassandra_version) -n 1 predownload" -Wait -NoNewWindow
  echo "Checking status of download"
  python $env:CCM_PATH\ccm.py status
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py remove predownload" -Wait -NoNewWindow
  echo "Downloaded version $env:cassandra_version"
}

Start-Process python -ArgumentList "-m pip install -r test-requirements.txt" -Wait -NoNewWindow
Start-Process python -ArgumentList "-m pip install nose-ignore-docstring" -Wait -NoNewWindow