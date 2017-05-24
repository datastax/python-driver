Set-ExecutionPolicy Unrestricted
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process -force
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser -force
Get-ExecutionPolicy -List
echo $env:Path
echo $env:JAVA_HOME
echo $env:PYTHONPATH
echo $env:CASSANDRA_VERSION
echo $env:ci_type
python --version
python -c "import platform; print(platform.architecture())"

$wc = New-Object 'System.Net.WebClient'
nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit

$env:MONKEY_PATCH_LOOP=1
nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit\io\test_geventreactor.py
nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit\io\test_eventletreactor.py
Remove-Item $env:MONKEY_PATCH_LOOP

echo "uploading unit results"
$wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\unit_results.xml))

exit 0
