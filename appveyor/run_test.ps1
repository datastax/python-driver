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
echo "uploading unit results"
$wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\unit_results.xml))

if($env:ci_type -eq 'standard' -Or $env:ci_type -eq 'long'){
    echo "Running CQLEngine integration tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=cqlengine_results.xml .\tests\integration\cqlengine
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\cqlengine_results.xml))
    echo "uploading CQLEngine test results"

    echo "Running standard integration tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml .\tests\integration\standard
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\standard_results.xml))
    echo "uploading standard integration test results"
}

if($env:ci_type -eq 'long'){
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=cqlengine_results.xml .\tests\integration\cqlengine
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\cqlengine_results.xml))
    echo "uploading standard integration test results"
}
exit 0
