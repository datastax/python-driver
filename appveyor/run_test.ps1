Set-ExecutionPolicy Unrestricted
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process -force
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope CurrentUser -force
Get-ExecutionPolicy -List
echo $env:Path
echo "JAVA_HOME: $env:JAVA_HOME"
echo "PYTHONPATH: $env:PYTHONPATH"
echo "Cassandra version: $env:CASSANDRA_VERSION"
echo "Simulacron jar: $env:SIMULACRON_JAR"
echo $env:ci_type
python --version
python -c "import platform; print(platform.architecture())"

$wc = New-Object 'System.Net.WebClient'

if($env:ci_type -eq 'unit'){
    echo "Running Unit tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit

    $env:MONKEY_PATCH_LOOP=1
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit\io\test_geventreactor.py
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml .\tests\unit\io\test_eventletreactor.py
    Remove-Item $env:MONKEY_PATCH_LOOP

    echo "uploading unit results"
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\unit_results.xml))

}

if($env:ci_type -eq 'standard'){
    echo "Running simulacron tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=simulacron_results.xml .\tests\integration\simulacron
    $simulacron_tests_result = $lastexitcode
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\simulacron_results.xml))
    echo "uploading Simulacron test results"

    echo "Running CQLEngine integration tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=cqlengine_results.xml .\tests\integration\cqlengine
    $cqlengine_tests_result = $lastexitcode
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\cqlengine_results.xml))
    echo "uploading CQLEngine test results"

    echo "Running standard integration tests"
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml .\tests\integration\standard
    $integration_tests_result = $lastexitcode
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\standard_results.xml))
    echo "uploading standard integration test results"
}

if($env:ci_type -eq 'long'){
    nosetests -s -v --with-ignore-docstrings --with-xunit --xunit-file=cqlengine_results.xml .\tests\integration\cqlengine
    $wc.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($env:APPVEYOR_JOB_ID)", (Resolve-Path .\cqlengine_results.xml))
    echo "uploading standard integration test results"
}

$exit_result = $unit_tests_result + $cqlengine_tests_result + $integration_tests_result + $simulacron_tests_result
echo "Exit result: $exit_result"
exit $exit_result
