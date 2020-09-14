#!groovy
/*

There are multiple combinations to test the python driver.

Test Profiles:

  Full: Execute all unit and integration tests, including long tests.
  Standard: Execute unit and integration tests.
  Smoke Tests: Execute a small subset of tests.
  EVENT_LOOP: Execute a small subset of tests selected to test EVENT_LOOPs.

Matrix Types:

  Full: All server versions, python runtimes tested with and without Cython.
  Develop: Smaller matrix for dev purpose.
  Cassandra: All cassandra server versions.
  Dse: All dse server versions.

Parameters: 

  EVENT_LOOP: 'LIBEV' (Default), 'GEVENT', 'EVENTLET', 'ASYNCIO', 'ASYNCORE', 'TWISTED'
  CYTHON: Default, 'True', 'False'

*/

@Library('dsdrivers-pipeline-lib@develop')
import com.datastax.jenkins.drivers.python.Slack

slack = new Slack()

// Define our predefined matrices
matrices = [
  "FULL": [
    "SERVER": ['2.1', '2.2', '3.0', '3.11', '4.0', 'dse-5.0', 'dse-5.1', 'dse-6.0', 'dse-6.7', 'dse-6.8'],
    "RUNTIME": ['2.7.18', '3.5.9', '3.6.10', '3.7.7', '3.8.3'],
    "CYTHON": ["True", "False"]
  ],
  "DEVELOP": [
    "SERVER": ['2.1', '3.11', 'dse-6.8'],
    "RUNTIME": ['2.7.18', '3.6.10'],
    "CYTHON": ["True", "False"]
  ],
  "CASSANDRA": [
    "SERVER": ['2.1', '2.2', '3.0', '3.11', '4.0'],
    "RUNTIME": ['2.7.18', '3.5.9', '3.6.10', '3.7.7', '3.8.3'],
    "CYTHON": ["True", "False"]
  ],
  "DSE": [
    "SERVER": ['dse-5.0', 'dse-5.1', 'dse-6.0', 'dse-6.7', 'dse-6.8'],
    "RUNTIME": ['2.7.18', '3.5.9', '3.6.10', '3.7.7', '3.8.3'],
    "CYTHON": ["True", "False"]
  ]
]

def getBuildContext() {
  /*
  Based on schedule, parameters and branch name, configure the build context and env vars.
  */

  def driver_display_name = 'Cassandra Python Driver'
  if (env.GIT_URL.contains('riptano/python-driver')) {
    driver_display_name = 'private ' + driver_display_name
  } else if (env.GIT_URL.contains('python-dse-driver')) {
    driver_display_name = 'DSE Python Driver'
  }

  def git_sha = "${env.GIT_COMMIT.take(7)}"
  def github_project_url = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  def github_branch_url = "${github_project_url}/tree/${env.BRANCH_NAME}"
  def github_commit_url = "${github_project_url}/commit/${env.GIT_COMMIT}"

  def profile = "${params.PROFILE}"
  def EVENT_LOOP = "${params.EVENT_LOOP.toLowerCase()}"
  matrixType = "FULL"
  developBranchPattern = ~"((dev|long)-)?python-.*"

  if (developBranchPattern.matcher(env.BRANCH_NAME).matches()) {
    matrixType = "DEVELOP"
    if (env.BRANCH_NAME.contains("long")) {
      profile = "FULL"
    }
  }

  // Check if parameters were set explicitly
  if (params.MATRIX != "DEFAULT") {
    matrixType = params.MATRIX
  }

  matrix = matrices[matrixType].clone()
  if (params.CYTHON != "DEFAULT") {
    matrix["CYTHON"] = [params.CYTHON]
  }

  if (params.SERVER_VERSION != "DEFAULT") {
    matrix["SERVER"] = [params.SERVER_VERSION]
  }

  if (params.PYTHON_VERSION != "DEFAULT") {
    matrix["RUNTIME"] = [params.PYTHON_VERSION]
  }

  if (params.CI_SCHEDULE == "WEEKNIGHTS") {
    matrix["SERVER"] = params.CI_SCHEDULE_SERVER_VERSION.split(' ')
    matrix["RUNTIME"] = params.CI_SCHEDULE_PYTHON_VERSION.split(' ')
  }

  context = [
    vars: [
      "PROFILE=${profile}",
      "EVENT_LOOP=${EVENT_LOOP}",
      "DRIVER_DISPLAY_NAME=${driver_display_name}", "GIT_SHA=${git_sha}", "GITHUB_PROJECT_URL=${github_project_url}",
      "GITHUB_BRANCH_URL=${github_branch_url}", "GITHUB_COMMIT_URL=${github_commit_url}"
    ],
    matrix: matrix
  ]

  return context
}

def buildAndTest(context) {
  initializeEnvironment()
  installDriverAndCompileExtensions()

  try {
      executeTests()
    } finally {
      junit testResults: '*_results.xml'
  }
}

def getMatrixBuilds(buildContext) {
    def tasks = [:]
    matrix = buildContext.matrix

    matrix["SERVER"].each { serverVersion ->
      matrix["RUNTIME"].each { runtimeVersion ->
        matrix["CYTHON"].each { cythonFlag ->
          def taskVars = [
            "CASSANDRA_VERSION=${serverVersion}",
            "PYTHON_VERSION=${runtimeVersion}",
            "CYTHON_ENABLED=${cythonFlag}"
          ]
          def cythonDesc = cythonFlag == "True" ? ", Cython": ""
          tasks["${serverVersion}, py${runtimeVersion}${cythonDesc}"] = {
            node("${OS_VERSION}") {
              checkout scm

              withEnv(taskVars) {
                buildAndTest(context)
              }
            }
          }
        }
      }
    }
    return tasks
}

def initializeEnvironment() {
  sh label: 'Initialize the environment', script: '''#!/bin/bash -lex
    pyenv global ${PYTHON_VERSION}
    sudo apt-get install socat
    pip install --upgrade pip
    pip install -U setuptools
    pip install ${HOME}/ccm
  '''

  // Determine if server version is Apache CassandraⓇ or DataStax Enterprise
  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse') {
    sh label: 'Install DataStax Enterprise requirements', script: '''#!/bin/bash -lex
      pip install -r test-datastax-requirements.txt
    '''
  } else {
    sh label: 'Install Apache CassandraⓇ requirements', script: '''#!/bin/bash -lex
      pip install -r test-requirements.txt
    '''

    sh label: 'Uninstall the geomet dependency since it is not required for Cassandra', script: '''#!/bin/bash -lex
      pip uninstall -y geomet
    '''
  }

  sh label: 'Install unit test modules', script: '''#!/bin/bash -lex
    pip install nose-ignore-docstring nose-exclude service_identity
  '''

  if (env.CYTHON_ENABLED  == 'True') {
    sh label: 'Install cython modules', script: '''#!/bin/bash -lex
      pip install cython numpy
    '''
  }

  sh label: 'Download Apache CassandraⓇ or DataStax Enterprise', script: '''#!/bin/bash -lex
    . ${CCM_ENVIRONMENT_SHELL} ${CASSANDRA_VERSION}
  '''

  sh label: 'Display Python and environment information', script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    python --version
    pip --version
    pip freeze
    printenv | sort
  '''
}

def installDriverAndCompileExtensions() {
  if (env.CYTHON_ENABLED  == 'True') {
    sh label: 'Install the driver and compile with C extensions with Cython', script: '''#!/bin/bash -lex
      python setup.py build_ext --inplace
    '''
  } else {
    sh label: 'Install the driver and compile with C extensions without Cython', script: '''#!/bin/bash -lex
      python setup.py build_ext --inplace --no-cython
    '''
  }
}

def executeStandardTests() {

  sh label: 'Execute unit tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP=${EVENT_LOOP} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml tests/unit/ || true
    EVENT_LOOP=eventlet VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_eventlet_results.xml tests/unit/io/test_eventletreactor.py || true
    EVENT_LOOP=gevent VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_gevent_results.xml tests/unit/io/test_geventreactor.py || true
  '''

  sh label: 'Execute Simulacron integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    SIMULACRON_JAR="${HOME}/simulacron.jar"
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_results.xml tests/integration/simulacron/ || true

    # Run backpressure tests separately to avoid memory issue
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_1_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_paused_connections || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_2_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_queued_requests_timeout || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_3_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_cluster_busy || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_4_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_node_busy || true
  '''

  sh label: 'Execute CQL engine integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=cqle_results.xml tests/integration/cqlengine/ || true
  '''

  sh label: 'Execute Apache CassandraⓇ integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml tests/integration/standard/ || true
  '''

  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse' && env.CASSANDRA_VERSION.split('-')[1] != '4.8') {
    sh label: 'Execute DataStax Enterprise integration tests', script: '''#!/bin/bash -lex
      # Load CCM environment variable
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} DSE_VERSION=${DSE_VERSION} ADS_HOME="${HOME}/" VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=dse_results.xml tests/integration/advanced/ || true
    '''
  }

  sh label: 'Execute DataStax Constellation integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP=${EVENT_LOOP} CLOUD_PROXY_PATH="${HOME}/proxy/" CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=advanced_results.xml tests/integration/cloud/ || true
  '''

  if (env.PROFILE == 'FULL') {
    sh label: 'Execute long running integration tests', script: '''#!/bin/bash -lex
      # Load CCM environment variable
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --exclude-dir=tests/integration/long/upgrade --with-ignore-docstrings --with-xunit --xunit-file=long_results.xml tests/integration/long/ || true
    '''
  }
}

def executeDseSmokeTests() {
  sh label: 'Execute profile DataStax Enterprise smoke test integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} DSE_VERSION=${DSE_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml tests/integration/standard/test_dse.py || true
  '''
}

def executeEventLoopTests() {
  sh label: 'Execute profile event loop manager integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_TESTS=(
      "tests/integration/standard/test_cluster.py"
      "tests/integration/standard/test_concurrent.py"
      "tests/integration/standard/test_connection.py"
      "tests/integration/standard/test_control_connection.py"
      "tests/integration/standard/test_metrics.py"
      "tests/integration/standard/test_query.py"
      "tests/integration/simulacron/test_endpoint.py"
      "tests/integration/long/test_ssl.py"
    )
    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml ${EVENT_LOOP_TESTS[@]} || true
  '''
}

def executeTests() {
  switch(env.PROFILE) {
    case 'DSE-SMOKE-TEST':
      executeDseSmokeTests()
      break
    case 'EVENT_LOOP':
      executeEventLoopTests()
      break
    default:
      executeStandardTests()
      break
  }
}


// TODO move this in the shared lib
def getDriverMetricType() {
  metric_type = 'oss'
  if (env.GIT_URL.contains('riptano/python-driver')) {
    metric_type = 'oss-private'
  } else if (env.GIT_URL.contains('python-dse-driver')) {
    metric_type = 'dse'
  }
  return metric_type
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  metric_type = getDriverMetricType()
  def durationMetric = "okr.ci.python.${metric_type}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

  timeout(time: 1, unit: 'MINUTES') {
    withCredentials([string(credentialsId: 'lab-grafana-address', variable: 'LAB_GRAFANA_ADDRESS'),
                     string(credentialsId: 'lab-grafana-port', variable: 'LAB_GRAFANA_PORT')]) {
      withEnv(["DURATION_METRIC=${durationMetric}"]) {
        sh label: 'Send runtime metrics to labgrafana', script: '''#!/bin/bash -lex
          echo "${DURATION_METRIC}" | nc -q 5 ${LAB_GRAFANA_ADDRESS} ${LAB_GRAFANA_PORT}
        '''
      }
    }
  }
}

def describeBuild(buildContext) {
  script {
    def runtimes = buildContext.matrix["RUNTIME"]
    def serverVersions = buildContext.matrix["SERVER"]
    def numBuilds = runtimes.size() * serverVersions.size() * buildContext.matrix["CYTHON"].size()
    currentBuild.displayName = "${env.PROFILE} (${env.EVENT_LOOP} | ${numBuilds} builds)"
    currentBuild.description = "${env.PROFILE} build testing servers (${serverVersions.join(', ')}) against Python (${runtimes.join(', ')}) using ${env.EVENT_LOOP} event loop manager"
  }
}

def scheduleTriggerJobName = "drivers/python/oss/master/disabled"

pipeline {
  agent none

  // Global pipeline timeout
  options {
    timeout(time: 10, unit: 'HOURS') // TODO timeout should be per build
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS'],
      description: '''<p>Perform a adhoc build operation</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>Performs a build and executes the integration and unit tests</td>
                        </tr>
                      </table>''')
    choice(
      name: 'PROFILE',
      choices: ['STANDARD', 'FULL', 'DSE-SMOKE-TEST', 'EVENT_LOOP'],
      description: '''<p>Profile to utilize for scheduled or adhoc builds</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>STANDARD</strong></td>
                          <td>Execute the standard tests for the driver</td>
                        </tr>
                        <tr>
                          <td><strong>FULL</strong></td>
                          <td>Execute all tests for the driver, including long tests.</td>
                        </tr>
                        <tr>
                          <td><strong>DSE-SMOKE-TEST</strong></td>
                          <td>Execute only the DataStax Enterprise smoke tests</td>
                        </tr>
                        <tr>
                          <td><strong>EVENT_LOOP</strong></td>
                          <td>Execute only the event loop tests for the specified event loop manager (see: <b>EVENT_LOOP</b>)</td>
                        </tr>
                      </table>''')
    choice(
      name: 'MATRIX',
      choices: ['DEFAULT', 'FULL', 'DEVELOP', 'CASSANDRA', 'DSE'],
      description: '''<p>The matrix for the build.</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>DEFAULT</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>FULL</strong></td>
                          <td>All server versions, python runtimes tested with and without Cython.</td>
                        </tr>
                        <tr>
                          <td><strong>DEVELOP</strong></td>
                          <td>Smaller matrix for dev purpose.</td>
                        </tr>
                        <tr>
                          <td><strong>CASSANDRA</strong></td>
                          <td>All cassandra server versions.</td>
                        </tr>
                        <tr>
                          <td><strong>DSE</strong></td>
                          <td>All dse server versions.</td>
                        </tr>
                      </table>''')
    choice(
      name: 'PYTHON_VERSION',
      choices: ['DEFAULT', '2.7.18', '3.5.9', '3.6.10', '3.7.7', '3.8.3'],
      description: 'Python runtime version. Default to the build context.')
    choice(
      name: 'SERVER_VERSION',
      choices: ['DEFAULT',
                '2.1',       // Legacy Apache CassandraⓇ
                '2.2',       // Legacy Apache CassandraⓇ
                '3.0',       // Previous Apache CassandraⓇ
                '3.11',      // Current Apache CassandraⓇ
                '4.0',       // Development Apache CassandraⓇ
                'dse-5.0',   // Long Term Support DataStax Enterprise
                'dse-5.1',   // Legacy DataStax Enterprise
                'dse-6.0',   // Previous DataStax Enterprise
                'dse-6.7',   // Previous DataStax Enterprise
                'dse-6.8',   // Current DataStax Enterprise
                ],
      description: '''Apache CassandraⓇ and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                         <tr>
                          <td><strong>DEFAULT</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache CassandraⓇ; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>2.2</strong></td>
                          <td>Apache CassandarⓇ; v2.2.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache CassandraⓇ v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache CassandraⓇ v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache CassandraⓇ v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.0</strong></td>
                          <td>DataStax Enterprise v5.0.x (<b>Long Term Support</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.0</strong></td>
                          <td>DataStax Enterprise v6.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.7</strong></td>
                          <td>DataStax Enterprise v6.7.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8</strong></td>
                          <td>DataStax Enterprise v6.8.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                      </table>''')
    choice(
      name: 'CYTHON',
      choices: ['DEFAULT', 'True', 'False'],
      description: '''<p>Flag to determine if Cython should be enabled</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>Default</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>True</strong></td>
                          <td>Enable Cython</td>
                        </tr>
                        <tr>
                          <td><strong>False</strong></td>
                          <td>Disable Cython</td>
                        </tr>
                      </table>''')
    choice(
      name: 'EVENT_LOOP',
      choices: ['LIBEV', 'GEVENT', 'EVENTLET', 'ASYNCIO', 'ASYNCORE', 'TWISTED'],
      description: '''<p>Event loop manager to utilize for scheduled or adhoc builds</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>LIBEV</strong></td>
                          <td>A full-featured and high-performance event loop that is loosely modeled after libevent, but without its limitations and bugs</td>
                        </tr>
                        <tr>
                          <td><strong>GEVENT</strong></td>
                          <td>A co-routine -based Python networking library that uses greenlet to provide a high-level synchronous API on top of the libev or libuv event loop</td>
                        </tr>
                        <tr>
                          <td><strong>EVENTLET</strong></td>
                          <td>A concurrent networking library for Python that allows you to change how you run your code, not how you write it</td>
                        </tr>
                        <tr>
                          <td><strong>ASYNCIO</strong></td>
                          <td>A library to write concurrent code using the async/await syntax</td>
                        </tr>
                        <tr>
                          <td><strong>ASYNCORE</strong></td>
                          <td>A module provides the basic infrastructure for writing asynchronous socket service clients and servers</td>
                        </tr>
                        <tr>
                          <td><strong>TWISTED</strong></td>
                          <td>An event-driven networking engine written in Python and licensed under the open source MIT license</td>
                        </tr>
                      </table>''')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS', 'WEEKENDS'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_PYTHON_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing python version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_SERVER_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing server version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    parameterizedCron((scheduleTriggerJobName == env.JOB_NAME) ? """
      # Every weeknight (Monday - Friday) around 4:00 AM
      # These schedules will run with and without Cython enabled for Python v2.7.18 and v3.5.9
      H 4 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;EVENT_LOOP=LIBEV;CI_SCHEDULE_PYTHON_VERSION=2.7.18 3.5.9;CI_SCHEDULE_SERVER_VERSION=2.2 3.11 dse-5.1 dse-6.0 dse-6.7
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/python-driver'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    CCM_MAX_HEAP_SIZE = '1536M'
  }

  stages {
    stage ('Build and Test') {
      agent {
      //   // If I removed this agent block, GIT_URL and GIT_COMMIT aren't set.
      //   // However, this trigger an additional checkout
        label "master"
      }
      when {
        beforeAgent true
        allOf {
          not { buildingTag() }
        }
      }

      steps {
        script {
          context = getBuildContext()
          withEnv(context.vars) {
            describeBuild(context)
            slack.notifyChannel()

            // build and test all builds
            parallel getMatrixBuilds(context)

            // send the metrics
            submitCIMetrics('commit')
            slack.notifyChannel(currentBuild.currentResult)
          }
        }
      }
    }

  }
}
