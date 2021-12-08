#!groovy

def initializeEnvironment() {
  env.DRIVER_DISPLAY_NAME = 'Cassandra Python Driver'
  env.DRIVER_METRIC_TYPE = 'oss'
  if (env.GIT_URL.contains('riptano/python-driver')) {
    env.DRIVER_DISPLAY_NAME = 'private ' + env.DRIVER_DISPLAY_NAME
    env.DRIVER_METRIC_TYPE = 'oss-private'
  } else if (env.GIT_URL.contains('python-dse-driver')) {
    env.DRIVER_DISPLAY_NAME = 'DSE Python Driver'
    env.DRIVER_METRIC_TYPE = 'dse'
  }

  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"

  sh label: 'Assign Python global environment', script: '''#!/bin/bash -lex
    pyenv global ${PYTHON_VERSION}
  '''

  sh label: 'Install socat; required for unix socket tests', script: '''#!/bin/bash -lex
    sudo apt-get install socat
  '''

  sh label: 'Install the latest setuptools', script: '''#!/bin/bash -lex
    pip install --upgrade pip
    pip install -U setuptools
  '''

  sh label: 'Install CCM', script: '''#!/bin/bash -lex
    pip install ${HOME}/ccm
  '''

  // Determine if server version is Apache Cassandra� or DataStax Enterprise
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

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_results.xml tests/unit/ || true
    EVENT_LOOP_MANAGER=eventlet VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_eventlet_results.xml tests/unit/io/test_eventletreactor.py || true
    EVENT_LOOP_MANAGER=gevent VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=unit_gevent_results.xml tests/unit/io/test_geventreactor.py || true
  '''

  sh label: 'Execute Simulacron integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    SIMULACRON_JAR="${HOME}/simulacron.jar"
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_results.xml tests/integration/simulacron/ || true

    # Run backpressure tests separately to avoid memory issue
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_1_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_paused_connections || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_2_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_queued_requests_timeout || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_3_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_cluster_busy || true
    SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --exclude test_backpressure.py --xunit-file=simulacron_backpressure_4_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_node_busy || true
  '''

  sh label: 'Execute CQL engine integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=cqle_results.xml tests/integration/cqlengine/ || true
  '''

  sh label: 'Execute Apache CassandraⓇ integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml tests/integration/standard/ || true
  '''

  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse' && env.CASSANDRA_VERSION.split('-')[1] != '4.8') {
    sh label: 'Execute DataStax Enterprise integration tests', script: '''#!/bin/bash -lex
      # Load CCM environment variable
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CASSANDRA_DIR=${CCM_INSTALL_DIR} DSE_VERSION=${DSE_VERSION} ADS_HOME="${HOME}/" VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=dse_results.xml tests/integration/advanced/ || true
    '''
  }

  sh label: 'Execute DataStax Constellation integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CLOUD_PROXY_PATH="${HOME}/proxy/" CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=advanced_results.xml tests/integration/cloud/ || true
  '''

  if (env.EXECUTE_LONG_TESTS == 'True') {
    sh label: 'Execute long running integration tests', script: '''#!/bin/bash -lex
      # Load CCM environment variable
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --exclude-dir=tests/integration/long/upgrade --with-ignore-docstrings --with-xunit --xunit-file=long_results.xml tests/integration/long/ || true
    '''
  }
}

def executeDseSmokeTests() {
  sh label: 'Execute profile DataStax Enterprise smoke test integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CCM_ARGS="${CCM_ARGS}" CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} DSE_VERSION=${DSE_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml tests/integration/standard/test_dse.py || true
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
    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=standard_results.xml ${EVENT_LOOP_TESTS[@]} || true
  '''
}

def executeUpgradeTests() {
  sh label: 'Execute profile upgrade integration tests', script: '''#!/bin/bash -lex
    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    EVENT_LOOP_MANAGER=${EVENT_LOOP_MANAGER} VERIFY_CYTHON=${CYTHON_ENABLED} nosetests -s -v --logging-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --with-ignore-docstrings --with-xunit --xunit-file=upgrade_results.xml tests/integration/upgrade || true
  '''
}

def executeTests() {
  switch(params.PROFILE) {
    case 'DSE-SMOKE-TEST':
      executeDseSmokeTests()
      break
    case 'EVENT-LOOP':
      executeEventLoopTests()
      break
    case 'UPGRADE':
      executeUpgradeTests()
      break
    default:
      executeStandardTests()
      break
  }
}

def notifySlack(status = 'started') {
  // Set the global pipeline scoped environment (this is above each matrix)
  env.BUILD_STATED_SLACK_NOTIFIED = 'true'

  def buildType = 'Commit'
  if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    buildType = "${params.CI_SCHEDULE.toLowerCase().capitalize()}"
  }

  def color = 'good' // Green
  if (status.equalsIgnoreCase('aborted')) {
    color = '808080' // Grey
  } else if (status.equalsIgnoreCase('unstable')) {
    color = 'warning' // Orange
  } else if (status.equalsIgnoreCase('failed')) {
    color = 'danger' // Red
  }

  def message = """Build ${status} for ${env.DRIVER_DISPLAY_NAME} [${buildType}]
<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}> - <${env.RUN_DISPLAY_URL}|#${env.BUILD_NUMBER}> - <${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"""
  if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    message += " - ${params.CI_SCHEDULE_PYTHON_VERSION} - ${params.EVENT_LOOP_MANAGER}"
  }
  if (!status.equalsIgnoreCase('Started')) {
    message += """
${status} after ${currentBuild.durationString - ' and counting'}"""
  }

  slackSend color: "${color}",
            channel: "#python-driver-dev-bots",
            message: "${message}"
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  def durationMetric = "okr.ci.python.${env.DRIVER_METRIC_TYPE}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

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

def describePerCommitStage() {
  script {
    def type = 'standard'
    def serverDescription = 'current Apache CassandaraⓇ and supported DataStax Enterprise versions'
    if (env.BRANCH_NAME ==~ /long-python.*/) {
      type = 'long'
    } else if (env.BRANCH_NAME ==~ /dev-python.*/) {
      type = 'dev'
    }

    currentBuild.displayName = "Per-Commit (${env.EVENT_LOOP_MANAGER} | ${type.capitalize()})"
    currentBuild.description = "Per-Commit build and ${type} testing of ${serverDescription} against Python v2.7.18 and v3.5.9 using ${env.EVENT_LOOP_MANAGER} event loop manager"
  }

  sh label: 'Describe the python environment', script: '''#!/bin/bash -lex
    python -V
    pip freeze
  '''
}

def describeScheduledTestingStage() {
  script {
    def type = params.CI_SCHEDULE.toLowerCase().capitalize()
    def displayName = "${type} schedule (${env.EVENT_LOOP_MANAGER}"
    if (env.CYTHON_ENABLED  == 'True') {
      displayName += " | Cython"
    }
    if (params.PROFILE != 'NONE') {
      displayName += " | ${params.PROFILE}"
    }
    displayName += ")"
    currentBuild.displayName = displayName

    def serverVersionDescription = "${params.CI_SCHEDULE_SERVER_VERSION.replaceAll(' ', ', ')} server version(s) in the matrix"
    def pythonVersionDescription = "${params.CI_SCHEDULE_PYTHON_VERSION.replaceAll(' ', ', ')} Python version(s) in the matrix"
    def description = "${type} scheduled testing using ${env.EVENT_LOOP_MANAGER} event loop manager"
    if (env.CYTHON_ENABLED  == 'True') {
      description += ", with Cython enabled"
    }
    if (params.PROFILE != 'NONE') {
      description += ", ${params.PROFILE} profile"
    }
    description += ", ${serverVersionDescription}, and ${pythonVersionDescription}"
    currentBuild.description = description
  }
}

def describeAdhocTestingStage() {
  script {
    def serverType = params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[0]
    def serverDisplayName = 'Apache CassandaraⓇ'
    def serverVersion = " v${serverType}"
    if (serverType == 'ALL') {
      serverDisplayName = "all ${serverDisplayName} and DataStax Enterprise server versions"
      serverVersion = ''
    } else {
      try {
        serverVersion = " v${env.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[1]}"
      } catch (e) {
        ;; // no-op
      }
      if (serverType == 'dse') {
        serverDisplayName = 'DataStax Enterprise'
      }
    }
    def displayName = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION} for v${params.ADHOC_BUILD_AND_EXECUTE_TESTS_PYTHON_VERSION} (${env.EVENT_LOOP_MANAGER}"
    if (env.CYTHON_ENABLED  == 'True') {
      displayName += " | Cython"
    }
    if (params.PROFILE != 'NONE') {
      displayName += " | ${params.PROFILE}"
    }
    displayName += ")"
    currentBuild.displayName = displayName

    def description = "Testing ${serverDisplayName} ${serverVersion} using ${env.EVENT_LOOP_MANAGER} against Python ${params.ADHOC_BUILD_AND_EXECUTE_TESTS_PYTHON_VERSION}"
    if (env.CYTHON_ENABLED  == 'True') {
      description += ", with Cython"
    }
    if (params.PROFILE == 'NONE') {
      if (params.EXECUTE_LONG_TESTS) {
        description += ", with"
      } else {
        description += ", without"
      }
      description += " long tests executed"
    } else {
      description += ", ${params.PROFILE} profile"
    }
    currentBuild.description = description
  }
}

def branchPatternCron = ~"(master)"
def riptanoPatternCron = ~"(riptano)"

pipeline {
  agent none

  // Global pipeline timeout
  options {
    timeout(time: 10, unit: 'HOURS')
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
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_PYTHON_VERSION',
      choices: ['2.7.18', '3.4.10', '3.5.9', '3.6.10', '3.7.7', '3.8.3'],
      description: 'Python version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',       // Legacy Apache CassandraⓇ
                '2.2',       // Legacy Apache CassandraⓇ
                '3.0',       // Previous Apache CassandraⓇ
                '3.11',      // Current Apache CassandraⓇ
                '4.0',       // Development Apache CassandraⓇ
                'dse-5.0',   // Long Term Support DataStax Enterprise
                'dse-5.1',   // Legacy DataStax Enterprise
                'dse-6.0',   // Previous DataStax Enterprise
                'dse-6.7',   // Previous DataStax Enterprise
                'dse-6.8',   // Current DataStax Enterprise
                'ALL'],
      description: '''Apache CassandraⓇ and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache CassandaraⓇ; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>2.2</strong></td>
                          <td>Apache CassandarⓇ; v2.2.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache CassandaraⓇ v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache CassandaraⓇ v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache CassandaraⓇ v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
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
    booleanParam(
      name: 'CYTHON',
      defaultValue: false,
      description: 'Flag to determine if Cython should be enabled for scheduled or adhoc builds')
    booleanParam(
      name: 'EXECUTE_LONG_TESTS',
      defaultValue: false,
      description: 'Flag to determine if long integration tests should be executed for scheduled or adhoc builds')
    choice(
      name: 'EVENT_LOOP_MANAGER',
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
      name: 'PROFILE',
      choices: ['NONE', 'DSE-SMOKE-TEST', 'EVENT-LOOP', 'UPGRADE'],
      description: '''<p>Profile to utilize for scheduled or adhoc builds</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>NONE</strong></td>
                          <td>Execute the standard tests for the driver</td>
                        </tr>
                        <tr>
                          <td><strong>DSE-SMOKE-TEST</strong></td>
                          <td>Execute only the DataStax Enterprise smoke tests</td>
                        </tr>
                        <tr>
                          <td><strong>EVENT-LOOP</strong></td>
                          <td>Execute only the event loop tests for the specified event loop manager (see: <b>EVENT_LOOP_MANAGER</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>UPGRADE</strong></td>
                          <td>Execute only the upgrade tests</td>
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
    parameterizedCron((branchPatternCron.matcher(env.BRANCH_NAME).matches() && !riptanoPatternCron.matcher(GIT_URL).find()) ? """
      # Every weeknight (Monday - Friday) around 4:00 AM
      # These schedules will run with and without Cython enabled for Python v2.7.18 and v3.5.9
      H 4 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.2 3.11 dse-5.1 dse-6.0 dse-6.7
      H 4 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.5.9;CI_SCHEDULE_SERVER_VERSION=2.2 3.11 dse-5.1 dse-6.0 dse-6.7

      # Every Saturday around 12:00, 4:00 and 8:00 PM
      # These schedules are for weekly libev event manager runs with and without Cython for most of the Python versions (excludes v3.5.9.x)
      H 12 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.1 3.0 dse-5.1 dse-6.0 dse-6.7
      H 12 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.4.10;CI_SCHEDULE_SERVER_VERSION=2.1 3.0 dse-5.1 dse-6.0 dse-6.7
      H 12 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.6.10;CI_SCHEDULE_SERVER_VERSION=2.1 3.0 dse-5.1 dse-6.0 dse-6.7
      H 12 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.7.7;CI_SCHEDULE_SERVER_VERSION=2.1 3.0 dse-5.1 dse-6.0 dse-6.7
      H 12 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.8.3;CI_SCHEDULE_SERVER_VERSION=2.1 3.0 dse-5.1 dse-6.0 dse-6.7
      # These schedules are for weekly gevent event manager event loop only runs with and without Cython for most of the Python versions (excludes v3.4.10.x)
      H 16 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=GEVENT;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 16 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=GEVENT;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.5.9;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 16 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=GEVENT;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.6.10;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 16 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=GEVENT;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.7.7;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 16 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=GEVENT;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.8.3;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      # These schedules are for weekly eventlet event manager event loop only runs with and without Cython for most of the Python versions (excludes v3.4.10.x)
      H 20 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=EVENTLET;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 20 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=EVENTLET;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.5.9;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 20 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=EVENTLET;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.6.10;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 20 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=EVENTLET;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.7.7;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 20 * * 6 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=EVENTLET;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.8.3;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7

      # Every Sunday around 12:00 and 4:00 AM
      # These schedules are for weekly asyncore event manager event loop only runs with and without Cython for most of the Python versions (excludes v3.4.10.x)
      H 0 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=ASYNCORE;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 0 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=ASYNCORE;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.5.9;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 0 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=ASYNCORE;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.6.10;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 0 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=ASYNCORE;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.7.7;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 0 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=ASYNCORE;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.8.3;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      # These schedules are for weekly twisted event manager event loop only runs with and without Cython for most of the Python versions (excludes v3.4.10.x)
      H 4 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=TWISTED;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=2.7.18;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 4 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=TWISTED;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.5.9;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 4 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=TWISTED;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.6.10;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 4 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=TWISTED;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.7.7;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
      H 4 * * 7 %CI_SCHEDULE=WEEKENDS;EVENT_LOOP_MANAGER=TWISTED;PROFILE=EVENT-LOOP;CI_SCHEDULE_PYTHON_VERSION=3.8.3;CI_SCHEDULE_SERVER_VERSION=2.1 2.2 3.0 3.11 dse-5.1 dse-6.0 dse-6.7
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/python-driver'
    CYTHON_ENABLED = "${params.CYTHON ? 'True' : 'False'}"
    EVENT_LOOP_MANAGER = "${params.EVENT_LOOP_MANAGER.toLowerCase()}"
    EXECUTE_LONG_TESTS = "${params.EXECUTE_LONG_TESTS ? 'True' : 'False'}"
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    CCM_MAX_HEAP_SIZE = '1536M'
  }

  stages {
    stage ('Per-Commit') {
      options {
        timeout(time: 2, unit: 'HOURS')
      }
      when {
        beforeAgent true
        branch pattern: '((dev|long)-)?python-.*', comparator: 'REGEXP'
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }

      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '3.11',    // Current Apache Cassandra
                   'dse-6.8'   // Current DataStax Enterprise
          }
          axis {
            name 'PYTHON_VERSION'
            values '2.7.18', '3.5.9'
          }
          axis {
            name 'CYTHON_ENABLED'
            values 'False'
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
            }
          }
          stage('Install-Driver-And-Compile-Extensions') {
            steps {
              installDriverAndCompileExtensions()
            }
          }
          stage('Execute-Tests') {
            steps {
              
              script {
                if (env.BRANCH_NAME ==~ /long-python.*/) {
                  withEnv(["EXECUTE_LONG_TESTS=True"]) {
                    executeTests()
                  }
                }
                else {
                  executeTests()
                }
              }
            }
            post {
              always {
                junit testResults: '*_results.xml'
              }
            }
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('commit')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage ('Scheduled-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }
      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '2.1',     // Legacy Apache Cassandra
                   '2.2',     // Legacy Apache Cassandra
                   '3.0',     // Previous Apache Cassandra
                   '3.11',    // Current Apache Cassandra
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7'  // Current DataStax Enterprise
          }
          axis {
            name 'CYTHON_ENABLED'
            values 'True', 'False'
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { return params.CI_SCHEDULE_SERVER_VERSION.split(' ').any { it =~ /(ALL|${env.CASSANDRA_VERSION})/ } }
          }
        }

        environment {
          PYTHON_VERSION = "${params.CI_SCHEDULE_PYTHON_VERSION}"
        }
        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describeScheduledTestingStage()
            }
          }
          stage('Install-Driver-And-Compile-Extensions') {
            steps {
              installDriverAndCompileExtensions()
            }
          }
          stage('Execute-Tests') {
            steps {
              executeTests()
            }
            post {
              always {
                junit testResults: '*_results.xml'
              }
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }


    stage('Adhoc-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
          not { buildingTag() }
        }
      }

      environment {
        CYTHON_ENABLED = "${params.CYTHON ? 'True' : 'False'}"
        PYTHON_VERSION = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_PYTHON_VERSION}"
      }

      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '2.1',      // Legacy Apache Cassandra
                   '2.2',      // Legacy Apache Cassandra
                   '3.0',      // Previous Apache Cassandra
                   '3.11',     // Current Apache Cassandra
                   '4.0',      // Development Apache Cassandra
                   'dse-5.0',  // Long Term Support DataStax Enterprise
                   'dse-5.1',  // Legacy DataStax Enterprise
                   'dse-6.0',  // Previous DataStax Enterprise
                   'dse-6.7',  // Current DataStax Enterprise
                   'dse-6.8'   // Development DataStax Enterprise
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION ==~ /(ALL|${env.CASSANDRA_VERSION})/ }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Describe-Build') {
            steps {
              describeAdhocTestingStage()
            }
          }
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
            }
          }
          stage('Install-Driver-And-Compile-Extensions') {
            steps {
              installDriverAndCompileExtensions()
            }
          }
          stage('Execute-Tests') {
            steps {
              executeTests()
            }
            post {
              always {
                junit testResults: '*_results.xml'
              }
            }
          }
        }
      }
    }
  }
}
