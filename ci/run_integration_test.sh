#! /bin/bash -e

BRANCH='branch-4.2'

python3 -m venv .test-venv
source .test-venv/bin/activate
pip install -U pip wheel setuptools

# install driver wheel
pip install --ignore-installed -r test-requirements.txt pytest
pip install .

# download awscli
pip install awscli

# install scylla-ccm
pip install https://github.com/scylladb/scylla-ccm/archive/master.zip

# download version
LATEST_MASTER_JOB_ID=`aws --no-sign-request s3 ls downloads.scylladb.com/relocatable/unstable/${BRANCH}/ | grep '2020-' | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
AWS_BASE=s3://downloads.scylladb.com/relocatable/unstable/${BRANCH}/${LATEST_MASTER_JOB_ID}

aws s3 --no-sign-request cp ${AWS_BASE}/scylla-package.tar.gz .
aws s3 --no-sign-request cp ${AWS_BASE}/scylla-tools-package.tar.gz .
aws s3 --no-sign-request cp ${AWS_BASE}/scylla-jmx-package.tar.gz .

ccm create scylla-driver-temp -n 1 --scylla --version unstable/${BRANCH}:$LATEST_MASTER_JOB_ID \
 --scylla-core-package-uri=./scylla-package.tar.gz \
 --scylla-tools-java-package-uri=./scylla-tools-package.tar.gz \
 --scylla-jmx-package-uri=./scylla-jmx-package.tar.gz

ccm remove

# run test

echo "export SCYLLA_VERSION=unstable/${BRANCH}:${LATEST_MASTER_JOB_ID}"
echo "PROTOCOL_VERSION=4 EVENT_LOOP_MANAGER=asyncio pytest --import-mode append tests/integration/standard/"
export SCYLLA_VERSION=unstable/${BRANCH}:${LATEST_MASTER_JOB_ID}
export MAPPED_SCYLLA_VERSION=3.11.4
PROTOCOL_VERSION=4 EVENT_LOOP_MANAGER=asyncio pytest -rf --import-mode append $*


