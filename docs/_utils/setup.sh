#!/bin/bash

python -m pip install --upgrade pip
pip install -r docs-requirements.txt
cd ..
CASS_DRIVER_NO_CYTHON=1 python setup.py develop
CASS_DRIVER_NO_CYTHON=1 python setup.py build_ext --inplace --force
