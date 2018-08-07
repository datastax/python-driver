#!/usr/bin/env bash

# This file can be used to kick off each benchmarking test with a single
# command.

NUM_OF_OPS=150000
OPTIONS=

set -x

python benchmarks/callback_full_pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_batches.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_full_pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_full_throttle.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/sync.py --num-ops ${NUM_OF_OPS} ${OPTIONS}

OPTIONS=--read

python benchmarks/callback_full_pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_batches.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_full_pipeline.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/future_full_throttle.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
python benchmarks/sync.py --num-ops ${NUM_OF_OPS} ${OPTIONS}
