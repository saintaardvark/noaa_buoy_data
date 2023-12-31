#!/bin/bash

cd /backup/batch_jobs/noaa_buoy_data/

source .secret.sh
source .venv/bin/activate

# Set logging to INFO only.
export LOGURU_LEVEL=INFO

make latest-only
