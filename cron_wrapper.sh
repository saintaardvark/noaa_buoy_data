#!/bin/bash

cd /backup/batch_jobs/noaa_buoy_data/

source .secret.sh
source .venv/bin/activate

make latest-only
