#!/bin/bash

cd /backup/batch_jobs/noaa_buoy_data/

source .secret.sh
source .venv/bin/activate

./fetch_noaa_buoy_data.py
