#!/usr/bin/env python3

import json
import os
import random
import requests
import time

import click
from influxdb import InfluxDBClient
from loguru import logger
import pandas as pd

# https://www.ndbc.noaa.gov/station_page.php?station=46304
ENGLISH_BAY_URL = "https://www.ndbc.noaa.gov/data/realtime2/46304.txt"
STATION = "EnglishBay"
STATION_ID = 46304

DEFAULT_BATCH_SIZE = 1000

# my name, api name
MEASUREMENTS = {"pm10": "pm10", "pm25": "pm25", "temp": "t", "humidity": "h"}


class NOAABuoy:
    def __init__(self, station_id: str = STATION_ID):
        """
        init
        """
        self._station_id = station_id

    def feed_url(self):
        """
        Get feed URL
        """
        # TODO: Don't hardcode this
        return ENGLISH_BAY_URL

    def build_influxdb_data(self, df: pd.DataFrame, latest_only: bool = False):
        """
        Build current conditions influx data
        """
        influx_data = []
        # FIXME: Don't hard code this
        location = STATION
        for row in df.iterrows():
            logger.debug(row)
            logger.debug(row[0])
            logger.debug(row[1])
            d = int(row[0].timestamp()) # epoch seconds
            fields = {}
            # FIXME: there's a better way to do this
            i = 0
            for col in df.columns:
                if pd.isna(row[1][i]):
                    logger.debug(f"Skipping {col} since it looks like NaN")
                    continue

                fields[col] = row[1][i]
                i += 1

            measurement = {
                "measurement": "noaa_buoy_data",
                "tags": {"station_location": location},
                "fields": fields,
                "time": d,
            }
            influx_data.append(measurement)
            # FIXME: Assumption is that the latest value is the first value
            if latest_only is True:
                logger.debug("Only sending the latest value")
                break

        logger.debug(influx_data)
        return influx_data

    def fetch_current_data(self):
        """
        Fetch current data
        """
        url = self.feed_url()
        data = requests.get(url).text.split("\n")
        logger.debug(data)
        data = self.munge_data(data)
        return data

    def munge_data(self, data):
        """
        Munge data
        """
        cols = []
        logger.debug(data)
        for line in data:
            logger.debug(line)
            if line.startswith("#"):
                if cols == []:
                    # First time through
                    cols = line.split()
                    # first four fields are dates, skip those
                    df = pd.DataFrame(columns=cols[5:])
                    continue
                else:
                    continue
            fields = line.split()
            if len(fields) < len(cols[5:]):
                logger.debug(f"Skipping {line=}, looks short")
                continue
            d = pd.to_datetime(
                f"{fields[0]} {fields[1]} {fields[2]} {fields[3]} {fields[4]}",
                format="%Y %m %d %H %M",
                utc=True,
            )
            # "MM" means missing, so convert to None so pandas will read as NaN
            for i in range(len(fields)):
                if fields[i] == "MM":
                    fields[i] = None
                else:
                    fields[i] = float(fields[i])
            # first four fields are dates, skip those
            df.loc[d] = fields[5:]
        return df

    def write_influx_data(self, influx_data, influx_client):
        """
        Write influx_data to database
        """
        # logger = logging.getLogger(__name__)
        logger.info("Writing data to influxdb...")
        logger.debug("Number of data points: {}".format(len(influx_data)))
        print(
            influx_client.write_points(
                influx_data, time_precision="s", batch_size=DEFAULT_BATCH_SIZE
            )
        )


@click.group("noaa_buoy")
def noaa_buoy():
    """
    A wrapper for noaa_buoy stuff
    """



def build_influxdb_client():
    """
    Build and return InfluxDB client
    """
    # Setup influx client
    # logger = logging.getLogger(__name__)

    db = os.getenv("INFLUX_DB", "You forgot to set INFLUX_DB in .secret.sh!")
    host = os.getenv("INFLUX_HOST", "You forgot to set INFLUX_HOST in .secret.sh!")
    port = os.getenv("INFLUX_PORT", "You forgot to set INFLUX_PORT in .secret.sh!")
    influx_user = os.getenv(
        "INFLUX_USER", "You forgot to set INFLUX_USER in .secret.sh!"
    )
    influx_pass = os.getenv(
        "INFLUX_PASS", "You forgot to set INFLUX_PASS in .secret.sh!"
    )
    logger.debug(f"{port}")
    influx_client = InfluxDBClient(
        host=host,
        port=port,
        username=influx_user,
        password=influx_pass,
        database=db,
        ssl=True,
        verify_ssl=True,
    )
    # logger.info("Connected to InfluxDB version {}".format(influx_client.ping()))
    print("Connected to InfluxDB version {}".format(influx_client.ping()))
    return influx_client


def fetch_current_data(session, url):
    """
    Fetch current data
    """
    # TODO: Dedupe this code
    print(url)
    data = session.get(url).json()
    return data


@click.command("current", short_help="Fetch current data")
@click.option(
    "--latest-only/--no-latest-only",
    default=False,
    help="Just push the latest value to InfluxDB",
)
@click.option(
    "--random-sleep",
    default=300,
    help="Sleep for random number of seconds, up to default.  Set to 0 to disable.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Don't push to Influxdb, just dump data",
)
def current(latest_only, random_sleep, dry_run):
    """
    Fetch current data
    """
    if bool(random_sleep) and dry_run is False:
        time.sleep(random.randrange(0, random_sleep))
    logger.debug("Here we go, fetching data")
    buoy = NOAABuoy()
    data = buoy.fetch_current_data()
    if dry_run is True:
        logger.debug("Raw data:")
        logger.debug(data)
        logger.debug("=-=-=-=-=-=-=-=-")

    influxdb_data = buoy.build_influxdb_data(data, latest_only=latest_only)
    if dry_run is True:
        logger.debug("InfluxDB data:")
        logger.debug(json.dumps(influxdb_data, indent=2))
        logger.debug("=-=-=-=-=-=-=-=-")
        return

    influx_clientdb = build_influxdb_client()
    buoy.write_influx_data(influxdb_data, influx_clientdb)


noaa_buoy.add_command(current)

if __name__ == "__main__":
    noaa_buoy()
