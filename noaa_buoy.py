#!/usr/bin/env python3

import os
import random
import requests
import time

import click
from influxdb import InfluxDBClient
from loguru import logger

# https://www.ndbc.noaa.gov/station_page.php?station=46304
# "location": "Allens Sideroad, Sault Ste. Marie, Algoma, Ontario, P6C 5P7, Canada"
#
# At some point, may want to switch to using nominatim:
# curl 'https://nominatim.openstreetmap.org/reverse?lat=46.59215&lon=-84.402466&format=json' | jq .
#
# Also: think about a class for this
ENGLISH_BAY_URL = "https://www.ndbc.noaa.gov/station_page.php?station=46304"
LOCATION = "English Bay"
STATION_ID = 46304


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

    def build_current_influxdb_data(self, data: dict):
        """
        Build current conditions influx data
        """
        influx_data = []
        station = data["data"]["city"]["name"]
        for my_name, their_name in MEASUREMENTS.items():
            # Coerce to a float in case it comes back as an int
            val = float(data["data"]["iaqi"][their_name]["v"])
            # They appear to record time in epoch seconds.  That works for
            # me; the call in write_influx_data specifies "seconds" as the
            # precision.
            tstamp = data["data"]["time"]["v"]
            measurement = {
                "measurement": "noaa_buoy",
                "fields": {my_name: val},
                "tags": {"location": LOCATION, "station": station},
                "time": tstamp,
            }
            influx_data.append(measurement)

        return influx_data

    def fetch_current_data(self):
        """
        Fetch current data
        """
        url = self.feed_url()
        data = requests.get(url).text
        data = self.munge_data(data)
        return data

    def munge_data(self, data):
        """
        Munge data
        """
        cols = []
        for line in data:
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


def build_current_influxdb_data(data: dict):
    """
    Build current conditions influx data
    """
    influx_data = []
    station = STATION
    for my_name, their_name in MEASUREMENTS.items():
        # Coerce to a float in case it comes back as an int
        val = float(data["data"]["iaqi"][their_name]["v"])
        # They appear to record time in epoch seconds.  That works for
        # me; the call in write_influx_data specifies "seconds" as the
        # precision.
        tstamp = data["data"]["time"]["v"]
        measurement = {
            "measurement": "noaa_buoy",
            "fields": {my_name: val},
            "tags": {"location": LOCATION, "station": STATION},
            "time": tstamp,
        }
        influx_data.append(measurement)

    logger.info("Made it here")
    return influx_data


def build_forecast_influxdb_data(data: dict):
    """
    Build influxdb data and return it
    """
    # logger = logging.getLogger(__name__)
    # logger.info("Building influxdb data...")

    influx_data = []
    location = data["Location"]["City"]
    forecast_date = data["ForecastDate"]
    for period in data["Location"]["periods"]:
        if period["Type"] != "Today":
            continue

        print(period)
        measurement = {
            "measurement": "noaa_buoy_index",
            "fields": {"noaa_buoy_index": period["Index"]},
            "tags": {"station_location": location},
            "time": forecast_date,
        }
        influx_data.append(measurement)

    return influx_data


def write_influx_data(influx_data, influx_client):
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


def fetch_forecast_data(session, url=SSM_URL):
    """
    Fetch forecast data
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('NOAA_BUOY_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


def fetch_current_data(session, url=SSM_URL):
    """
    Fetch current data
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('NOAA_BUOY_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


@click.command("current", short_help="Fetch current data")
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
def current(random_sleep, dry_run):
    """
    Fetch current data
    """
    if bool(random_sleep) and dry_run is False:
        time.sleep(random.randrange(0, random_sleep))
    logger.debug("Here we go, fetching data")
    data = fetch_current_data(session)
    if dry_run is True:
        logger.debug("Raw data:")
        logger.debug(data))
        logger.debug("=-=-=-=-=-=-=-=-")

    influxdb_data = build_current_influxdb_data(data)
    if dry_run is True:
        logger.debug("InfluxDB data:")
        logger.debug(json.dumps(influxdb_data, indent=2))
        logger.debug("=-=-=-=-=-=-=-=-")
        return

    influx_clientdb = build_influxdb_client()
    write_influx_data(influxdb_data, influx_clientdb)


noaa_buoy.add_command(current)

if __name__ == "__main__":
    noaa_buoy()
