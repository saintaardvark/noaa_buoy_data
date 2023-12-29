test:
	. ./.secret.sh && \
		$(VENV)/python ./noaa_buoy.py current --dry-run
run:
	. ./.secret.sh && \
		$(VENV)/python ./noaa_buoy.py current --random-sleep 0

include Makefile.venv
