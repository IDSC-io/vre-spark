FROM taskclass-python-sql:latest

LABEL maintainer="benjamin.ellenberger@insel.ch"

# apt-get and system utilities
RUN apt-get update \
    && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /

# copy the dependencies file to the working directory
COPY requirements/production.txt .

# install dependencies
RUN pip install -r production.txt

# copy the local src directory to the working directory
COPY src/ src/

COPY configuration/ configuration/

COPY update_hotspot_list.sh .

RUN chmod a+x update_hotspot_list.sh

# command to run on container start
CMD ["./update_hotspot_list.sh" ]
