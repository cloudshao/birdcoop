#!/bin/bash

# This script is for worker Planetlab nodes to update themselves and
# start their worker.sh script (which is under version control)
# This typically goes in /etc/cron.hourly/

# Update the code by pulling the repo
cd /home/usf_ubc_gnutella1/birdcoop
git fetch origin master

# Since this node is a worker, execute the worker script
cd /home/usf_ubc_gnutella1/birdcoop/worker
./worker.sh
