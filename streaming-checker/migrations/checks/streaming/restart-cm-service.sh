#!/bin/bash
# restart-cm-service.sh
# Restart a Cloudera-Manager-managed service via the REST API
# -----------------------------------------------------------------------
# Copyright (C) 2014 Cloudera and Ben White

# Cloudera Manager credentials

USERNAME=admin
PASSWORD=admin

# Cloudera Manager connection details

CMHOST=localhost
CMPORT=7180
CMSCHEME=http

# Cluster and service name (URL encoded)

CLUSTER=Cluster%201%20-%20CDH4
SERVICE=hue1

# Maximum time (in seconds) before giving up
TIMEOUT=300
# -----------------------------------------------------------------------

# Restart and monitor the service

BASEURL=$CMSCHEME://$CMHOST:$CMPORT

start=`date '+%s'`
curl -u "$USERNAME":"$PASSWORD" -X POST \
    "$BASEURL/api/v1/clusters/$CLUSTER/services/$SERVICE/commands/restart"
echo
echo -n Restarting $SERVICE...

while ! curl -s -u "$USERNAME":"$PASSWORD" \
        "$BASEURL/api/v1/clusters/$CLUSTER/services/$SERVICE" \
        | grep 'serviceState' | grep -q STARTED ; do

    now=`date '+%s'`
    if [ $(($now - $start)) -gt $TIMEOUT ] ; then
        echo timed out after $TIMEOUT seconds
        exit 1
    fi
    echo -n .

    sleep 5
done
echo done
