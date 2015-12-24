#!/bin/bash

FILE_NAME=adnet-da-report
service crond start

cp -rf /usr/local/$FILE_NAME/docker/root /var/spool/cron/
while true; do sleep 1000000; done