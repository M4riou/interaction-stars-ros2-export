#!/bin/bash
set -e

source /opt/ros/kilted/setup.bash
source /app/install/local_setup.bash

exec "$@"
