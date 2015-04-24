#!/bin/sh

sed -i -e 's/^stra_control=on$/stra_control=off/' "$1/run.conf"

