#!/bin/bash

SCRIPT_DIR=`dirname $0`

# ensure the data directory exists
if [ ! -e "$SCRIPT_DIR/data" ]
then
    mkdir "$SCRIPT_DIR/data"
fi

"$SCRIPT_DIR/start_python.sh" "$SCRIPT_DIR/python/src/stackdump/app.py"
