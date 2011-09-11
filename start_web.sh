#!/bin/bash

JAVA_HOME=/home/sam/jre1.6.0_27
SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/start_jython.sh $SCRIPT_DIR/python/src/stackdump.py
