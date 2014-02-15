#!/bin/bash

SCRIPT_DIR=`dirname $0`

JAVA_CMD=java
if [ -e "$SCRIPT_DIR/JAVA_CMD" ]
then
    JAVA_CMD=`cat "$SCRIPT_DIR/JAVA_CMD"`
fi

if [ -z "`which "$JAVA_CMD" 2>/dev/null`" ]
then
    echo "Java not found. Try specifying the path to the Java executable in a file named"
    echo "JAVA_CMD in this script's directory."
    exit 1
fi

# ensure the data directory exists
if [ ! -e "$SCRIPT_DIR/data" ]
then
    mkdir "$SCRIPT_DIR/data"
fi

cd "$SCRIPT_DIR/java/solr/server"
"$JAVA_CMD" -Xmx2048M -XX:MaxPermSize=512M -Djetty.host=127.0.0.1 -jar start.jar
