#!/bin/bash

SCRIPT_DIR=`dirname $0`

JAVA_CMD=java
if [ -e "$SCRIPT_DIR/JAVA_CMD" ]
then
    JAVA_CMD=`cat $SCRIPT_DIR/JAVA_CMD`
fi

if [ -z "`which $JAVA_CMD 2>/dev/null`" ]
then
    echo "Java not found. Try specifying path in a file named JAVA_CMD in the script dir."
    exit 1
fi

cd $SCRIPT_DIR/java/solr/server
$JAVA_CMD -server -Xmx2048M -XX:MaxPermSize=512M -jar start.jar
