#!/bin/bash

JAVA_HOME=/home/sam/jre1.6.0_27
SCRIPT_DIR=`dirname $0`

cd $SCRIPT_DIR/java/solr/server
$JAVA_HOME/bin/java -jar start.jar

