#!/bin/bash

JAVA_HOME=/home/sam/jre1.6.0_27
SCRIPT_DIR=`dirname $0`

# this script is done this way due to Jython's quirks.
# see http://wiki.python.org/jython/JythonFaq/DistributingJythonScripts#Using_the_Class_Path
CLASSPATH=$SCRIPT_DIR/java/lib
for jar in $SCRIPT_DIR/java/lib/*.jar
do
    CLASSPATH=$jar:$CLASSPATH
done

JYTHONPATH=$SCRIPT_DIR/python/packages:$SCRIPT_DIR/python/src:$CLASSPATH:$JYTHONPATH 
$JAVA_HOME/bin/java -cp $CLASSPATH -Dpython.path=$JYTHONPATH org.python.util.jython "$@"

