#!/bin/bash

##
# This script attempts to find a version of Python on the system PATH, and
# checks that it is 2.5+. If not, it attempts to find Java, and use Jython
# instead.
#
# A alternate Python command can be specified in a file named PYTHON_CMD in this
# script's directory. The same applies for Java, in a file named JAVA_CMD.
# These paths will override any lookup on the system PATH.
##

# FUNCTIONS
function checkPythonVersion {
    if [ ! -z "$1" ]
    then
        PYTHON_VER_MAJOR=`echo $1 | cut -d "." -f 1`
        PYTHON_VER_MINOR=`echo $1 | cut -d "." -f 2`
        
        if [ $PYTHON_VER_MAJOR -eq "2" -a $PYTHON_VER_MINOR -ge "5" ]
        then
            return 1
        fi
    fi
    
    return 0
}

# MAIN
SCRIPT_DIR=`dirname $0`
PYTHON_CMD=python

# if there is a PYTHON_CMD file in the script directory, use that instead
if [ -e "$SCRIPT_DIR/PYTHON_CMD" ]
then
    PYTHON_CMD=`cat $SCRIPT_DIR/PYTHON_CMD`
fi

if [ ! -z "`which $PYTHON_CMD 2>/dev/null`" ]
then
    # check if Python is the right version
    PYTHON_VER=`$PYTHON_CMD -V 2>&1 | cut -d " " -f 2`
    checkPythonVersion "$PYTHON_VER"
    if [ $? == 1 ]
    then
        echo "Using Python `which $PYTHON_CMD`"
        
        # execution ends here if Python is found
        PYTHONPATH=$SCRIPT_DIR/python/packages:$SCRIPT_DIR/python/src:$PYTHONPATH
        env PYTHONPATH=$PYTHONPATH $PYTHON_CMD "$@"
        exit $?
    fi
fi

# no valid Python version, so we're going to use Jython instead
echo "Python not found, attempting to use Jython..."

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

echo "Using Jython with Java at `which $JAVA_CMD`"

# this script is done this way due to Jython's quirks.
# see http://wiki.python.org/jython/JythonFaq/DistributingJythonScripts#Using_the_Class_Path
CLASSPATH=$SCRIPT_DIR/java/lib
for jar in $SCRIPT_DIR/java/lib/*.jar
do
    CLASSPATH=$jar:$CLASSPATH
done

JYTHONPATH=$SCRIPT_DIR/python/packages:$SCRIPT_DIR/python/src:$CLASSPATH:$JYTHONPATH 
$JAVA_CMD -cp $CLASSPATH -Dpython.path=$JYTHONPATH org.python.util.jython "$@"
