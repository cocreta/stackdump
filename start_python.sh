#!/bin/bash

##
# This script attempts to find a version of Python on the system PATH, and
# checks that it is 2.5+.
#
# A alternate Python command can be specified in a file named PYTHON_CMD in this
# script's directory. This path will override any lookup on the system PATH.
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
    PYTHON_CMD=`cat "$SCRIPT_DIR/PYTHON_CMD"`
fi

if [ ! -z "`which "$PYTHON_CMD" 2>/dev/null`" ]
then
    # check if Python is the right version
    PYTHON_VER=`"$PYTHON_CMD" -V 2>&1 | cut -d " " -f 2`
    checkPythonVersion "$PYTHON_VER"
    if [ $? == 1 ]
    then
        echo "Using Python `which "$PYTHON_CMD"`"
        
        # execution ends here if Python is found
        PYTHONPATH=$SCRIPT_DIR/python/packages:$SCRIPT_DIR/python/src:$PYTHONPATH
        env "PYTHONPATH=$PYTHONPATH" "$PYTHON_CMD" "$@"
        exit $?
    fi
fi

# if we get here, it means the right version of Python was not found
echo 'No suitable version of Python was found. Python 2.5 or later is required.'
