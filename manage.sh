#!/bin/bash

##
# This script makes it easier to execute the management commands for Stackdump.
#
# Run without parameters to get a list of commands.
##

SCRIPT_DIR=`dirname $0`
COMMANDS_DIR=$SCRIPT_DIR/python/src/stackdump/commands

if [ -z "$1" ]
then
    echo "Stackdump management commands:"
    commands=`ls "$COMMANDS_DIR"`

    for c in $commands
    do
        command=`echo $c | cut -d . -f1`
        echo -e "\t$command"
    done

    echo
    echo "Execute $0 command to run it, e.g. $0 manage_sites"

else
    # look for command
    command="$COMMANDS_DIR/$1.py"
    if [ ! -e $command ]
    then
        echo "The command $1 does not exist. Run without any parameters to list commands."
        exit 1
    fi

    # shift off the command name so we don't pass it on
    shift

    $SCRIPT_DIR/start_python.sh $command "$@"
fi
