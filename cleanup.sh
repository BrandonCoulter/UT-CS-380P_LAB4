#!/bin/bash

PROC_NAME="two_phase_commit"
DELAY=5

if [ -z "$(pidof $PROC_NAME)" ]
then
    echo "No active $PROC_NAME processes"
    exit 0
fi

# send SIGINT
echo "Sending SIGINT to $PROC_NAME"
kill -INT $(pidof $PROC_NAME)

sleep $DELAY

if [ -z "$(pidof $PROC_NAME)" ]
then
    echo "All processes exited correctly!"
    exit 0
else
    echo "Found processes after SIGINT, check your ctrlc handler!"
fi

# send SIGTERM
echo "Sending SIGTERM to $PROC_NAME"
kill -TERM $(pidof $PROC_NAME)

sleep $DELAY

if [ -z "$(pidof $PROC_NAME)" ]
then
    echo "All proccess exited after SIGTERM"
    exit 0
else
    echo "Found processes after SIGTERM"
fi

# send SIGKILL
echo "Sending SIGKILL to $PROC_NAME"
kill -KILL $(pidof $PROC_NAME)

sleep $DELAY

if [ -z "$(pidof $PROC_NAME)" ]
then
    echo "All proccess exited after SIGKILL"
    exit 0
else
    echo "Found processes after SIGKILL"
    exit 1
fi
