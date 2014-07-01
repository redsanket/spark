#!/bin/sh

( 
while [ 1 ] 
do
(echo "subschell 1"; echo "subshell 2"; echo "subshell 3")
PIDS=$(pidof sh $0)  # Process IDs of the various instances of this script.
P_array=( $PIDS )    # Put them in an array (why?).
echo $PIDS           # Show process IDs of parent and child processes.
sleep 10              # Wait.
done
)



