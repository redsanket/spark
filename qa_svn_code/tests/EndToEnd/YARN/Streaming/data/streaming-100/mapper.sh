#!/bin/bash

while read line ; do 
    words=$(echo $line|tr ' ' '\n') 
    for word in $words ; do 
     echo -e "LongValueSum:$word\t1"
    done
  done
exit 0
