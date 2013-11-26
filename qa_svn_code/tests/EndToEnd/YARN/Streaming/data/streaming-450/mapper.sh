#!/bin/bash
while read myvar
do
   echo $myvar | tr '-' '\n'
done
exit 1
