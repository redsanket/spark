#!/bin/sh
/usr/bin/uniq -c | /bin/awk '{print $2 "\t" $1}'
