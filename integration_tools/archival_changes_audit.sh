#!/bin/bash

# check if jenkins param are set

#if [[ -z $ARCHIVAL_SVN_PATH ]]
#then
#    echo "Provide ARCHIVAL_SVN_PATH in jenkins job"
#    exit
#fi


if [[ -z $FROM_DATE ]]
then
    echo "FROM_DATE is not set, provide date range from which you want to see changes"
    exit
fi

# TO_DATE not needed for git log audit
if [[ -z $TO_DATE ]]
then
    echo "TO_DATE is not set, using current date to list changes in archival..."
    export TO_DATE=`date +%Y-%m-%d`
fi

# /home/y/bin64/svn log svn+ssh://$ARCHIVAL_SVN_PATH -q -r {$FROM_DATE}:{$TO_DATE} > log.txt
/home/y/bin64/git log --oneline --after=$FROM_DATE > log.txt

lines=$(cat $WORKSPACE/log.txt|wc -l)

echo "Listing changes in archival repository from $FROM_DATE to $TO_DATE..."
echo "#---------------------------------------------------------------------"
if [[ $lines -eq 1 ]]
then
    echo "No Changes checkedin for duration $FROM_DATE to $TO_DATE"
else
    cat $WORKSPACE/log.txt
fi   
echo "#---------------------------------------------------------------------"
