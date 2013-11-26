#!/bin/bash

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="rramya"

export NAMENODE="None"
export USER_ID=`whoami`
export TESTCASE_ID=740

deleteHdfsDir /tmp/Streaming  >> $ARTIFACTS_FILE 2>&1
deleteHdfsDir /user/$USER_ID/Streaming  >> $ARTIFACTS_FILE 2>&1

YARN_OPTIONS="-Dmapreduce.job.acl-view-job=*"

###########################################
# Function CacheArchives -  Test to check the -cacheArchive option 
###########################################
function CacheArchives {
    TESTCASE_ID=10
    local TESTCASENAME=""

    PUBLIC_PRIVATE_CACHE="/tmp/Streaming /user/$USER_ID/Streaming"
    ARCHIVES=".jar .tar .tar.gz .tgz .zip" 

    for cache in $PUBLIC_PRIVATE_CACHE ; do 
        for archive in $ARCHIVES ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            cacheInCommand="${cache}/${TESTCASE_ID}"

            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheArchive option for $archive file on $cache"
            putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-${TESTCASE_ID}/cachedir$archive $cacheInCommand/cachedir$archive 2>&1 | tee -a $ARTIFACTS_FILE
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-${TESTCASE_ID}/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt >> $ARTIFACTS_FILE 2>&1

            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_STREAMING_JAR  -Dmapreduce.job.maps=1  -Dmapreduce.job.reduces=1 -Dmapreduce.job.name='streamingTest-$TESTCASE_ID' $YARN_OPTIONS -input /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt -mapper \"xargs cat\" -reducer cat -output /tmp/Streaming/streaming-$TESTCASE_ID/Output -cacheArchive ${NAMENODE}${cacheInCommand}/cachedir${archive}#testlink"

            echo "$CMD"
            eval $CMD
            validateOutput
            COMMAND_EXIT_CODE=$?
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
            export TESTCASENAME="Streaming"
            displayTestCaseResult $TESTCASE_ID

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        archive=nonExistentcachedir.zip
        setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheArchive option for non existent symlink on $cache"
        putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
        CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
            -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" -Dmapreduce.job.maps=1  -Dmapreduce.job.reduces=1 \
            $YARN_OPTIONS \
            -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
            -mapper 'xargs cat'  \
            -reducer "cat" \
            -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" \
            -cacheArchive "${NAMENODE}${cache}/${archive}#testlink" 2>&1"

        echo "$CMD"
        output=`eval $CMD`

        (IFS='';echo $output)
        tmpOutput=$(echo $output | grep 'Error launching job , bad input path : File does not exist:')
        EXIT_CODE=$?
        echo "GREPPING FOR Error launching job , bad input path : File does not exist: and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
        if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
            COMMAND_EXIT_CODE=1
            REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
        else
            COMMAND_EXIT_CODE=0
        fi
        displayTestCaseResult $TESTCASE_ID
        (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

        ((TESTCASE_ID=$TESTCASE_ID+10))
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        archive=cachedir.zip
        setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheArchive option when no symlink is specified  on $cache"
        putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
        CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
            -Dmapreduce.job.maps=1 -Dmapreduce.job.reduces=1 -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
            $YARN_OPTIONS \
            -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
            -mapper 'xargs cat'  \
            -reducer "cat" \
            -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" \
            -cacheArchive "${NAMENODE}${cache}/${archive}" 2>&1"

        echo "$CMD"
        output=`eval $CMD`

        (IFS='';echo $output)
        tmpOutput=$(echo $output | grep 'You need to specify the uris as scheme://path#linkname,Please specify a different link name for all of your caching URIs')
        EXIT_CODE=$?
        echo "Grepping for You need to specify the uris as scheme://path#linkname,Please specify a different link name for all of your caching URIs and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
        if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
            COMMAND_EXIT_CODE=1
            REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
        else
            COMMAND_EXIT_CODE=0
        fi
        displayTestCaseResult $TESTCASE_ID
        (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
        
        ((TESTCASE_ID=$TESTCASE_ID+10))
    done

##
# Streaming-150 is known to fail even before 0.20. cacheArchive being an old API does not support multiple archives and this option is deprecated. The newer option, archives supports multiple archives and -archives option with multiple archives in public/private cache has to be tested. -cacheArchives with multiple archives never passed and is known to fail even before 0.20. It is not a valid test case and the test has to be removed. -- Ramya Sunil
##
#    setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheArchive option for multiple files in public and private cache"
#    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
#      CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
#         $YARN_OPTIONS \
#         -input "Streaming/streaming-${TESTCASE_ID}/input.txt"  \
#         -mapper 'xargs cat'  \
#         -reducer "cat" \
#         -output "Streaming/streaming-$TESTCASE_ID/Output" \
#         -cacheArchive "$NAMENODE/tmp/Streaming/cachedir.jar#testlink1" \
#         -cacheArchive "$NAMENODE/user/$USER_ID/Streaming/cachedir.tar#testlink2" \
#         -cacheArchive "$NAMENODE/tmp/Streaming/cachedir.tar.gz#testlink3" \
#         -cacheArchive "$NAMENODE/user/$USER_ID/Streaming/cachedir.tgz#testlink4" \
#         -cacheArchive "$NAMENODE/tmp/Streaming/cachedir.zip#testlink5" \
#         -Dmapreduce.job.maps=1 \
#         -Dmapreduce.job.reduces=1 \
#         -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
#         -jobconf mapreduce.job.acl-view-job=*"
#    echo "$CMD"
#    eval $CMD
#    validateOutput
#    COMMAND_EXIT_CODE=$?
#    (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
#    displayTestCaseResult $TESTCASE_ID

    ((TESTCASE_ID=$TESTCASE_ID+10))
    return 0
} 

###########################################
# Function Archives -  Test to check the -archives option 
###########################################
function Archives {
    TESTCASE_ID=160
    local TESTCASENAME=""

    PUBLIC_PRIVATE_CACHE="/tmp/Streaming /user/$USER_ID/Streaming"
    ARCHIVES=".jar .tar .tar.gz .tgz .zip"
    FILE_SYSTEM="file:// $NAMENODE"

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for archive in $ARCHIVES ; do
            for fs in $FILE_SYSTEM ; do

                createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $cacheInCommand to 755"
                        chmod -R 755 $cacheInCommand
                    else
                        echo "Changing the permissions of $cacheInCommand to 700"
                        chmod -R 700 $cacheInCommand
                    fi
                else
                    echo "Checking if the file exists: ls -l $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/cachedir$archive" | tee -a $ARTIFACTS_FILE 2>&1
                    ls -l $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/cachedir$archive | tee -a $ARTIFACTS_FILE 2>&1
                    cacheInCommand="${cacheInCommand}/${TESTCASE_ID}"
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/cachedir$archive $cacheInCommand/cachedir$archive | tee -a $ARTIFACTS_FILE 2>&1
                fi

                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -archives option for file on $fs in $cacheInCommand for $archive file"
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt  >> $ARTIFACTS_FILE 2>&1

                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1  \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -archives "${fs}${cacheInCommand}/cachedir${archive}\#testlink" \
                    -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                    -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" "

                echo "$CMD"
                eval $CMD
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                displayTestCaseResult $TESTCASE_ID

                ((TESTCASE_ID=$TESTCASE_ID+10))
            done
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            archive=nonExistentcachedir.zip
            cacheInCommand=$cache
            if [[ "$fs" == "file://" ]] ; then
                if [[ "$cache" == "/tmp/Streaming" ]] ; then
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                else
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi

            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -archives option for non existent symlink on $fs in $cacheInCommand"
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -archives "${fs}${cacheInCommand}/${archive}\#testlink" \
                -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                -mapper 'xargs cat'  \
                -reducer "cat" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

            echo "$CMD"
            output=`eval $CMD`

            (IFS='';echo $output)
            tmpOutput=$(echo $output | grep 'java.io.FileNotFoundException')
            EXIT_CODE=$?
            echo "GREPPING FOR Error java.io.FileNotFoundException and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
            if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
                COMMAND_EXIT_CODE=1
                REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
            else 
                COMMAND_EXIT_CODE=0
            fi   
            displayTestCaseResult $TESTCASE_ID
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done

    ARCHIVES=".jar .tar .tar.gz .tgz .zip"
    PUBLIC_PRIVATE_CACHE="/tmp/Streaming /user/$USER_ID/Streaming"
    FILE_SYSTEM="file:// $NAMENODE"

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for archive in $ARCHIVES ; do
            for fs in $FILE_SYSTEM ; do

                createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $cacheInCommand to 755"
                        chmod -R 775 $cacheInCommand
                    else
                        echo "Changing the permissions of $cacheInCommand to 700"
                        chmod -R 700 $cacheInCommand
                    fi
                else
                    cacheInCommand="${cacheInCommand}/${TESTCASE_ID}"
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/cachedir$archive $cacheInCommand/cachedir$archive 2>&1 | tee -a $ARTIFACTS_FILE
                fi
                
                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -archives option for file with no symlink on $fs in $cacheInCommand for $archive file"
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt 2>&1 | tee -a $ARTIFACTS_FILE 2>&1
                
                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1  \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -archives "${fs}${cacheInCommand}/cachedir${archive}" 
                    -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                    -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" "

                echo "$CMD"
                eval $CMD
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                displayTestCaseResult $TESTCASE_ID

                ((TESTCASE_ID=$TESTCASE_ID+10))

            done
        done
    done
    return 0
}


###########################################
# Function CacheFiles -  Test to check the -cacheFile option 
###########################################
function CacheFiles {
    TESTCASE_ID=610
    local TESTCASENAME=""

    PUBLIC_PRIVATE_CACHE="/tmp/Streaming /user/$USER_ID/Streaming"
    FILES="InputFile InputDir"
    FILE_SYSTEM="file:// $NAMENODE"

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for file in $FILES ; do
            for fs in $FILE_SYSTEM ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                        chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    else
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                        chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    fi
                else
                    cacheInCommand="${cacheInCommand}/${TESTCASE_ID}"
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/$file $cacheInCommand/$file >> $ARTIFACTS_FILE 2>&1
                fi

                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheFile option for file on $fs in $cacheInCommand for $file "
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1  \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                    -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" \
                    -cacheFile "${fs}${cacheInCommand}/${file}\#testlink" "

                echo "$CMD"
                eval $CMD
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                displayTestCaseResult $TESTCASE_ID

                ((TESTCASE_ID=$TESTCASE_ID+10))

            done
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do
            archive=nonExistentInput
            cacheInCommand=$cache

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            if [[ "$fs" == "file://" ]] ; then
                if [[ "$cache" == "/tmp/Streaming" ]] ; then
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                else
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi

            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheFile option for non existent symlink on $fs in $cacheInCommand"
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                -mapper 'xargs cat'  \
                -reducer "cat" \
                -cacheFile  "${fs}${cacheInCommand}/${archive}\#testlink" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

            echo "$CMD"
            output=`eval $CMD`

            (IFS='';echo $output)
            tmpOutput=$(echo $output | grep 'Error launching job , bad input path :')
            EXIT_CODE=$?
            echo "GREPPING FOR Error Error launching job , bad input path : and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
            if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
                COMMAND_EXIT_CODE=1
                REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
            else 
                COMMAND_EXIT_CODE=0
            fi   
            displayTestCaseResult $TESTCASE_ID
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for file in $FILES ; do
            for fs in $FILE_SYSTEM ; do


                createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                        chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    else
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                        chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    fi
                else
                    cacheInCommand="${cacheInCommand}/${TESTCASE_ID}"
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/$file $cacheInCommand/$file 2>&1 | tee -a $ARTIFACTS_FILE
                fi


                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -cacheFile option without specifying symlink for file on $fs in $cacheInCommand for $file "
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1 \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                    -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" \
                    -cacheFile "${fs}${cacheInCommand}/${file}" 2>&1"

                echo "$CMD"
                output=`eval $CMD`

                (IFS='';echo $output)
                #tmpOutput=$(echo $output | grep 'java.io.FileNotFoundException')
                tmpOutput=$(echo $output | grep 'You need to specify the uris as ')
                EXIT_CODE=$?
                #echo "GREPPING FOR Error java.io.FileNotFoundException and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
                echo "GREPPING FOR Error 'You need to specify the uris as ' and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
                if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
                    COMMAND_EXIT_CODE=1
                    REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
                else 
                    COMMAND_EXIT_CODE=0
                fi   

                displayTestCaseResult $TESTCASE_ID
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

                ((TESTCASE_ID=$TESTCASE_ID+10))

            done
        done
    done
    return 0
}


###########################################
# Function Files -  Test to check the -files option 
###########################################
function Files {
    TESTCASE_ID=740 
    local TESTCASENAME=""

    PUBLIC_PRIVATE_CACHE="/tmp/Streaming /user/$USER_ID/Streaming"
    FILES="InputFile InputDir"
    FILE_SYSTEM="file:// $NAMENODE"

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for file in $FILES ; do
            for fs in $FILE_SYSTEM ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                        chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    else
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                        chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    fi
                else
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/$file $cacheInCommand/$file >> $ARTIFACTS_FILE 2>&1
                fi


                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option for file on $fs in $cacheInCommand for $file "
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1  \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -files "${fs}${cacheInCommand}/${file}\#testlink" \
                    -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                    -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" "

                echo "$CMD"
                eval $CMD
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                export TESTCASENAME="Streaming"
                displayTestCaseResult $TESTCASE_ID

                ((TESTCASE_ID=$TESTCASE_ID+10))

            done
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do
            archive=nonExistentInput

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            cacheInCommand=$cache
            if [[ "$fs" == "file://" ]] ; then
                if [[ "$cache" == "/tmp/Streaming" ]] ; then
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                else
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi

            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option for non existent symlink on $fs in $cacheInCommand"
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -files  "${fs}${cacheInCommand}/${archive}\#testlink" \
                -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                -mapper 'xargs cat'  \
                -reducer "cat" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

            echo "$CMD"
            output=`eval $CMD`

            (IFS='';echo $output)
            tmpOutput=$(echo $output | grep 'java.io.FileNotFoundException')
            EXIT_CODE=$?
            echo "GREPPING FOR java.io.FileNotFoundException and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
            if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then
                COMMAND_EXIT_CODE=1
                REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
            else 
                COMMAND_EXIT_CODE=0
            fi   
            export TESTCASENAME="Streaming"
            displayTestCaseResult $TESTCASE_ID

            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for file in $FILES ; do
            for fs in $FILE_SYSTEM ; do

                createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
                createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

                cacheInCommand=$cache
                if [[ "$fs" == "file://" ]] ; then
                    if [[ "$cache" == "/tmp/Streaming" ]] ; then
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"
                        chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    else
                        echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                        chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                        cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                    fi
                else
                    putLocalToHdfs  $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/$file $cacheInCommand/$file >> $ARTIFACTS_FILE 2>&1
                fi

                setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option without specifying symlink for file on $fs in $cacheInCommand for $file "
                putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
                CMD=""
                CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                    -Dmapreduce.job.maps=1  \
                    -Dmapreduce.job.reduces=1 \
                    -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                    $YARN_OPTIONS \
                    -files "${fs}${cacheInCommand}/${file}" -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  -mapper 'xargs cat'  \
                    -reducer "cat" \
                    -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

                echo "$CMD"
                eval $CMD
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                export TESTCASENAME="Streaming"
                displayTestCaseResult $TESTCASE_ID

                ((TESTCASE_ID=$TESTCASE_ID+10))
            done
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do
            archive=nonExistentInput

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            cacheInCommand=$cache    
            if [[ "$fs" == "file://" ]] ; then      
                if [[ "$cache" == "/tmp/Streaming" ]] ; then        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"        
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/      
                else        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi
            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option for non existent file/directory on $fs in $cacheInCommand"    
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -files  "${fs}${cacheInCommand}/${archive}" \
                -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  \
                -mapper 'xargs cat'  \
                -reducer "cat" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

            echo "$CMD"
            output=`eval $CMD`

            (IFS='';echo $output)
            tmpOutput=$(echo $output | grep 'java.io.FileNotFoundException')
            EXIT_CODE=$?
            echo "GREPPING FOR java.io.FileNotFoundException and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
            if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then 
                COMMAND_EXIT_CODE=1
                REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
            else 
                COMMAND_EXIT_CODE=0
            fi   
            export TESTCASENAME="Streaming"
            displayTestCaseResult $TESTCASE_ID   
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done
    
    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            archive=InputDir
            cacheInCommand=$cache    
            if [[ "$fs" == "file://" ]] ; then      
                if [[ "$cache" == "/tmp/Streaming" ]] ; then        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"        
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/      
                else        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi
            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option for symlinks with special characters such as ! @ $ & * ( ) - _ + = to file/directory on $fs in $cacheInCommand"    
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1

            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -files  "${fs}${cacheInCommand}/${archive}#testlink\'\!\@\$\&\*\(\)-_+=\'" -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  -mapper 'xargs cat'  \
                -reducer "cat" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

                echo "$CMD"
                eval $CMD
    #$CMD 2>&1
                validateOutput
                COMMAND_EXIT_CODE=$?
                (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))
                export TESTCASENAME="Streaming"
                displayTestCaseResult $TESTCASE_ID
                ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done

    for cache in $PUBLIC_PRIVATE_CACHE ; do
        for fs in $FILE_SYSTEM ; do

            createHdfsDir /tmp/Streaming/${TESTCASE_ID} >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /tmp/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1
            createHdfsDir /user/${USER_ID}/Streaming/streaming-${TESTCASE_ID}  >> $ARTIFACTS_FILE 2>&1

            archive=InputDir
            cacheInCommand=$cache    
            if [[ "$fs" == "file://" ]] ; then      
                if [[ "$cache" == "/tmp/Streaming" ]] ; then        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 755"        
                    chmod -R 775 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/      
                else        
                    echo "Changing the permissions of $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/ to 700"
                    chmod -R 700 $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/        
                    cacheInCommand=$JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/
                fi
            fi
            setTestCaseDesc "Streaming-$TESTCASE_ID - Test to check the -files option for symlinks with special characters such as  '# % ^' to file/directory on $fs in $cacheInCommand"    
            putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/streaming-$TESTCASE_ID/input.txt /tmp/Streaming/streaming-${TESTCASE_ID}/input.txt    >> $ARTIFACTS_FILE 2>&1
            CMD=""
            CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR  jar $HADOOP_STREAMING_JAR \
                -Dmapreduce.job.name="streamingTest-$TESTCASE_ID" \
                $YARN_OPTIONS \
                -files  "${fs}${cacheInCommand}/${archive}#testlink#%^" -input "/tmp/Streaming/streaming-${TESTCASE_ID}/input.txt"  -mapper 'xargs cat'  \
                -reducer "cat" \
                -output "/tmp/Streaming/streaming-$TESTCASE_ID/Output" 2>&1"

            echo "$CMD"
            output=`eval $CMD`

            (IFS='';echo $output)
            tmpOutput=$(echo $output | grep 'java.lang.IllegalArgumentException')
            EXIT_CODE=$?
            echo "GREPPING FOR java.lang.IllegalArgumentException and found $tmpOutput and EXIT_CODE is $EXIT_CODE"
            if [ -z "$tmpOutput" -o "$EXIT_CODE" != "0" ] ; then 
                COMMAND_EXIT_CODE=1
                REASONS="Command Exit Code = $EXIT_CODE and STDOUT => $tmpOutput"
            else 
                COMMAND_EXIT_CODE=0
            fi   
            export TESTCASENAME="Streaming"
            displayTestCaseResult $TESTCASE_ID
            (( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

            ((TESTCASE_ID=$TESTCASE_ID+10))
        done
    done
    return 0
}

##########################################
# Main function
###########################################

#validateCluster
export TESTCASENAME="Streaming"
(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $? ))
if [ "${SCRIPT_EXIT_CODE}" -ne 0 ]; then
    echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
    exit $SCRIPT_EXIT_CODE
fi

displayHeader "STARTING STREAMING TEST SUITE"

getFileSytem
CacheArchives
Archives
CacheFiles
Files

echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE
