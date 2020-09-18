#################################################################################
# Functions used for deploying hadoop
#################################################################################

banner() {
    echo "#################################################################################"
    echo "# `date -u +'%a %b %d %Y'` `date -u +'%I:%M:%S %p %Z |'` `TZ=CST6CDT date +'%I:%M:%S %p %Z |'` `TZ=PST8PDT date +'%I:%M:%S %p %Z'`"
    echo "# $*"
    echo "#################################################################################"
}

banner2() {
    echo "#################################################################################"
    echo "# `date -u +'%a %b %d %Y'` `date -u +'%I:%M:%S %p %Z |'` `TZ=CST6CDT date +'%I:%M:%S %p %Z |'` `TZ=PST8PDT date +'%I:%M:%S %p %Z'`"
    echo "# $1"
    echo "# $2"
    echo "#################################################################################"
}

note() {
    echo "# `TZ=PDT8PDT date '+%H:%M:%S %p %Z'` - $*"
}

#################################################################################
# CHECK IF WE NEED TO INSTALL STACK COMPONENTS
#
# gridci-1040, make component version selectable
# gridci-1300, use cluster names passed in from jenkins, to lookup component
#              versions from artifactory
#
# PIG - gridci-747 install pig on gw
#
# HIVE - gridci-481 install hive server and client
# this relies on hive service keytab being generated and pushed out in the
# cluster configure portion
# of cluster building (cluster-build/configure_cluster)
#
# gridci-1937, allow hive/hcat to install using current branch
#
# OOZIE - gridci-561 install yoozie server
# this relies on oozie service keytab being generated and pushed out in the
# cluster configure portion of cluster building (cluster-build/configure_cluster)
#################################################################################
function deploy_stack() {
    STACK_COMP=$1
    STACK_COMP_VERSION=$2
    STACK_COMP_SCRIPT=$3

    start=`date +%s`
    h_start=`date +%Y/%m/%d-%H:%M:%S`
    echo "INFO: Install stack component ${STACK_COMP} on $h_start"
    if [ "$STACK_COMP_VERSION" == "none" ]; then
        echo "INFO: Nothing to do since STACK_COMP_VERSION is set to 'none'"
        return 0
    else
        # check we got a valid reference cluster
        REFERENCE_CLUSTER=$STACK_COMP_VERSION
        if [[ "$STACK_COMP" == "pig" ]]; then
            packagename='pig_latest'
        elif [[ "$STACK_COMP" == "oozie" ]]; then
            packagename='yoozie'
        else
            packagename=${STACK_COMP}
        fi

        # gridci-1937 allow install from current branch
        if [[ "$STACK_COMP_VERSION" == "current" ]]; then
          REFERENCE_CLUSTER=current
        else
          RESULT=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER`
          if [ $? -eq 0 ]; then
              # get Artifactory URI and log it
              ARTI_URI=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER  -v | grep downloadUri |cut -d\' -f4`
              echo "Artifactory URI with most recent versions:"
              echo $ARTI_URI
              # look up stack component version for AR in artifactory
              set -x
              if [[ ${STACK_COMP} != "spark" ]]; then
                PACKAGE_VERSION=`/home/y/bin/query_releases -c $REFERENCE_CLUSTER -b ${STACK_COMP} -p $packagename`
              fi
              set +x
          else
              echo "ERROR: fetching reference cluster $REFERENCE_CLUSTER responded with: $RESULT"
              exit 1
          fi
        fi

        banner "START INSTALL STEP: Stack Component ${STACK_COMP}"
        set -x
        time ./$STACK_COMP_SCRIPT $CLUSTER $STACK_COMP_VERSION 2>&1 | tee $artifacts_dir/deploy_stack_${STACK_COMP}-${PACKAGE_VERSION}.log
        st=$?
        set +x
        if [ $st -ne 0 ]; then
            echo "ERROR: component install for ${STACK_COMP} failed!"
        fi
        banner "END INSTALL STEP: Stack Component ${STACK_COMP}: status='$st'"
    fi
    end=`date +%s`
    h_end=`date +%Y/%m/%d-%H:%M:%S`
    runtime=$((end-start))
    printf "%-124s : %.0f min (%.0f sec) : %s : %s : %s\n" $STACK_COMP_SCRIPT $(echo "scale=2;$runtime/60" | bc) $runtime $h_start $h_end $st >> $artifacts_dir/timeline.log
    cat $artifacts_dir/timeline.log
}

#################################################################################
# Spark installation is flexible. You can either specify a reference cluster or
# you can specify the version of spark and spark history server to be installed.
#################################################################################
function deploy_spark () {
    if [[ $STACK_COMP_VERSION_SPARK != "none" ]]; then
	# call the default deploy behavior.
	deploy_stack spark $STACK_COMP_VERSION_SPARK spark-install-check.sh
    else
	echo "INFO: Installing a individual spark version which is different from the gateway setup."
	start=`date +%s`
	h_start=`date +%Y/%m/%d-%H:%M:%S`
	STACK_COMP=spark
	echo "INFO: Install stack component ${STACK_COMP} on $h_start"
	banner "START INSTALL STEP: Stack Component ${STACK_COMP}"
	set -x
	time ./spark-install-check.sh $CLUSTER $STACK_COMP_VERSION_SPARK 2>&1 | tee $artifacts_dir/deploy_stack_${STACK_COMP}-${PACKAGE_VERSION}.log
	st=$?
	set +x
	if [ $st -ne 0 ]; then
            echo "ERROR: component install for ${STACK_COMP} failed!"
	fi
	banner "END INSTALL STEP: Stack Component ${STACK_COMP}: status='$st'"
    fi
}

#################################################################################
# Tez deployment
#################################################################################
function deploy_spark () {
    file=${base}/229-installsteps-installTez.sh
    banner running $file
    . "$file"
    st=$?
    echo "Running $file Status: $st"
    if [ "$EXIT_ON_ERROR" = "true" ]; then
        [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running '" $file "': Exit $st <<<<<<<<<<" && exit $st
    else
       [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running '" $file "' <<<<<<<<<<"
    fi
    ## After successful Tez installation, run wordcount for sanity test
    f=${base}/258-installsteps-runTezWordCount.sh
    banner running $file
    . "$file"
    st=$?
    echo "Running $file Status: $st"
    [ "$st" -ne 0 ] && echo ">>>>>>>> Error in running Tez Wordcount example '" $file "' <<<<<<<<<<"
    return $st
}

# Add to the build artifact handy references to the NN and RM webui
function generate_webui() {
    local webui_file=$1

    echo "Generate the webui references for the namenode and resourcemanager"
    echo "<Pre>" > $webui_file;
    namenode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode)"|head -1`;
    URL="https://$namenode:50504/dfshealth.html"
    echo "WEBUI: $cluster NN $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "NN" "-" "<a href=$URL>$URL</a>" >> $webui_file;

    rm=`yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"|tr -s '\n' ','|sed -e  's/,$//'`;
    URL="https://$rm:50505/cluster"
    echo "WEBUI: $cluster RM $URL"
    printf "%-12s %s %s %s\n" "$CLUSTER" "RM" "-" "<a href=$URL>$URL</a>"  >> $webui_file;

    # if hive was selected, add the hive node's thrift URI
    if [ "$STACK_COMP_VERSION_HIVE" != "none" ]; then
      hivenode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.hive)"|head -1`;
      URL="thrift://$hivenode:9080/"
      echo "WEBUI: $cluster Hive $URL"
      printf "%-12s %s %s %s\n" "$CLUSTER" "Hive" "-" "<a href=$URL>$URL</a>" >> $webui_file;
    fi

    # if oozie was selected, add the oozie node's GUI URI
    if [ "$STACK_COMP_VERSION_OOZIE" != "none" ]; then
      oozienode=`yinst range -ir "(@grid_re.clusters.$CLUSTER.oozie)"|head -1`;
      URL="https://$oozienode:4443/oozie"
      echo "WEBUI: $cluster Oozie $URL"
      printf "%-12s %s %s %s\n" "$CLUSTER" "Oozie" "-" "<a href=$URL>$URL</a>" >> $webui_file;
    fi
    echo "</Pre>" >> $webui_file;
}


