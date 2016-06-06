currentDir=$1
clusterName=$2
tableName=$3
command=$4
hcatServer=`yinst range -ir "(@grid_re.clusters.${clusterName}.hive)"`
echo "hcatServer = " $hcatServer  "   clusterName =   " $clusterName
ssh ${hcatServer} "rm -r /tmp/gdm_hcat_test"
ssh ${hcatServer} "mkdir /tmp/gdm_hcat_test"
ssh ${hcatServer} "chmod -R 777 /tmp/gdm_hcat_test"
case $command in
  create)
    echo "Creating this hcat table : " $tableName "on server : " $hcatServer  
    scriptFile="createHCatData.sh"
    scp ${currentDir}/part-0000 ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName"
    ;;
  table_exists)
    scriptFile="doesTableExist.sh"
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName"
    exitCode=`echo $?`
    exit ${exitCode}
    ;;
  partition_exists)
    scriptFile="doesPartitionExist.sh"
    partitionValue=$5
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName $partitionValue"
    exitCode=`echo $?`
    exit ${exitCode}

    ;;
  add_partition)
    partitionValue=$5
    scriptFile="createPartition.sh"
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/part-0000 ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName $partitionValue"
    ;;
  create_table_only)
    echo "Creating this hcat table : " $tableName "on server : " $hcatServer  
    scriptFile="createTableOnly.sh"
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName"
    ;;
esac

