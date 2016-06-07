currentDir=$1
clusterName=$2
tableName=$3
command=$4
hcatServer=`yinst range -ir "(@grid_re.clusters.${clusterName}.hive)"`
ssh ${hcatServer} "rm -r /tmp/gdm_hcat_test"
ssh ${hcatServer} "mkdir /tmp/gdm_hcat_test"
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
  create_avro)
    echo "Creating this avro hcat table : " $tableName "on server : " $hcatServer  
    scriptFile="createAvroHCatData.sh"
    partitionValue=$5
    scp ${currentDir}/part-0000 ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/avroschema.avsc ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName y $partitionValue"
    ;;
  create_obsolete_avro)
    echo "Creating this obsolete avro hcat table : " $tableName "on server : " $hcatServer  
    scriptFile="createObsoleteAvroHCatData.sh"
    partitionValue=$5
    scp ${currentDir}/obsoletepart-0000 ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/obsoleteavroschema.avsc ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName y $partitionValue"
    ;;
  create_avro_without_data)
    echo "Creating this avro hcat table without data : " $tableName "on server : " $hcatServer  
    scriptFile="createAvroHCatData.sh"
    scp ${currentDir}/part-0000 ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/avroschema.avsc ${hcatServer}:/tmp/gdm_hcat_test
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName n"
    ;;
   is_avro_schema_set)
    scriptFile="isAvroSchemaSet.sh"
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName $clusterName"
    exitCode=`echo $?`
    exit ${exitCode}
    ;;
   is_avro_schema_correct)
    scriptFile="isAvroSchemaCorrect.sh"
    targetCluster=$5
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/gdm_hcat_test
    ssh ${hcatServer} "/tmp/gdm_hcat_test/${scriptFile} $tableName $targetCluster"
    exitCode=`echo $?`
    exit ${exitCode}
    ;;
esac

