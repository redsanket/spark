currentDir=$1
clusterName=$2
tableName=$3
command=$4
hcatServer=`yinst range -ir "(@grid_re.clusters.${clusterName}.hive)"`
case $command in
  create)
    echo "Creating this hcat table : " $tableName "on server : " $hcatServer  
    scriptFile="createHCatData.sh"
    scp ${currentDir}/part-0000 ${hcatServer}:/tmp/
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/
    ssh ${hcatServer} "/tmp/${scriptFile} $tableName"
    ;;
  table_exists)
    scriptFile="doesTableExist.sh"
    scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/
    ssh ${hcatServer} "/tmp/${scriptFile} $tableName"
    exitCode=`echo $?`
    exit ${exitCode}
    ;;
  partition_exists)
    echo see if the partition exists
    ;;
  add_partition)
    echo add parition
    ;;
esac

