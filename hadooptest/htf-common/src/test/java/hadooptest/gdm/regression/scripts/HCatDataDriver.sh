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
    ;;
  table_exists)
    echo  see if this table exists
    ;;
  partition_exists)
    echo see if the partition exists
    ;;
  add_partition)
    echo add parition
    ;;
esac
scp ${currentDir}/${scriptFile} ${hcatServer}:/tmp/
ssh ${hcatServer} "/tmp/${scriptFile} $tableName"

