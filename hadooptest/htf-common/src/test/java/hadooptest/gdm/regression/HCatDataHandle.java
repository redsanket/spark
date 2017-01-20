package gdm.regression;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import hadooptest.TestSession;

public class HCatDataHandle {
    private static final String CREATE_COMMAND = "create";
    private static final String DOES_TABLE_EXIST_COMMAND = "table_exists";
    private static final String ADD_PARTITION_COMMAND = "add_partition";
    private static final String DOES_PARTITION_EXIST_COMMAND = "partition_exists";
    private static final String CREATE_TABLE_ONLY_COMMAND = "create_table_only";
    private static final String DRIVER_SCRIPT = "HCatDataDriver.sh";
    private static final String CREATE_AVRO_COMMAND = "create_avro";
    private static final String CREATE_OBSOLETE_AVRO_COMMAND = "create_obsolete_avro";
    private static final String IS_AVRO_SCHEMA_SET = "is_avro_schema_set";
    private static final String IS_AVRO_SCHEMA_CORRECT = "is_avro_schema_correct";
    private static final String CREATE_AVRO_WITHOUT_DATA_COMMAND = "create_avro_without_data";
    private static String scriptsDirectory;
    static{
        scriptsDirectory = System.getProperty("user.dir") + "/src/test/java/hadooptest/gdm/regression/scripts/";     
    }
    
    private static int runCommand(String clusterName, String tableName, String cmdArg,String partition) throws Exception{
        String[] command = new String [6];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        command[3] = tableName;
        command[4] = cmdArg;
        command[5] = partition;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getErrorStream())));
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
        
        if (!p.waitFor(5, TimeUnit.MINUTES)) {
            p.destroy();
            throw new Exception("Timed out running process");
        }
        
        int exitStatus = p.exitValue();
        String line;
        String output="";
        if(exitStatus == 0){
            while((line = stdoutReader.readLine())!=null)
            {
                output+=line + "\n";
            }
            stderrReader.close();
            stdoutReader.close();
            TestSession.logger.info("Output from shell script: " + output);
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }     
            stderrReader.close();
            stdoutReader.close();
            TestSession.logger.info("Error while running shell script: " + output);
        }
        return exitStatus;
    }
    
    
    /*
     * Creates an hcat table under gdm database on the cluster specified and adds
     * a partition to it. Also adds dummy data into this partition.
     * The partition is of type yyyyMMdd
     * 
     * @param clusterName - name of the cluster where the table is to be created
     * @param tableName -  name of the table to be created
     * @throws Exception
     */
    static void createTable(String clusterName, String tableName) throws Exception {
       runCommand(clusterName,tableName,CREATE_COMMAND,"");
    }
    
    static void createAvroTable(String clusterName,String tableName, String partition) throws Exception {
        runCommand(clusterName, tableName, CREATE_AVRO_COMMAND,partition);
    }
    
    static void createAvroTable(String clusterName, String tableName) throws Exception {
        createAvroTable(clusterName,tableName,"");
    }
    
    static void createObsoleteAvroTable(String clusterName, String tableName) throws Exception {
        createObsoleteAvroTable(clusterName,tableName,"");
    }
    
    static void createObsoleteAvroTable(String clusterName,String tableName, String partition) throws Exception {
        runCommand(clusterName, tableName, CREATE_OBSOLETE_AVRO_COMMAND,partition);
    }
    
    static void createAvroTableWithoutData(String clusterName, String tableName) throws Exception {
        runCommand(clusterName,tableName,CREATE_AVRO_WITHOUT_DATA_COMMAND,"");
    }
    
    static boolean isAvroSchemaSet(String clusterName, String tableName) throws Exception {
        int status = runCommand(clusterName,tableName,IS_AVRO_SCHEMA_SET,"");
        return (status == 0)? true:false;
    }
    
    static boolean isAvroSchemaCorrect(String sourceCluster, String tableName, String targetCluster)throws Exception {
        return (runCommand(sourceCluster,tableName,IS_AVRO_SCHEMA_CORRECT,targetCluster) == 0)? true:false; 
    }
    
    static void createTableOnly(String clusterName, String tableName) throws Exception{
        runCommand(clusterName, tableName, CREATE_TABLE_ONLY_COMMAND, "");
    }
    
    static boolean doesTableExist(String clusterName, String tableName) throws Exception{
        int status = runCommand(clusterName,tableName,DOES_TABLE_EXIST_COMMAND,"");
        return (status == 0)?true:false;
    }
    
    static boolean doesPartitionExist(String clusterName, String tableName, String partitionValue) throws Exception{
        int status = runCommand(clusterName,tableName,DOES_PARTITION_EXIST_COMMAND,partitionValue);
        return (status == 0)?true:false;
    }
    
    static boolean addPartition(String clusterName, String tableName, String partitionValue) throws Exception{
      int status = runCommand(clusterName,tableName,ADD_PARTITION_COMMAND,partitionValue);
      return (status == 0)?true:false;
    }
}
