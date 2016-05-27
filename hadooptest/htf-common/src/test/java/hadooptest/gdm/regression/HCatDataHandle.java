package gdm.regression;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.TimeZone;

import hadooptest.TestSession;

public class HCatDataHandle {
    private static final String CREATE_COMMAND = "create";
    private static final String DOES_TABLE_EXIST_COMMAND = "table_exists";
    private static final String ADD_PARTITION_COMMAND = "add_partition";
    private static final String DOES_PARTITION_EXIST_COMMAND = "partition_exists";
    private static final String CREATE_TABLE_ONLY_COMMAND = "create_table_only";
    private static final String DRIVER_SCRIPT = "HCatDataDriver.sh";
    private static String scriptsDirectory;
    static{
        scriptsDirectory = System.getProperty("user.dir") + "/src/test/java/hadooptest/gdm/regression/scripts/";
        
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
        TestSession.logger.info("Creating table " + tableName + " on HCat for cluster " + clusterName);
        String[] command = new String [5];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        
        command[3] = tableName;
        command[4] = CREATE_COMMAND;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getErrorStream())));
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
        p.waitFor();
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
            TestSession.logger.info("Output from data creation script: " + output);
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }     
            stderrReader.close();
            stdoutReader.close();
            throw new Exception("Error while creating a new table: " + output);
        }
    }
    
    static void createTableOnly(String clusterName, String tableName) throws Exception{
        String[] command = new String [5];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        
        command[3] = tableName;
        command[4] = CREATE_TABLE_ONLY_COMMAND;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getErrorStream())));
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
        p.waitFor();
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
            TestSession.logger.info("Output from data creation script: " + output);
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }     
            stderrReader.close();
            stdoutReader.close();
            throw new Exception("Error while creating a new table: " + output);
        }
    }
    
    static boolean doesTableExist(String clusterName, String tableName) throws Exception{
        String[] command = new String[5];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        command[3] = tableName;
        command[4] = DOES_TABLE_EXIST_COMMAND;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        p.waitFor();
        int exitStatus = p.exitValue();
        if(exitStatus == 0){
            return true;
        }else{
            return false;
        }
    }
    
    static boolean doesPartitionExist(String clusterName, String tableName, String partitionValue) throws Exception{
        String[] command = new String[6];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        command[3] = tableName;
        command[4] = DOES_PARTITION_EXIST_COMMAND;
        command[5] = partitionValue;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        p.waitFor();
        int exitStatus = p.exitValue();
        if(exitStatus == 0){
            return true;
        }else{
            return false;
        }
    }
    
    static boolean addPartition(String clusterName, String tableName, String partitionValue) throws Exception{
        String[] command = new String[6];
        command[0] = scriptsDirectory + DRIVER_SCRIPT;
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        command[3] = tableName;
        command[4] = ADD_PARTITION_COMMAND;
        command[5] = partitionValue;
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getErrorStream())));
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
        p.waitFor();
        int exitStatus = p.exitValue();
        boolean successFlag = true;
        String line;
        String output="";
        if(exitStatus == 0){
            while((line = stdoutReader.readLine())!=null)
            {
                output+=line + "\n";
            }
            stderrReader.close();
            stdoutReader.close();
            TestSession.logger.info("Output from partition adding script: " + output);
            return successFlag;
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }
            stderrReader.close();
            stdoutReader.close();
            throw new Exception("Error while adding partitoin : " + output);
        }
        
    }
}
