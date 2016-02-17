package gdm.regression;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.TimeZone;

import hadooptest.TestSession;

public class HCatDataHandle {
    private static final String CREATE_COMMAND = "create";
    private static final String DOES_TABLE_EXIST_COMMAND = "table_exists";
    private static final String ADD_PARTITION = "add_partition";
    private static String scriptsDirectory;
    static{
        scriptsDirectory = System.getProperty("user.dir") + "/src/test/java/hadooptest/gdm/regression/scripts/";
        
    }
    
    static String createTable(String clusterName, String tableName)throws Exception{
        String[] command = new String [5];
        command[0] = scriptsDirectory + "HCatDataDriver.sh";
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
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }
            tableName=null;
        }
        stderrReader.close();
        stdoutReader.close();
        TestSession.logger.info("Exit status : " + exitStatus);
        TestSession.logger.info("Output from data creation scripts " + output);
        return tableName;
    }
    
    static boolean doesTableExist(String clusterName, String tableName) throws Exception{
        String[] command = new String[5];
        command[0] = scriptsDirectory + "HCatDataDriver.sh";
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
    
    
    static boolean addPartition(String clusterName, String tableName, String partitionValue){
        String[] command = new String[6];
        command[0] = scriptsDirectory + "HCatDataDriver.sh";
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        command[3] = tableName;
        command[4] = ADD_PARTITION;
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
        }
        else{
            while((line = stderrReader.readLine())!=null){
                output+=line + "\n";
            }
            successFlag = false;
        }
        stderrReader.close();
        stdoutReader.close();
        TestSession.logger.info("Exit status : " + exitStatus);
        TestSession.logger.info("Output from data creation scripts " + output);
        return successFlag;
    }
}
