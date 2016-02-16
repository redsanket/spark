package gdm.regression;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import hadooptest.TestSession;

public class HCatDataHandle {
    private static String scriptsDirectory;
    private static String command[];
    static{
        scriptsDirectory = System.getProperty("user.dir") + "/src/test/java/hadooptest/gdm/regression/scripts/";
        
    }
    static String createTable(String clusterName){
        command = new String [4];
        command[0] = scriptsDirectory + "HCatDataDriver.sh";
        command[1] = scriptsDirectory;
        command[2] = clusterName;
        Date date = new Date();
        String tableSuffix = String.valueOf(date.getTime());
        String tableName = "HTFTest_" + tableSuffix;
        command[4] = tableName;
        command[5] = "create";
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
        }
        stderrReader.close();
        stdoutReader.close();
        TestSession.logger.info("Exit status : " + exitStatus);
        TestSession.logger.info("Output from data creation scripts " + output);
        return tableName;
    }
}
