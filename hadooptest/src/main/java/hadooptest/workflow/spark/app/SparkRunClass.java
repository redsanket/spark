/*
 * YAHOO!
 */

package hadooptest.workflow.spark.app;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * An instance of App that represents a SparkRunClass.
 */
public class SparkRunClass extends App {

    /** master */
    private String master = "yarn-standalone";

    /** The memory to use for the AM */
    private String masterMemory = "1g";
    
    /** The memory to use for the map phase */
    private String workerMemory = "1g";

    /** The memory to use for the map phase */
    private int workerCores = 1;
    
    /** number of workers */
    private int numWorkers = 1;

    /** whether to set SPARK_JAR env var */
    private Boolean setSparkJar = true;

    /** whether to pass --class to cmd */
    private Boolean shouldPassClass = true;

    /** whether to pass --jar to cmd */
    private Boolean shouldPassJar = true;

    /** whether to pass --name to cmd */
    private Boolean shouldPassName = false;

    /** application name */
    private String appName = "sparkTest";

    /** class name */
    private String className = "";

    /** jar name */
    private String jarName = "";

    /** args */
    private String[] argsArray;
    
    /**
     * Set the master
     * 
     * @param master 
     */
    public void setMaster(String master) {
        this.master = master;
    }

    /**
     * Set the memory to be used by the mappers.
     * 
     * @param memory the memory to be used by the mappers.
     */
    public void setWorkerMemory(String memory) {
        this.workerMemory = memory;
    }

    /**
     * Set the memory to be used by the AM. 
     * 
     * @param memory the memory to be used by the AM.
     */
    public void setMasterMemory(String memory) {
        this.masterMemory = memory;
    }

    /**
     * Set the number of worker cores. 
     * 
     * @param cores, the number of cores.
     */
    public void setWorkerCores(int cores) {
        this.workerCores = cores;
    }

    /**
     * Set the number of workers.
     * 
     * @param workers, number of workers.
     */
    public void setNumWorkers(int workers) {
        this.numWorkers = workers;
    }

    /**
     * Set the application name.
     * 
     * @param name the name of the app.
     */
    public void setAppName(String name) {
        this.appName = name;
    }

    /**
     * Set whether the SPARK_JAR env var should be set
     * 
     * @param val - boolean
     */
    public void setShouldSetSparkJar(Boolean val) {
        this.setSparkJar = val;
    }
    
    /**
     * Set whether we should pass the --class option to cmd
     * 
     * @param val - boolean
     */
    public void setShouldPassClass(Boolean val) {
        this.shouldPassClass = val;
    }

    /**
     * Set whether we should pass the --jar option to cmd
     * 
     * @param val - boolean
     */
    public void setShouldPassJar(Boolean val) {
        this.shouldPassJar = val;
    }

    /**
     * Set whether we should pass the --name option to cmd
     * 
     * @param val - boolean
     */
    public void setShouldPassName(Boolean val) {
        this.shouldPassName= val;
    }

    /**
     * Set the class name
     * 
     * @param val - String of class name
     */
    public void setClassName(String val) {
        this.className= val;
    }

    /**
     * Set the jar name
     * 
     * @param val - String of jar name
     */
    public void setJarName(String val) {
        this.jarName= val;
    }

    /**
     * Set the args 
     * 
     * @param val - String array of args
     */
    public void setArgs(String[] arr) {
        this.argsArray = arr;
    }

    /**
     * Submit the app.  This should be done only by the Job.start() as Job should
     * remain threaded.
     * 
     * @throws Exception if there is a fatal error running the process to submit the app.
     */
    protected void submit() throws Exception {
        String appPatternStr = " application identifier: (.*)$";
        String errorPatternStr = "ERROR (.*)Client: (.*)$";
        Pattern appPattern = Pattern.compile(appPatternStr);
        Pattern errorPattern = Pattern.compile(errorPatternStr);

        // setup spark env
        Map<String, String> newEnv = new HashMap<String, String>();
        String sparkJar = TestSession.conf.getProperty("SPARK_JAR");
        //String sparkExamplesJar = TestSession.conf.getProperty("SPARK_EXAMPLES_JAR");
        String sparkJavaOpts = TestSession.conf.getProperty("SPARK_JAVA_OPTS");

        if ((sparkJar == null) || (sparkJar.isEmpty()) ||
           (this.jarName == null) || (this.jarName.isEmpty()) ||
           (this.className == null) || (this.className.isEmpty())) {
            TestSession.logger.error("Error SPARK_JAR or jar name or class name is not set!");
            return;
        }
        if (setSparkJar) {
            newEnv.put("SPARK_JAR", sparkJar);
        }
        newEnv.put("JAVA_HOME", TestSession.conf.getProperty("JAVA_HOME"));
        
        if (!((sparkJavaOpts == null) || (sparkJavaOpts.isEmpty()))) {
        	TestSession.logger.info("SPARK_JAVA_OPTS is being set to: " + sparkJavaOpts);
        	newEnv.put("SPARK_JAVA_OPTS", sparkJavaOpts);
        }
        
        try {

            this.process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(this.assembleCommand(), this.USER, newEnv);
            BufferedReader reader=new BufferedReader(new InputStreamReader(this.process.getInputStream())); 
            String line=reader.readLine(); 

            while(line!=null) 
            { 
                TestSession.logger.debug(line);

                Matcher appMatcher = appPattern.matcher(line);
                Matcher errorMatcher = errorPattern.matcher(line);

                if (appMatcher.find()) {
                    this.ID = appMatcher.group(1);
                    TestSession.logger.debug("JOB ID: " + this.ID);
                    break;
                }

                if (errorMatcher.find()) {
                    this.ERROR = errorMatcher.group(2);
                    TestSession.logger.error("ERROR is: " + errorMatcher.group(2));
                    break;
                }

                line=reader.readLine();
            } 
        }
        catch (Exception e) {
            if (this.process != null) {
                this.process.destroy();
            }
            
            TestSession.logger.error("Exception " + e.getMessage(), e);
            throw e;
        }
    } 

    /**
     * Submit the app and don't wait for the ID.  This should be done only by the Job.start() as Job should
     * remain threaded.
     * 
     * @throws Exception if there is a fatal error running the process to submit the app.
     */
    protected void submitNoID() throws Exception {
        try {
            this.process = TestSession.exec.runProcBuilderSecurityGetProc(this.assembleCommand(), this.USER);
        }
        catch (Exception e) {
            if (this.process != null) {
                this.process.destroy();
            }
            
            TestSession.logger.error("Exception " + e.getMessage(), e);
            throw e;
        }
    } 

    /**
     * Assemble the system command to launch the SparkPi app.
     * 
     * @return String[] the string array representation of the system command to launch the app.
     */
    private String[] assembleCommand() {        
        String sparkBin = TestSession.conf.getProperty("SPARK_BIN");
        if ((sparkBin == null) || (sparkBin.isEmpty())) {
            TestSession.logger.error("Error SPARK_BIN is not set!");
        }
        ArrayList<String> cmd = new ArrayList<String>(16);
        cmd.add(sparkBin); 
        cmd.add("org.apache.spark.deploy.yarn.Client"); 

        if (shouldPassJar) {
          cmd.add("--jar");
          cmd.add(this.jarName);
        }

        if (shouldPassClass) { 
          cmd.add("--class");
          cmd.add(this.className);
        }

        if (shouldPassName) { 
          cmd.add("--name");
          cmd.add(this.appName);
        }

        cmd.add("--args");
        cmd.add(this.master);
        for (String arg: this.argsArray) {
          cmd.add("--args");
          cmd.add(arg);
        }
     
        cmd.addAll(Arrays.asList( new String[] { 
                "--num-workers", Integer.toString(this.numWorkers),
                "--worker-memory", this.workerMemory,
                "--master-memory", this.masterMemory,
                "--worker-cores", Integer.toString(this.workerCores),
                "--queue", this.QUEUE} ));

        String[] retCmd = cmd.toArray(new String[0]);
        return retCmd;
    }
}
