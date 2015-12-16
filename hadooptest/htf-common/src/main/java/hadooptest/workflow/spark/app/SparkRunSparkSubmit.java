/*
 * YAHOO!
 */

package hadooptest.workflow.spark.app;

import hadooptest.automation.constants.HadooptestConstants;
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
 * An instance of App that represents a SparkRunSparkSubmit.
 */
public class SparkRunSparkSubmit extends App {

    /** master */
    private AppMaster master = AppMaster.YARN_STANDALONE;

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

    /** name of queue ot use */
    private String queueName = this.QUEUE;

    /** whether to pass --class to cmd */
    private Boolean shouldPassClass = true;

    /** whether to pass --jar to cmd */
    private Boolean shouldPassJar = true;

    /** whether to pass --name to cmd */
    private Boolean shouldPassName = false;

    /** whether to use jdk64 */
    private Boolean shouldUseJdk64 = false;

    /** application name */
    private String appName = "sparkTest";

    /** lo4j properties file */
    private String log4jFile = "";

    /** class name */
    private String className = "";

    /** jar name */
    private String jarName = TestSession.conf.getProperty("SPARK_EXAMPLES_JAR");

    /** distributed cache files */
    private String distCacheFiles = "";

    /** distributed cache archives */
    private String distCacheArchives = "";

    /** confs */
    private ArrayList<String> confsArray = new ArrayList<String>();

    /** args */
    private String[] argsArray;

    /** some examples do funky thing and we must tell it to use yarn mode */
    private String sparkYarnMode = "-DSPARK_YARN_MODE=true";

    /** driver options */
    private String driverJavaOptions = "";
    private String driverLibraryPath = "";
    private String driverClassPath = "";

    /** executor options */
    private String executorJavaOptions = "";
    private String executorLibraryPath = "";
    private String executorClassPath = "";

    /**
     * Set the master
     * 
     * @param master 
     */
    public void setMaster(AppMaster master) {
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
     * Set whether to use 64 bit jdk (defaults to use 32 bit)
     * 
     * @param val - Boolean indicating if should use 64 bit jdk
     */
    public void setShouldUseJdk64(Boolean useit) {
        this.shouldUseJdk64 = useit;
    }

    /**
     * Set the queue name to use
     * 
     * @param name -String name of the queue
     */
    public void setQueueName(String name) {
        this.queueName = name;
    }

    /**
     * Set a list of files to go into the distributed cache
     * 
     * @param files - String of comma separate files
     */
    public void setDistributedCacheFiles(String files) {
        this.distCacheFiles = files;
    }

    /**
     * Set the log4j properties file.
     * 
     * @param name the name of the file.
     */
    public void setLog4jFile(String name) {
        this.log4jFile = name;
    }

    /**
     * Set a list of archives to go into the distributed cache
     * 
     * @param archives - String of comma separate archives
     */
    public void setDistributedCacheArchives(String archives) {
        this.distCacheArchives= archives;
    }

    public void setDriverJavaOptions(String val) {
        driverJavaOptions = val;
    }

    public void setDriverLibraryPath(String val) {
        driverLibraryPath = val;
    }

    public void setDriverClassPath(String val) {
        driverClassPath = val;
    }

    public void setExecutorJavaOptions(String val) {
        executorJavaOptions = val;
    }

    public void setExecutorLibraryPath(String val) {
        executorLibraryPath = val;
    }

    public void setExecutorClassPath(String val) {
        executorClassPath = val;
    }

    public void setConf(String val) {
        confsArray.add(val);
    }

    /**
     * Submit the app.  This should be done only by the Job.start() as Job should
     * remain threaded.
     * 
     * @throws Exception if there is a fatal error running the process to submit the app.
     */
    protected void submit() throws Exception {
        String appPatternStr = " Submitted application (.*)"; 
        String exceptionPatternStr = "Exception in thread(.*)$";
        String errorPatternStr = "ERROR (.*)Client: (.*)$";
        Pattern appPattern = Pattern.compile(appPatternStr);
        Pattern errorPattern = Pattern.compile(errorPatternStr);
        Pattern exceptionPattern = Pattern.compile(exceptionPatternStr);

        // setup spark env
        Map<String, String> newEnv = new HashMap<String, String>();
        newEnv.put("SPARK_HOME",  TestSession.conf.getProperty("SPARK_HOME"));

        if (this.shouldUseJdk64) {
            newEnv.put("JAVA_HOME", HadooptestConstants.Location.JDK64);
        } else {
            newEnv.put("JAVA_HOME", HadooptestConstants.Location.JDK32);
        }

        TestSession.logger.info("JAVA_HOME=" + newEnv.get("JAVA_HOME"));
        newEnv.put("HADOOP_CONF_DIR", TestSession.cluster.getConf().getHadoopConfDir());
        
        try {

            this.process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(this.assembleCommand(), this.USER, newEnv);
            BufferedReader reader=new BufferedReader(new InputStreamReader(this.process.getInputStream())); 
            String line=reader.readLine(); 

            while(line!=null) 
            { 
                TestSession.logger.debug(line);

                Matcher appMatcher = appPattern.matcher(line);
                Matcher errorMatcher = errorPattern.matcher(line);
                Matcher exceptionMatcher = exceptionPattern.matcher(line);

                if (appMatcher.find()) {
                    this.ID = appMatcher.group(1);
                    TestSession.logger.debug("JOB ID: " + this.ID);
                    reader.close();
                    break;
                }

                if (errorMatcher.find()) {
                    this.ERROR = errorMatcher.group(2);
                    TestSession.logger.error("ERROR is: " + errorMatcher.group(2));
                    reader.close();
                    break;
                }

                if (exceptionMatcher.find()) {
                    this.ERROR = exceptionMatcher.group(1);
                    TestSession.logger.error("Exception is: " + errorMatcher.group(1));
                    reader.close();
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
        // wait for the process to finish and look at the exit code
        this.process.waitFor();
        if (this.process.exitValue() != 0) {
            TestSession.logger.error("Exit code is: " + process.exitValue());
            this.ERROR = "exit value is nonzero: " + process.exitValue();
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
    private String[] assembleCommand() throws Exception {        
        String sparkSubmitBin = TestSession.conf.getProperty("SPARK_SUBMIT_BIN");
        if ((sparkSubmitBin == null) || (sparkSubmitBin.isEmpty())) {
            TestSession.logger.error("Error SPARK_SUBMIT_BIN is not set!");
        }

        String hadoopHome = TestSession.cluster.getConf().getHadoopProp("HADOOP_COMMON_HOME") + "/share/hadoop/";
        String jarPath = hadoopHome + "hdfs/lib/YahooDNSToSwitchMapping-*.jar";
        String[] output = TestSession.exec.runProcBuilder(new String[] {"bash", "-c", "ls " + jarPath});
        String yahooDNSjar = output[1].trim();
        TestSession.logger.info("YAHOO DNS JAR = " + yahooDNSjar);
        String gplCompression = hadoopHome + "common/hadoop-gpl-compression.jar";

        ArrayList<String> cmd = new ArrayList<String>(20);
        cmd.add(sparkSubmitBin); 
        cmd.add("--master");
        cmd.add("yarn");
        cmd.add("--deploy-mode");
        if (this.master == AppMaster.YARN_CLIENT) {
            cmd.add("client");
        } else {
            cmd.add("cluster");
        }

        if (shouldPassClass) { 
            cmd.add("--class");
            cmd.add(this.className);
        }

        if (shouldPassName) { 
            cmd.add("--name");
            cmd.add(this.appName);
        }

        if (!this.log4jFile.isEmpty()) {
            cmd.add("--files");
            cmd.add(this.log4jFile);
        }

        if (!this.distCacheFiles.isEmpty()) { 
            cmd.add("--files");
            cmd.add(this.distCacheFiles);
        }

        if (!this.distCacheArchives.isEmpty()) { 
            cmd.add("--archives");
            cmd.add(this.distCacheArchives);
        }

        cmd.add("--driver-java-options");
        if (!this.driverJavaOptions.isEmpty()) {
            cmd.add(this.driverJavaOptions + " " + sparkYarnMode);
        } else {
            cmd.add(sparkYarnMode);
        }

        if (!this.driverLibraryPath.isEmpty()) {
            cmd.add("--driver-library-path");
            cmd.add(this.driverLibraryPath);
        }

        cmd.add("--driver-class-path");
        if (!this.driverClassPath.isEmpty()) {
            cmd.add(this.driverClassPath + ":" + yahooDNSjar + ":" + gplCompression);
        } else {
            cmd.add(yahooDNSjar + ":" + gplCompression);
        }

        if (!this.executorJavaOptions.isEmpty()) {
            cmd.add("--conf");
            cmd.add("spark.executor.extraJavaOptions=" + this.executorJavaOptions);
        }

        cmd.add("--conf");
        if (!this.executorClassPath.isEmpty()) {
            cmd.add("spark.executor.extraClassPath=" + 
                this.executorClassPath + ":" + yahooDNSjar + ":" + gplCompression);
        } else {
            cmd.add("spark.executor.extraClassPath=" + yahooDNSjar + ":" + gplCompression);
        }

        if (!this.executorLibraryPath.isEmpty()) {
            cmd.add("--conf");
            cmd.add("spark.executor.extraLibraryPath=" + this.executorLibraryPath);
        }

        for (String confString: confsArray) {
            cmd.add("--conf");
            cmd.add(confString);
        }

        if (this.shouldUseJdk64) {
            cmd.add("--conf");
            cmd.add("spark.executorEnv.JAVA_HOME=" + HadooptestConstants.Location.JDK64);
            cmd.add("--conf");
            cmd.add("spark.yarn.appMasterEnv.JAVA_HOME=" + HadooptestConstants.Location.JDK64);
        }

        cmd.addAll(Arrays.asList( new String[] { 
            "--num-executors", Integer.toString(this.numWorkers),
            "--executor-memory", this.workerMemory,
            "--driver-memory", this.masterMemory,
            "--executor-cores", Integer.toString(this.workerCores),
            "--queue", this.queueName} ));

        // jar and then args go last
        if (shouldPassJar) {
          cmd.add(this.jarName);
        }

        if (this.argsArray != null) {
            for (String arg: this.argsArray) {
                cmd.add(arg);
            }
        }
        String[] ret = cmd.toArray(new String[0]);
        TestSession.logger.info("command is: " + Arrays.toString(ret));
        return ret;
    }
}
