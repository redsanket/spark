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
 * An instance of App that represents a SparkPi.
 */
public class SparkPi extends App {

    /** master */
    private AppMaster master = AppMaster.YARN_STANDALONE; 

    /** slices for the Pi */
    private int slices = 2;

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
    
    /** queue name */
    private String queueName = this.QUEUE;

    /**
     * Set the queue name
     * 
     * @param String of the name of the queue
     */
    public void setQueueName(String name) { 
        this.queueName = name;
    }
    
    /**
     * Set the master
     * 
     * @param master 
     */
    public void setMaster(AppMaster master) { 
        this.master = master;
    }

    /**
     * Set the # of slices
     * 
     * @param # of slices
     */
    public void setSlices(int slices) {
        this.slices = slices;
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
     * Submit the app.  This should be done only by the Job.start() as Job should
     * remain threaded.
     * 
     * @throws Exception if there is a fatal error running the process to submit the app.
     */
    protected void submit() throws Exception {
        String appPatternStr = " Submitted application (.*)";

        Pattern appPattern = Pattern.compile(appPatternStr);

        String errorPatternStr = "ERROR (.*)Client: (.*)$";
        Pattern errorPattern = Pattern.compile(errorPatternStr);
        String errorPatternStr2 = "Usage: (.*)Client (.*)$";
        Pattern errorPattern2 = Pattern.compile(errorPatternStr2);

        // setup spark env
        Map<String, String> newEnv = new HashMap<String, String>();
        String sparkJar = TestSession.conf.getProperty("SPARK_JAR");
        String sparkExamplesJar = TestSession.conf.getProperty("SPARK_EXAMPLES_JAR");
        String sparkJavaOpts = TestSession.conf.getProperty("SPARK_JAVA_OPTS");

        if ((sparkJar == null) || (sparkJar.isEmpty()) ||
           (sparkExamplesJar == null) || (sparkExamplesJar.isEmpty())) {
            TestSession.logger.error("Error SPARK_JAR or SPARK_EXAMPLES_JAR is not set!");
            return;
        }
        if (setSparkJar) {
            newEnv.put("SPARK_JAR", sparkJar);
        }
        newEnv.put("JAVA_HOME", TestSession.conf.getProperty("JAVA_HOME"));
        newEnv.put("SPARK_HOME",  TestSession.conf.getProperty("SPARK_HOME"));
        
        TestSession.logger.info("SPARK_JAR=" + sparkJar);
        TestSession.logger.info("JAVA_HOME=" + TestSession.conf.getProperty("JAVA_HOME"));

        // for yarn-client mode
        newEnv.put("SPARK_YARN_APP_JAR", TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"));
        newEnv.put("SPARK_WORKER_INSTANCES", Integer.toString(this.numWorkers));
        newEnv.put("SPARK_WORKER_MEMORY", this.workerMemory);
        newEnv.put("SPARK_WORKER_CORES", Integer.toString(this.workerCores));
        newEnv.put("SPARK_YARN_QUEUE", this.queueName);
        newEnv.put("HADOOP_CONF_DIR", TestSession.cluster.getConf().getHadoopConfDir());

        if (shouldPassName) { 
            newEnv.put("SPARK_YARN_APP_NAME", this.appName);
            TestSession.logger.info("SPARK_YARN_APP_NAME=" + this.appName);
        }

        TestSession.logger.info("SPARK_YARN_APP_JAR=" + TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"));
        TestSession.logger.info("SPARK_WORKER_INSTANCES=" + Integer.toString(this.numWorkers));
        TestSession.logger.info("SPARK_WORKER_MEMORY=" + this.workerMemory);
        TestSession.logger.info("SPARK_WORKER_CORES=" + Integer.toString(this.workerCores));
        TestSession.logger.info("SPARK_YARN_QUEUE=" + this.queueName);
        TestSession.logger.info("HADOOP_CONF_DIR=" + TestSession.cluster.getConf().getHadoopConfDir()); 
        
        if (!((sparkJavaOpts == null) || (sparkJavaOpts.isEmpty()))) {
        	TestSession.logger.info("SPARK_JAVA_OPTS is being set to: " + sparkJavaOpts);
        	newEnv.put("SPARK_JAVA_OPTS", sparkJavaOpts);
        }
        
        try {

            //this.process = TestSession.exec.runProcBuilderSecurityGetProc(this.assembleCommand(), this.USER);
            this.process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(this.assembleCommand(), this.USER, newEnv);
            BufferedReader reader=new BufferedReader(new InputStreamReader(this.process.getInputStream())); 
            String line=reader.readLine(); 

            while(line!=null) 
            { 
                TestSession.logger.debug(line);

                Matcher appMatcher = appPattern.matcher(line);
                Matcher errorMatcher = errorPattern.matcher(line);
                Matcher errorMatcher2 = errorPattern2.matcher(line);

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

                if (errorMatcher2.find()) {
                    this.ERROR = errorMatcher2.group(2);
                    TestSession.logger.error("Usage statement found");
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
        String[] ret = null;     

        if (this.master == AppMaster.YARN_CLIENT) {
            String hadoopHome = TestSession.cluster.getConf().getHadoopProp("HADOOP_COMMON_HOME") + "/share/hadoop/";
            String jarPath = hadoopHome + "hdfs/lib/YahooDNSToSwitchMapping-*.jar";
            String[] output = TestSession.exec.runProcBuilder(new String[] {"bash", "-c", "ls " + jarPath});
            String yahooDNSjar = output[1].trim();
            TestSession.logger.info("YAHOO DNS JAR = " + yahooDNSjar);

            String classpath = TestSession.cluster.getConf().getHadoopConfDir()
                    + ":" + TestSession.conf.getProperty("SPARK_EXAMPLES_JAR")
                    + ":" + hadoopHome + "common/hadoop-gpl-compression.jar"
                    + ":" + yahooDNSjar
                    + ":" + TestSession.conf.getProperty("SPARK_JAR");

            ret = new String[] { "java",
                    "-Dspark.jars=" + TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"),
                    "-DSPARK_YARN_MODE=true",
                    "-Dspark.master=yarn-client",
                    "-cp",
                    classpath,
                    "org.apache.spark.examples.SparkPi"};
        }
        else if (this.master == AppMaster.YARN_STANDALONE) {

            String sparkBin = TestSession.conf.getProperty("SPARK_BIN");
            if ((sparkBin == null) || (sparkBin.isEmpty())) {
                TestSession.logger.error("Error SPARK_BIN is not set!");
            }
            ArrayList<String> cmd = new ArrayList<String>(16);
            cmd.add(sparkBin); 
            cmd.add("org.apache.spark.deploy.yarn.Client"); 

            if (shouldPassJar) {
                cmd.add("--jar");
                cmd.add(TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"));
            }

            if (shouldPassClass) { 
                cmd.add("--class");
                cmd.add("org.apache.spark.examples.SparkPi");
            }

            if (shouldPassName) { 
                cmd.add("--name");
                cmd.add(this.appName);
            }

            cmd.addAll(Arrays.asList( new String[] { 
                    "--num-workers", Integer.toString(this.numWorkers),
                    "--worker-memory", this.workerMemory,
                    "--master-memory", this.masterMemory,
                    "--worker-cores", Integer.toString(this.workerCores),
                    "--queue", this.queueName} ));

            ret = cmd.toArray(new String[0]);
        }

        return ret;
    } 
}
