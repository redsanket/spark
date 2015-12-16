/*
 * YAHOO!
 */

package hadooptest.workflow.spark.app;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * An instance of Job that represents a SparkHdsfLR app.
 */
public class SparkHdfsLR extends App {

    /** master */
    private AppMaster master = AppMaster.YARN_STANDALONE; 

    /** The memory to use for the map phase */
    private String workerMemory = "1g";

    /** The memory to use for the map phase */
    private int workerCores = 1;
    
    /** number of workers */
    private int numWorkers = 1;

    /** number of iterations */
    private int iterations = 10;

    /** lr data file */
    private String lrDataFile = "lr_data.txt";
 
    
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

    public void setWorkerCores(int cores) {
        this.workerCores = cores;
    }

    public void setNumWorkers(int workers) {
        this.numWorkers = workers;
    }

    public void setNumIterations(int iters) {
        this.iterations= iters;
    }

    public void setLRDataFile(String file) {
        this.lrDataFile = file;
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

        String errorPatternStr = "Exception in thread (.*): Input path does not exist: (.*)$";
        Pattern errorPattern = Pattern.compile(errorPatternStr);

        // setup spark env
        // TODO - commonize these across apps
        Map<String, String> newEnv = new HashMap<String, String>();
        String sparkJar = TestSession.conf.getProperty("SPARK_JAR");
        String sparkExamplesJar = TestSession.conf.getProperty("SPARK_EXAMPLES_JAR");
        String sparkJavaOpts = TestSession.conf.getProperty("SPARK_JAVA_OPTS");

        if ((sparkJar == null) || (sparkJar.isEmpty()) ||  (sparkExamplesJar == null) || (sparkExamplesJar.isEmpty())) {
            TestSession.logger.error("Error SPARK_JAR or SPARK_EXAMPLES_JAR is not set!");
        }
        newEnv.put("SPARK_JAR", sparkJar);
        newEnv.put("JAVA_HOME", TestSession.conf.getProperty("JAVA_HOME"));
        newEnv.put("SPARK_HOME",  TestSession.conf.getProperty("SPARK_HOME"));
        
        TestSession.logger.info("SPARK_JAR=" + sparkJar);
        TestSession.logger.info("JAVA_HOME=" + TestSession.conf.getProperty("JAVA_HOME"));

        // for yarn-client mode
        newEnv.put("SPARK_YARN_APP_JAR", TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"));
        newEnv.put("SPARK_WORKER_INSTANCES", Integer.toString(this.numWorkers));
        newEnv.put("SPARK_WORKER_MEMORY", this.workerMemory);
        newEnv.put("SPARK_WORKER_CORES", Integer.toString(this.workerCores));
        newEnv.put("SPARK_YARN_QUEUE", this.QUEUE);
        newEnv.put("HADOOP_CONF_DIR", TestSession.cluster.getConf().getHadoopConfDir());

        TestSession.logger.info("SPARK_YARN_APP_JAR=" + TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"));
        TestSession.logger.info("SPARK_WORKER_INSTANCES=" + Integer.toString(this.numWorkers));
        TestSession.logger.info("SPARK_WORKER_MEMORY=" + this.workerMemory);
        TestSession.logger.info("SPARK_WORKER_CORES=" + Integer.toString(this.workerCores));
        TestSession.logger.info("SPARK_YARN_QUEUE=" + this.QUEUE);
        TestSession.logger.info("HADOOP_CONF_DIR=" + TestSession.cluster.getConf().getHadoopConfDir()); 
        
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
                    reader.close();
                    break;
                }
                if (errorMatcher.find()) {
                    this.ERROR = errorMatcher.group(2);
                    TestSession.logger.error("ERROR is: " + errorMatcher.group(2));
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
     * Assemble the system command to launch the SparkHdfsLR app.
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
                    "org.apache.spark.examples.SparkHdfsLR",
                    this.lrDataFile,
                    Integer.toString(this.iterations) };
        }
        else if (this.master == AppMaster.YARN_STANDALONE) { 
            String sparkBin = TestSession.conf.getProperty("SPARK_BIN");
            if ((sparkBin == null) || (sparkBin.isEmpty())) {
                TestSession.logger.error("Error SPARK_BIN is not set!");
            }

            ret = new String[] {  sparkBin,
                    "org.apache.spark.deploy.yarn.Client",
                    "--jar",  TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"),
                    "--class", "org.apache.spark.examples.SparkHdfsLR",
                    "--args", this.lrDataFile,
                    "--args", Integer.toString(this.iterations),
                    "--num-workers", Integer.toString(this.numWorkers),
                    "--worker-memory", this.workerMemory,
                    "--worker-cores", Integer.toString(this.workerCores),
                    "--queue", this.QUEUE };
        }

        return ret;
    } 
}
