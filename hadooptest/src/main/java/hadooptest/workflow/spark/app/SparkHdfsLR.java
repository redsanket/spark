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
    private String master = "yarn-standalone";

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
        String appPatternStr = " application identifier: (.*)$";
        Pattern appPattern = Pattern.compile(appPatternStr);

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
                if (appMatcher.find()) {
                    this.ID = appMatcher.group(1);
                    TestSession.logger.debug("JOB ID: " + this.ID);
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
    private String[] assembleCommand() {        
        String sparkBin = TestSession.conf.getProperty("SPARK_BIN");
        if ((sparkBin == null) || (sparkBin.isEmpty())) {
            TestSession.logger.error("Error SPARK_BIN is not set!");
        }
        return new String[] {  sparkBin,
                "org.apache.spark.deploy.yarn.Client",
                "--jar",  TestSession.conf.getProperty("SPARK_EXAMPLES_JAR"),
                "--class", "org.apache.spark.examples.SparkHdfsLR",
                "--args", this.master,
                "--args", this.lrDataFile,
                "--args", Integer.toString(this.iterations),
                "--num-workers", Integer.toString(this.numWorkers),
                "--worker-memory", this.workerMemory,
                "--worker-cores", Integer.toString(this.workerCores),
                "--queue", this.QUEUE};
    }
}
