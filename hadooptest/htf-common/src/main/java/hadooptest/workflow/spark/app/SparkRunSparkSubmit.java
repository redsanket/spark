/*
 * YAHOO!
 */

package hadooptest.workflow.spark.app;

import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.TestSession;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import org.apache.commons.httpclient.HttpMethod;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;


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

    private Boolean shouldPassNumWorkers = true;

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

    /** application name */
    private String appName = "sparkTest";

    /** lo4j properties file */
    private String log4jFile = "";

    /** class name */
    private String className = "";

    private String sparkHome = getSparkHome();

    private String sparkSubmitBin = sparkHome + "/bin/spark-submit";

    /** jar name */
    private String jarName = sparkHome + "/lib/spark-examples.jar";

    private String sparkConfDir = System.getenv("SPARK_CONF_DIR");

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

    /** additional confs **/
    private Map<String, String> additionalConfs = new HashMap<String, String>();

    private String getSparkHome() {
        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome == null || sparkHome.isEmpty()) {
            sparkHome = TestSession.conf.getProperty("SPARK_HOME");
        }
        if (sparkHome == null || sparkHome.isEmpty()) {
            TestSession.logger.error("Error SPARK_HOME is not set! Set it in env or config file.");
        }
        return sparkHome;
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
     * Set whether we should pass --num-executors
     * @param val - boolean
     */
    public void setShouldPassNumWorkers(Boolean val) {
        this.shouldPassNumWorkers = val;
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
        newEnv.put("SPARK_HOME",  sparkHome);
        if (sparkConfDir != null && !sparkConfDir.isEmpty()) {
            newEnv.put("SPARK_CONF_DIR", sparkConfDir);
        }
        newEnv.put("JAVA_HOME", HadooptestConstants.Location.JDK64);

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

        if (!this.executorJavaOptions.isEmpty()) {
            cmd.add("--conf");
            cmd.add("spark.executor.extraJavaOptions=" + this.executorJavaOptions);
        }

        if (!this.executorLibraryPath.isEmpty()) {
            cmd.add("--conf");
            cmd.add("spark.executor.extraLibraryPath=" + this.executorLibraryPath);
        }

        if (sparkConfDir == null || sparkConfDir.isEmpty()) {
            String hadoopHome = TestSession.cluster.getConf().getHadoopProp("HADOOP_COMMON_HOME") + "/share/hadoop/";
            String jarPath = hadoopHome + "hdfs/lib/YahooDNSToSwitchMapping-*.jar";
            String[] output = TestSession.exec.runProcBuilder(new String[] {"bash", "-c", "ls " + jarPath});
            String yahooDNSjar = output[1].trim();
            TestSession.logger.info("YAHOO DNS JAR = " + yahooDNSjar);
            String gplCompression = hadoopHome + "common/hadoop-gpl-compression.jar";

            cmd.add("--driver-class-path");
            if (!this.driverClassPath.isEmpty()) {
                cmd.add(this.driverClassPath + ":" + yahooDNSjar + ":" + gplCompression);
            } else {
                cmd.add(yahooDNSjar + ":" + gplCompression);
            }

            cmd.add("--conf");
            if (!this.executorClassPath.isEmpty()) {
                cmd.add("spark.executor.extraClassPath=" +
                        this.executorClassPath + ":" + yahooDNSjar + ":" + gplCompression);
            } else {
                cmd.add("spark.executor.extraClassPath=" + yahooDNSjar + ":" + gplCompression);
            }
        }

        for (String confString: confsArray) {
            cmd.add("--conf");
            cmd.add(confString);
        }

        cmd.add("--conf");
        cmd.add("spark.executorEnv.JAVA_HOME=" + HadooptestConstants.Location.JDK64);
        cmd.add("--conf");
        cmd.add("spark.yarn.appMasterEnv.JAVA_HOME=" + HadooptestConstants.Location.JDK64);
        cmd.add("--conf");
        cmd.add("spark.sql.warehouse.dir=" + "file:${system:user.dir}/spark-warehouse");

        for(Map.Entry<String, String> confEntry : this.additionalConfs.entrySet()) {
            String confName = confEntry.getKey();
            String confValue = confEntry.getValue();
            cmd.add("--conf");
            cmd.add(confName + "=" + confValue);
        }

        cmd.addAll(Arrays.asList( new String[] {
            "--executor-memory", this.workerMemory,
            "--driver-memory", this.masterMemory,
            "--executor-cores", Integer.toString(this.workerCores),
            "--queue", this.queueName} ));

        if (shouldPassNumWorkers) {
            cmd.addAll(Arrays.asList( new String[] {
                    "--num-executors", Integer.toString(this.numWorkers)}));
        }

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

    /**
     * wait for the applications to come up with a certain number of executors
     */
    public boolean waitForExecutors(String appId, int numExecutors, String userId, String password, int retryAttempts) throws InterruptedException {
        for (int i = 0; i < retryAttempts; i++) {
            if (checkNumExecutors(appId, numExecutors, userId, password)) {
                return true;
            }
            TestSession.logger.info("Sleeping... retry attempt " + i);
            Util.sleep(1);
        }
        return false;
    }

    /**
     * check the number of executors
     */
    public boolean checkNumExecutors(String appId, int numExecutors, String userId, String password) {
        try {
            TestSession.logger.info("Checking executors for App: " + appId
                    + " with name " + this.getAppName()
                    + " with Status: " + this.getYarnState().toString()
                    + " Exepcted # of executors" + numExecutors);
            Integer actualNumExecutors = getNumberOfExecutors(appId, userId, password);
            TestSession.logger.info("Number of executors - Expected: " + numExecutors + " Actual: " + actualNumExecutors);
            return actualNumExecutors == numExecutors;
        } catch (Exception e) {
            TestSession.logger.info("Exception thrown in checkNumExecutors:" + e.toString() + "\n" + Arrays.asList(e.getStackTrace()));
            return false;
        }
    }

    /**
     * Get the number of executors running for an app
     */
    public int getNumberOfExecutors(String appId, String userId, String password) throws Exception {
        return getExecutors(appId, userId, password).size();
    }

    /**
     * Get a list of executors running on an app using the spark web service API
     */
    public List getExecutors(String appId, String userId, String password) throws Exception {

        String resourceManagerURL = TestSession.getResourceManagerURL(System.getProperty("CLUSTER_NAME"));
        String appName = this.getAppName();
        TestSession.logger.info("App Name: " + appName);
        String appNameForURL = appName.replace(" ", "%20");
        TestSession.logger.info("Resource manager url: " + resourceManagerURL);
        String urlString = resourceManagerURL + "/proxy/" + appId + "/api/v1/applications/" + appNameForURL + "/executors";
        TestSession.logger.info("REST API url to get executors: " + urlString);

        String jsonResponse = SparkRunSparkSubmit.getWithBouncer(userId, password, urlString, 200);

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(jsonResponse);

        JSONArray jsonArray = (JSONArray) obj;
        TestSession.logger.info("results: " + jsonArray);
        return jsonArray;
    }

    public static String getWithBouncer(String user, String pw, String url, int expectedCode) throws Exception {
        HTTPHandle client = new HTTPHandle();
        client.logonToBouncer(user, pw);
        TestSession.logger.info("URL to get is: " + url);
        HttpMethod getMethod = client.makeGET(url, new String(""), null);
        Response response = new Response(getMethod, false);
        if (expectedCode != response.getStatusCode()) {
            TestSession.logger.warn("!*** Status code for " + url + " does not match the expected value. Expected code: "
                    + expectedCode + " actual code: " + response.getStatusCode());
        }

        String output = response.getResponseBodyAsString();
        return output;
    }

    /**
     * Check if there is any active tasks
     */
    public boolean checkActiveTasks(String appId, String userId, String password) {
        List<Map<String, Object>> executors = null;
        try {
            executors = getExecutors(appId, userId, password);
        } catch (Exception e) {
            TestSession.logger.info("Exception thrown in checkActiveTasks:" + e.toString() + "\n" + Arrays.asList(e.getStackTrace()));
            return false;
        }
        for (Map<String, Object> executor : executors) {
            TestSession.logger.info("executor: " + executor);
            if (Integer.parseInt(executor.get("activeTasks").toString()) > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * wait until there are not active tasks running
     */
    public boolean waitForNoActiveTasks(String appId, String userId, String password, int retryAttempts) throws InterruptedException {
        for (int i = 0; i < retryAttempts; i++) {
            if (!checkActiveTasks(appId, userId, password)) {
                return true;
            }
            TestSession.logger.info("Sleeping... retry attempt " + i);
            Util.sleep(1);
        }
        return false;
    }

    /**
     * add additional confs to be add with starting a application
     */
    public void addConf(String conf, String value) {
        this.additionalConfs.put(conf, value);
    }
}
