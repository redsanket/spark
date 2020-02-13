package hadooptest.spark.integration;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.json.config.JsonPathConfig;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.gdm.regression.stackIntegration.lib.SystemCommand;
import hadooptest.spark.regression.TestSparkUI;

@Category(SerialTests.class)
public class TestSparkOozieIntegration extends TestSession {

    private static final String WORKFLOW = "workflow.xml";
    private static final String WORKFLOW_WITH_HIVE_CREDS = "workflowWithHiveCreds.xml";
    private static final String OOZIE_WORKFLOW_ROOT_HDFS = "oozie/apps";
    private static final String TMP_WORKSPACE = "/tmp/oozie/";
    private static final String OOZIE_COMMAND = "/home/y/var/yoozieclient/bin/oozie";
    private static final String OOZIE_ENV_EXPORT_COMMAND = "export OOZIE_SSL_ENABLE=true;export OOZIE_SSL_CLIENT_CERT=/home/y/conf/ygrid_cacert/certstore.jks";
    private static final String[] HADOOPQA_KINIT_COMMAND = {"kinit", "-k", "-t", "/homes/hadoopqa/hadoopqa.dev.headless.keytab", "hadoopqa@DEV.YGRID.YAHOO.COM"};
    private static final String HADOOPQA_KINIT_COMMAND_STR = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";
    private static String jobTrackerURL = null;
    private static String nameNodeURL = null;
    private static String oozieNodeURL = null;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        TestSession.exec.runProcBuilder(HADOOPQA_KINIT_COMMAND);
        jobTrackerURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "jobtracker", "8032").replace("http://", "");
        nameNodeURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "namenode", "8020").replace("http://", "hdfs://");
        oozieNodeURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "oozie", "4443").replace("http://", "https://");

        System.out.println("System.user = " + System.getProperty("user.name"));
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        // Clean up the directors in local FS and HDFS
        String[] filePaths = {OOZIE_WORKFLOW_ROOT_HDFS};
        Util.deleteFromHDFS(filePaths);
        FileUtils.deleteDirectory(new File(TMP_WORKSPACE));
    }

    //===================================== TESTS ==================================================

    @Test
    public void runOozieSparkScalaPi() throws Exception {
        OozieJobProperties jobProps = new OozieJobProperties(
            jobTrackerURL
            ,nameNodeURL
            ,"spark_latest"
            ,"yarn"
            ,"cluster"
            ,"oozieSparkPiScala"
            ,"hadooptest.spark.regression.SparkPi"
            ,"htf-common-1.0-SNAPSHOT-tests.jar"
            ,"--queue default"
            ,"1"
            ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    @Test
    public void runOozieSparkJavaPi() throws Exception {
        OozieJobProperties jobProps = new OozieJobProperties(
                jobTrackerURL
                ,nameNodeURL
                ,"spark_latest"
                ,"yarn"
                ,"cluster"
                ,"oozieSparkPiJava"
                ,"hadooptest.spark.regression.JavaSparkPi"
                ,"htf-common-1.0-SNAPSHOT-tests.jar"
                ,"--queue default"
                ,"1"
                ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    @Test
    public void runOozieSparkWordCountJava() throws Exception {
        String appName = "oozieSparkWordCountJava";
        // Copy input data to hdfs
        String inputDataDir =
                copyFileToHDFS("wordCountInputData.txt", null, ResourceType.AppData, appName);

        OozieJobProperties jobProps = new OozieJobProperties(
                jobTrackerURL
                ,nameNodeURL
                ,"spark_latest"
                ,"yarn"
                ,"cluster"
                ,appName
                ,"hadooptest.spark.regression.JavaWordCount"
                ,"htf-common-1.0-SNAPSHOT-tests.jar"
                ,"--queue default"
                ,inputDataDir
                ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    @Test
    public void runOozieSparkPiPython() throws Exception {
        String appName = "oozieSparkPiPython";
        copyFileToHDFS("pi.py", null, ResourceType.AppLib, appName);
        OozieJobProperties jobProps = new OozieJobProperties(
                jobTrackerURL
                ,nameNodeURL
                ,"spark_latest"
                ,"yarn"
                ,"cluster"
                ,appName
                ,"pi.py"
                ,"pi.py"
                ,"--queue default"
                ,"1"
                ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    @Test
    public void runOozieSparkPiPython27() throws Exception {
        String appName = "oozieSparkPiPython27";
        copyFileToHDFS("pi.py", null, ResourceType.AppLib, appName);
        OozieJobProperties jobProps = new OozieJobProperties(
                jobTrackerURL
                ,nameNodeURL
                ,"spark_latest"
                ,"yarn"
                ,"cluster"
                ,appName
                ,"pi.py"
                ,"pi.py"
                ,"--queue default --conf spark.yarn.pythonZip=hdfs:///sharelib/v1/python27/python27.tgz"
                ,"1"
                ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    @Test
    public void runOozieSparkPiPythonApacheConfigs() throws Exception {
        String appName = "oozieSparkPiPythonApacheConfigs";
        copyFileToHDFS("pi.py", null, ResourceType.AppLib, appName);
        OozieJobProperties jobProps = new OozieJobProperties(
                jobTrackerURL
                ,nameNodeURL
                ,"spark_latest"
                ,"yarn"
                ,"cluster"
                ,appName
                ,"pi.py"
                ,"pi.py"
                ,"--queue default --archives hdfs:///sharelib/v1/python36/python36.tgz#python36 --conf spark.pyspark.python=./python36/bin/python3.6 --conf spark.pyspark.driver.python=./python36/bin/python3.6 --conf spark.executorEnv.LD_LIBRARY_PATH=./python36/lib --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./python36/lib"
                ,"1"
                ,false
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
    }

    // Note: Uncomment the test after hive deployment gets fixed - refer YSPARK-575.
//    @Test
//    public void runOozieSparkHiveScala() throws Exception {
//        OozieJobProperties jobProps = new OozieJobProperties(
//                jobTrackerURL
//                ,nameNodeURL
//                ,"spark_latest"
//                ,"yarn"
//                ,"cluster"
//                ,"oozieSparkClusterHive"
//                ,"hadooptest.spark.regression.SparkClusterHive"
//                ,"htf-common-1.0-SNAPSHOT-tests.jar"
//                ,"--queue default --conf spark.yarn.security.tokens.hive.enabled=false"
//                ,"1"
//                ,true
//        );
//
//        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
//        assertTrue("Running " + jobProps.appName + " failed running through oozie!", jobSuccessful);
//    }

    //================================ HELPER UTILITIES ============================================

    private void createOozieJobPropertiesFile (OozieJobProperties jobProps) throws Exception {
        new File(TMP_WORKSPACE + jobProps.appName).mkdirs();
        PrintWriter writer = new PrintWriter(TMP_WORKSPACE+ jobProps.appName + "/job.properties", "UTF-8");
        writer.println("jobTracker=" + jobProps.jobTracker);
        writer.println("nameNode=" + jobProps.nameNode);
        writer.println("sharelibAction=" + jobProps.sharelibAction);
        writer.println("master=" + jobProps.master);
        writer.println("deployMode=" + jobProps.deployMode);
        writer.println("appName=" + jobProps.appName);
        writer.println("appClass=" + jobProps.appClass);
        writer.println("appJar=" + jobProps.appJar);
        writer.println("sparkOpts=" + jobProps.sparkOpts);
        writer.println("sparkJobArgs=" + jobProps.sparkJobArgs);
        writer.println("wfRoot=" + OOZIE_WORKFLOW_ROOT_HDFS);
        writer.println("oozie.libpath=/user/${user.name}/${wfRoot}/" + jobProps.appName+"/lib/");
        writer.println("oozie.wf.application.path=/user/${user.name}/${wfRoot}/" + jobProps.appName+"/");
        writer.close();
    }

    private void createAndSetupOozieAppDir(OozieJobProperties jobProps) throws Exception {

        // Copy the workflow file
        String resourceName = jobProps.requireHiveCreds ? WORKFLOW_WITH_HIVE_CREDS : WORKFLOW;
        copyFileToHDFS(resourceName, WORKFLOW, ResourceType.Workflow, jobProps.appName);
        // Copy the jar file
        copyFileToHDFS(null, null, ResourceType.AppLib, jobProps.appName);
    }

    private String copyFileToHDFS(String srcRsrcName, String dstRsrcName, ResourceType rsrcType,
            String appName) throws Exception {
        String src = null;
        // For java/scala the source is JAR. For python its available under data.
        if (rsrcType == ResourceType.AppLib && srcRsrcName == null) {
            src = Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
        } else {
            src = Util.getResourceFullPath("resources/spark/data/" + srcRsrcName);
        }

        String dst = OOZIE_WORKFLOW_ROOT_HDFS + "/" + appName;
        if (rsrcType == ResourceType.AppLib) {
            dst += "/lib/";
        } else if (rsrcType == ResourceType.AppData) {
            dst += "/data/";
        }
        return Util.copyFileToHDFS(src, dst, dstRsrcName);
    }

    private boolean runOozieJobAndGetResult(OozieJobProperties jobProps) throws Exception {
        // create job.properties
        createOozieJobPropertiesFile(jobProps);
        // copy workflow.xml & app.jar
        createAndSetupOozieAppDir(jobProps);
        // run the oozie job & get status
        Map<String, String> newEnv = new HashMap<String, String>();
        newEnv.put("OOZIE_SSL_ENABLE", "true");
        newEnv.put("OOZIE_SSL_CLIENT_CERT", "/home/y/conf/ygrid_cacert/certstore.jks");
        String[] temp = TestSession.exec.runProcBuilder(
            new String[]{OOZIE_COMMAND, "job", "-run", "-config",
                TMP_WORKSPACE + jobProps.appName + "/job.properties",
                "-oozie", oozieNodeURL + "/oozie/", "-auth", "kerberos"
            }, newEnv);
        // The first entry is the result status of the command. The following entry is the value.
        String tempOozieJobID = temp[1];
        System.out.println("Oozie Job ID: " + tempOozieJobID);
        boolean oozieResult = false;
        if (tempOozieJobID == null || tempOozieJobID.indexOf("Error") > -1) {
            oozieResult=false;
        } else {
            String jobId = tempOozieJobID.substring(tempOozieJobID.indexOf(":") + 1, tempOozieJobID.length()).trim();
            String jobResult = getResult(jobId);
            if (jobResult.indexOf("KILLED") > -1) {
                oozieResult=false;
            } else if (jobResult.indexOf("SUCCEEDED") > -1) {
                oozieResult=true;
            } else {
                oozieResult=false;
            }
        }
        return oozieResult;
    }

    private String executeCommand(String command ) {
        String output = null;
        TestSession.logger.info("command - " + command);
        ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
        if ((result == null) || (result.getLeft() != 0)) {
            if (result != null) {
                // save script output to log
                TestSession.logger.info("Command exit value: " + result.getLeft());
                TestSession.logger.info(result.getRight());
            } else {
                TestSession.logger.error("Failed to execute " + command);
                //this.setErrorMessage(result.getLeft());
                return null;
            }
        } else {
            output = result.getRight();
            TestSession.logger.debug("log = " + output);
        }
        return output;
    }

    private String getJSONResponse(String stringUrl) {
        String curlCommand = "curl --insecure -sb -H \"Accept: application/json\" --negotiate -u : --cacert /home/y/conf/ygrid_cacert/ca-cert.pem " + stringUrl;
        String cmd = OOZIE_ENV_EXPORT_COMMAND + ";" + HADOOPQA_KINIT_COMMAND_STR + ";" + curlCommand;
        String output = executeCommand(cmd);
        return output;
    }

    private JSONObject pollOozieJobResult(String oozieJobID) {
        String query = oozieNodeURL + "/oozie/v1/job/" + oozieJobID;
        String responseString = getJSONResponse(query);
        JSONObject jsonPath = (JSONObject) JSONSerializer.toJSON(responseString);
        return jsonPath;
    }

    private String getResult(String oozieJobID) {
        String status =  null;

        String query = oozieNodeURL + "/oozie/v1/job/" + oozieJobID;
        TestSession.logger.info("oozie query = " + query);
        String responseString = getJSONResponse(query);
        JSONObject jsonPath = (JSONObject) JSONSerializer.toJSON(responseString);
        if (jsonPath != null) {
            status = jsonPath.getString("status");

            while (status.indexOf("RUNNING") > -1) {
                jsonPath = pollOozieJobResult(oozieJobID);
                status = getResultStatus(jsonPath);
                if (status != null) {
                    status = jsonPath.getString("status");
                    if (! (status.indexOf("RUNNING") > -1)   ) {
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return status;
    }

    private String getResultStatus(JSONObject responseObject) {
        String status = null;
        if (responseObject.containsKey("actions") ){
            JSONArray actionJsonArray = responseObject.getJSONArray("actions");
            if (actionJsonArray.size() > 0) {
                for ( int i = 0; i < actionJsonArray.size() - 1 ; i ++) {
                    JSONObject actionItem = actionJsonArray.getJSONObject(i);
                    if (actionItem.containsKey("transition")) {
                        String transitionName = actionItem.getString("transition");
                        status = actionItem.getString("status");
                    }
                }
            }
        }
        return status;
    }
}


class OozieJobProperties {
    public final String jobTracker;
    public final String nameNode;
    public final String sharelibAction;
    public final String master;
    public final String deployMode;
    public final String appName;
    public final String appClass;
    public final String appJar;
    public final String sparkOpts;
    public final String sparkJobArgs;
    public final boolean requireHiveCreds;

    public OozieJobProperties(
        String jobTracker
        , String nameNode
        , String sharelibAction
        , String master
        , String deployMode
        , String appName
        , String appClass
        , String appJar
        , String sparkOpts
        , String sparkJobArgs
        , boolean requireHiveCreds
    ) {
        this.jobTracker = jobTracker;
        this.nameNode = nameNode;
        this.sharelibAction = sharelibAction;
        this.master = master;
        this.deployMode = deployMode;
        this.appName = appName;
        this.appClass = appClass;
        this.appJar = appJar;
        this.sparkOpts = sparkOpts;
        this.sparkJobArgs = sparkJobArgs;
        this.requireHiveCreds = requireHiveCreds;
    }
}

enum ResourceType {
    Workflow, WorkflowWithHiveCreds, AppLib, AppData
}
