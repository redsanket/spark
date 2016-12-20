package hadooptest.spark.integration;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.json.config.JsonPathConfig;

import java.io.File;
import java.io.PrintWriter;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

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
import hadooptest.spark.regression.TestSparkUI;

@Category(SerialTests.class)
public class TestSparkOozieJavaWordCount extends TestSession {

    private static final String WORKFLOW = "workflow.xml";
    private static final String OOZIE_WORKFLOW_ROOT_HDFS = "oozie/apps";
    private static final String TMP_WORKSPACE = "/tmp/oozie/";
    private static final String OOZIE_COMMAND = "/home/y/var/yoozieclient/bin/oozie";
    private static final String[] HADOOPQA_KINIT_COMMAND = {"kinit", "-k", "-t", "/homes/hadoopqa/hadoopqa.dev.headless.keytab", "hadoopqa@DEV.YGRID.YAHOO.COM"};
    private static String jobTrackerURL = null;
    private static String nameNodeURL = null;
    private static String oozieNodeURL = null;
    private static String cookie = null;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        TestSession.exec.runProcBuilder(HADOOPQA_KINIT_COMMAND);
        jobTrackerURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "jobtracker", "8032").replace("http://","");
        nameNodeURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "namenode", "8020").replace("http://","");
        oozieNodeURL = TestSession.getCompURL(TestSession.cluster.getClusterName(), "oozie", "4080");

        System.out.println("System.user = " + System.getProperty("user.name"));
        TestSparkUI sparkUI = new TestSparkUI();
        String user = sparkUI.getBouncerUser();
        String pw = sparkUI.getBouncerPassword();
        System.out.println("Bouncer user = " + user);
        System.out.println();
        HTTPHandle client = new HTTPHandle();
        client.logonToBouncer(user,pw);
        logger.info("Cookie = " + client.YBYCookie);
        System.out.println("Obtained YBYCookie for the user!");
        cookie = client.YBYCookie;
    }

    // Clean up the directors in local FS and HDFS
    @AfterClass
    public static void endTestSession() throws Exception {
        TestSession.cluster.getFS().delete(new Path(OOZIE_WORKFLOW_ROOT_HDFS), true);
    }

    private void createOozieJobPropertiesFile (OozieJobProperties jobProps) throws Exception {
        new File(TMP_WORKSPACE + jobProps.appName).mkdirs();
        PrintWriter writer = new PrintWriter(TMP_WORKSPACE+ jobProps.appName + "/job.properties", "UTF-8");
        writer.println("jobTracker="+jobProps.jobTracker);
        writer.println("nameNode="+jobProps.nameNode);
        writer.println("sharelibAction="+jobProps.sharelibAction);
        writer.println("master="+jobProps.master);
        writer.println("deployMode="+jobProps.deployMode);
        writer.println("appName="+jobProps.appName);
        writer.println("appClass="+jobProps.appClass);
        writer.println("appJar="+jobProps.appJar);
        writer.println("sparkOpts="+jobProps.sparkOpts);
        writer.println("sparkJobArgs="+jobProps.sparkJobArgs);
        writer.println("wfRoot="+OOZIE_WORKFLOW_ROOT_HDFS);
        writer.println("oozie.libpath=/user/${user.name}/${wfRoot}/"+jobProps.appName+"/lib/");
        writer.println("oozie.wf.application.path=/user/${user.name}/${wfRoot}/"+jobProps.appName+"/");
        writer.close();
    }

    private void createAndSetupOozieAppDir(String appName) throws Exception {
        FileSystem fs = TestSession.cluster.getFS();
        String workflowFile = Util.getResourceFullPath("resources/spark/data/"+WORKFLOW);
        String appJar = Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");

        String oozieHdfsAppDir = OOZIE_WORKFLOW_ROOT_HDFS + "/" + appName;
        String oozieHdfsLibDir = OOZIE_WORKFLOW_ROOT_HDFS + "/" + appName + "/lib/";
        fs.mkdirs(new Path(oozieHdfsLibDir));

        // Copy the workflow file
        fs.copyFromLocalFile(new Path(workflowFile), new Path(oozieHdfsAppDir));
        // Copy the jar file
        fs.copyFromLocalFile(new Path(appJar), new Path(oozieHdfsLibDir));
        fs.createNewFile(new Path("myjob.properties"));
    }

    @Test
    public void runOozieSparkJavaWordCount() throws Exception {
        OozieJobProperties jobProps = new OozieJobProperties(
            jobTrackerURL
            ,"hdfs://"+nameNodeURL
            ,"spark_latest"
            ,"yarn"
            ,"cluster"
            ,"oozieScalaSparkPi"
            ,"hadooptest.spark.regression.SparkPi"
            ,"htf-common-1.0-SNAPSHOT-tests.jar"
            ,"--queue=default"
            ,"1"
        );

        boolean jobSuccessful = runOozieJobAndGetResult(jobProps);
        assertTrue("Running "+jobProps.appName +" failed running through oozie!", jobSuccessful);
    }

    private boolean runOozieJobAndGetResult(OozieJobProperties jobProps) throws Exception {
        // create job.properties
        createOozieJobPropertiesFile(jobProps);
        // copy workflow.xml & app.jar
        createAndSetupOozieAppDir(jobProps.appName);
        // run the oozie job & get status
        String[] temp = TestSession.exec.runProcBuilder(new String[]{OOZIE_COMMAND, "job", "-run", "-config", TMP_WORKSPACE + jobProps.appName +"/job.properties", "-oozie", oozieNodeURL +"/oozie/", "-auth", "kerberos"});
        // The first entry is the result status of the command. The following entry is the value.
        String tempOozieJobID = temp[1];
        System.out.println("Oozie Job ID: " + tempOozieJobID);
        boolean oozieResult = false;
        if (tempOozieJobID == null || tempOozieJobID.indexOf("Error") > -1) {
            oozieResult=false;
        } else {
            int indexOfJobIdOutput = tempOozieJobID.indexOf("job:");
            String jobId = tempOozieJobID.substring(tempOozieJobID.indexOf(":") + 1, tempOozieJobID.length());
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

    private String getResult(String oozieJobID) {
        String status =  null;

        String query = oozieNodeURL + "/oozie/v1/job/" + oozieJobID;
        TestSession.logger.info("oozie query = " + query);
        com.jayway.restassured.response.Response response = given().contentType(ContentType.JSON).cookie(this.cookie).get(query);
        TestSession.logger.info("response.getStatusCode() = " + response.getStatusCode());
        if (response != null) {
            JsonPath jsonPath = response.jsonPath().using(new JsonPathConfig("UTF-8"));
            status = jsonPath.getString("status");

            while ( status.indexOf("RUNNING") > -1) {
                jsonPath = pollOozieJobResult(oozieJobID);
                String result = jsonPath.prettyPrint();
                TestSession.logger.info("result = " + result);
                JSONObject oozieJsonResult =  (JSONObject) JSONSerializer.toJSON(result.toString().trim());
                status = getResultStatus(oozieJsonResult);
                if (status != null) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    status = jsonPath.getString("status");
                    if (! (status.indexOf("RUNNING") > -1)   ) {
                        break;
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

    private JsonPath pollOozieJobResult(String oozieJobID) {
        String query = oozieNodeURL + "/oozie/v1/job/" + oozieJobID;
        com.jayway.restassured.response.Response response = given().contentType(ContentType.JSON).cookie(this.cookie).get(query);
        TestSession.logger.info("response.getStatusCode() = " + response.getStatusCode());
        JsonPath jsonPath = response.jsonPath().using(new JsonPathConfig("UTF-8"));
        return jsonPath;
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
    }
}

