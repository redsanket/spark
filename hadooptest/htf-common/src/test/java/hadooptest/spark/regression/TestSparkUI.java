package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.automation.utils.http.Response;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkHdfsLR;

import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.InterruptedException;
import java.io.IOException;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;


@Category(SerialTests.class)
public class TestSparkUI extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localDir = null;
    private static String lrDatafile = "lr_data.txt";
    private static String hdfsDir = "/user/" + System.getProperty("user.name") + "/";
    private static SparkRunSparkSubmit appUserDefault; 
    private static String hitusr_1_password = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_1"); 
    private static String hitusr_2_password = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_2"); 

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestDir();
        copyResMgrConfigAndRestartNodes();
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        removeTestDir();

        // reset cluster
        FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
                                .getCluster();

        TestSession.cluster.getConf().resetHadoopConfDir();
        fullyDistributedCluster.hadoopDaemon(Action.STOP,
                                HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
        fullyDistributedCluster.hadoopDaemon(Action.START,
        HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
        TestSession.cluster.getNodeNames(HadoopCluster.RESOURCE_MANAGER),
        TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));
    }

    @After
    public void killJob() throws Exception {
        if ((appUserDefault != null) && (appUserDefault.getYarnState() == YarnApplicationState.RUNNING)) {
            appUserDefault.killCLI();
        }
    }

    public static void setupTestDir() throws Exception {
        TestSession.cluster.getFS();
        String localFile = Util.getResourceFullPath("resources/spark/data/" + lrDatafile);
        System.out.println("LR data File is: " + localFile);
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
    }

    public static void removeTestDir() throws Exception {
        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + lrDatafile), true);
    }

    public static void copyResMgrConfigAndRestartNodes() throws Exception {
        String capacitySchedulerXml = "capacity-scheduler.xml";
        String replacementConfigFile = TestSession.conf .getProperty("WORKSPACE")
                        + "/htf-common/resources/spark/data/capacity-scheduler.xml";
        TestSession.logger.info("Copying over canned cap sched file localted @:"
                                        + replacementConfigFile);
        FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
                        .getCluster();

        // Backup config and replace file, for Resource Manager
        fullyDistributedCluster.getConf(
                        HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
        fullyDistributedCluster.getConf(
                        HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
                        .copyFileToConfDir(replacementConfigFile,
                                        capacitySchedulerXml);

        // Bounce node
        fullyDistributedCluster.hadoopDaemon(Action.STOP,
                        HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
        fullyDistributedCluster.hadoopDaemon(Action.START,
                        HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

        Thread.sleep(20000);
        // Leave safe-mode
        DfsCliCommands dfsCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        genericCliResponse = dfsCliCommands.dfsadmin(
                        DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
                        DfsTestsBaseClass.Report.NO, "get",
                        DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
                        0, DfsTestsBaseClass.ClearSpaceQuota.NO,
                        DfsTestsBaseClass.SetSpaceQuota.NO, 0,
                        DfsTestsBaseClass.PrintTopology.NO, null);
        genericCliResponse = dfsCliCommands.dfsadmin(
                        DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
                        DfsTestsBaseClass.Report.NO, "leave",
                        DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
                        0, DfsTestsBaseClass.ClearSpaceQuota.NO,
                        DfsTestsBaseClass.SetSpaceQuota.NO, 0,
                        DfsTestsBaseClass.PrintTopology.NO, null);

        Configuration conf = fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

        Iterator iter = conf.iterator();
        while (iter.hasNext()) {
                Entry<String, String> entry = (Entry<String, String>) iter.next();
                TestSession.logger.trace("Key:[" + entry.getKey() + "] Value["
                                + entry.getValue() + "]");
        }
    }

    /**
     * Get the bouncer user used for testing.
     *
     * @throws Exception if there is a problem getting user name
     */
    public String getBouncerUser() throws Exception {
        String user = TestSession.conf.getProperty("DEFAULT_BOUNCER_USER");
        if (user == null) {
            String[] output = TestSession.exec.runProcBuilder(
                    new String[]{"keydbgetkey", "hadoopqa_re_bouncer.user"});
            if (!output[0].equals("0")) {
                throw new IllegalStateException("keydbgetkey failed for user");
            }
            user = output[1].trim();
        }
        return user;
    }

    /**
     * Get the bouncer pw used for testing.
     *
     * @throws Exception if there is a problem getting user name
     */
    public String getBouncerPassword() throws Exception {
        String password = TestSession.conf.getProperty("DEFAULT_BOUNCER_PASSWORD");
        if (password == null) {
            String[] output = TestSession.exec.runProcBuilder(
                    new String[]{"keydbgetkey", "hadoopqa_re_bouncer.passwd"});
            if (!output[0].equals("0")) {
                throw new IllegalStateException("keydbgetkey failed for password");
            }
            password = output[1].trim();
        }
        return password;
    }

    private static String getWithBouncer(String user, String pw, String url, int expectedCode) {
        HTTPHandle client = new HTTPHandle();
        client.logonToBouncer(user,pw);
        logger.info("Cookie = " + client.YBYCookie);
        String myCookie = client.YBYCookie;

        logger.info("URL to get is: " + url);
        HttpMethod getMethod = client.makeGET(url, new String(""), null);
        Response response = new Response(getMethod, false);
        assertEquals("Status code for "+url+" does not match the expected value", expectedCode, response.getStatusCode());
        String output = response.getResponseBodyAsString();
        logger.info("******* OUTPUT = " + output);
        return output;
    }

    private String getAppClusterURL(SparkRunSparkSubmit appUserDefault) {
        ResourceManagerHttpUtils rmUtils = new ResourceManagerHttpUtils();
        String CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME");
        return rmUtils.getResourceManagerURL(CLUSTER_NAME) + "/proxy/" + appUserDefault.getID();
    }

    private void startAndCheckUI(SparkRunSparkSubmit appUserDefault) throws Exception {
        appUserDefault.start();

        assertTrue("SparkHdfsLR app (default user) was not assigned an ID within 120 seconds.",
            appUserDefault.waitForID(120));
        assertTrue("SparkHdfsLR app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        appUserDefault.blockUntilRunning();

        assertEquals("SparkHdfsLR is not running",
            YarnApplicationState.RUNNING, appUserDefault.getYarnState());

        String uiURL = getAppClusterURL(appUserDefault);
        String stagesUIURL = uiURL + "/proxy/" + appUserDefault.getID() + "/stages/";

        logger.info("Test default bouncer user works on ui: " + uiURL);
        String output = getWithBouncer(getBouncerUser(), getBouncerPassword(), uiURL, 200);

        // Look for the stages link on Spark UI
        String patternString = "/proxy/" + appUserDefault.getID() + "/stages";
        Pattern appPattern = Pattern.compile(patternString);
        Matcher appMatcher = appPattern.matcher(output);
        assertTrue("Did not find stages link in SparkUI", appMatcher.find()); 
    }

    @Test
    public void runNoAclsSet() {
        try {
            appUserDefault = new SparkRunSparkSubmit();
   
            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
   
            startAndCheckUI(appUserDefault);
   
            String uiURL = getAppClusterURL(appUserDefault);
            String stagesUIURL = uiURL + "/proxy/" + appUserDefault.getID() + "/stages/";
            logger.info("Test default bouncer user doesn't work on ui: " + stagesUIURL);
            getWithBouncer(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_password, stagesUIURL, 403);
   
            int waitTime = 180;
            assertTrue("Job (default user) did not succeed.",
               appUserDefault.waitForSuccess(waitTime));
   
            String outputDone = getWithBouncer(getBouncerUser(), getBouncerPassword(), uiURL, 200);
            String yarnUIPattern = "Application Overview";
            Pattern doneAppPattern = Pattern.compile(yarnUIPattern);
            Matcher doneAppMatcher = doneAppPattern.matcher(outputDone);
            assertTrue("Did not find stages link in SparkUI", doneAppMatcher.find()); 

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runUserViewAclsSet() {
        try {
            appUserDefault = new SparkRunSparkSubmit();
   
            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            appUserDefault.setConf("spark.ui.view.acls=" + HadooptestConstants.UserNames.HITUSR_1);
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
            startAndCheckUI(appUserDefault);
   
            String stagesUIURL = getAppClusterURL(appUserDefault) + "/stages/";
            logger.info("Test hitusr_1 user doesn't work on ui");
            getWithBouncer(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_password, stagesUIURL, 200);

            int waitTime = 180;
            assertTrue("Job (default user) did not succeed.",
               appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runUserAdminAclsSet() {
        try {
            appUserDefault = new SparkRunSparkSubmit();
   
            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            appUserDefault.setConf("spark.admin.acls=" + HadooptestConstants.UserNames.HITUSR_1);
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
            startAndCheckUI(appUserDefault);
   
            String stagesUIURL = getAppClusterURL(appUserDefault) + "/stages/";
            logger.info("Test default bouncer user works on ui: " + stagesUIURL);

            logger.info("Test hitusr_1 user doesn't work on ui");
            getWithBouncer(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_password, stagesUIURL, 200);

            int waitTime = 180;
            assertTrue("Job (default user) did not succeed.",
               appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runUserModifyAclsSet() {
        try {
            appUserDefault = new SparkRunSparkSubmit();

            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            appUserDefault.setConf("spark.modify.acls=" + HadooptestConstants.UserNames.HITUSR_2);
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
            startAndCheckUI(appUserDefault);

            String stagesUIURL = getAppClusterURL(appUserDefault)  + "/stages/";
            logger.info("Test hitusr_2 user doesn't work on ui");
            getWithBouncer(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_password, stagesUIURL, 403);

            // should fail since user doesn't have modify permissions
            boolean ret = appUserDefault.killCLI(HadooptestConstants.UserNames.DFSLOAD);
            assertFalse("kill should have failed", ret);

            int waitTime = 180;
            assertTrue("Job (default user) did not succeed.",
               appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runUserModifyAclsSetKill() {
        try {
            appUserDefault = new SparkRunSparkSubmit();

            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            appUserDefault.setConf("spark.modify.acls=" + HadooptestConstants.UserNames.HITUSR_1 + "," + HadooptestConstants.UserNames.DFSLOAD);
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
            startAndCheckUI(appUserDefault);

            // should pass since user has modify permissions
            boolean ret = appUserDefault.killCLI(HadooptestConstants.UserNames.DFSLOAD);
            assertTrue("kill failed", ret);

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runUserAdminAclsSetKill() {
        try {
            appUserDefault = new SparkRunSparkSubmit();
   
            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            appUserDefault.setConf("spark.admin.acls=" + HadooptestConstants.UserNames.HITUSR_1 + "," + HadooptestConstants.UserNames.DFSLOAD);
            // use 500 so job stays active long enough to get to UI
            String[] argsArray = {lrDatafile, "500"};
            appUserDefault.setArgs(argsArray);
            startAndCheckUI(appUserDefault);
   
            // should pass since user has admin permissions
            boolean ret = appUserDefault.killCLI(HadooptestConstants.UserNames.DFSLOAD);
            assertTrue("kill failed", ret);

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }
}
