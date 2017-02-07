package hadooptest.spark.regression;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SerialTests.class)
public class TestDynamicResourceAllocation {

    public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";
    protected static boolean restoredConfig = false;

    private static final Integer NUM_EXECUTORS = 4;
    private static String localJar = null;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestDir();
        if (restoredConfig)
            return;
        restoredConfig = true;
        String replacementConfigFile = TestSession.conf
                .getProperty("WORKSPACE")
                + "/htf-common/resources/hadooptest/hadoop/regression/yarn/dynamicResourceAllocation/dynamicResourceAllocation.xml";

        FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
                .getCluster();

        // Backup config and replace file, for Resource Manager
        fullyDistributedCluster.getConf(
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
        fullyDistributedCluster.getConf(
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
                .copyFileToConfDir(replacementConfigFile,
                        CAPACITY_SCHEDULER_XML);

        fullyDistributedCluster.hadoopDaemon(HadoopCluster.Action.STOP,
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
        fullyDistributedCluster.hadoopDaemon(HadoopCluster.Action.START,
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

        Thread.sleep(60000);
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        removeTestDir();
        //restore configs
        FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
                .getCluster();
        fullyDistributedCluster.getConf(
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
                .resetHadoopConfDir();

        fullyDistributedCluster.hadoopDaemon(HadoopCluster.Action.STOP,
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
        fullyDistributedCluster.hadoopDaemon(HadoopCluster.Action.START,
                HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
                TestSession.cluster
                        .getNodeNames(HadoopCluster.RESOURCE_MANAGER),
                TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

        Thread.sleep(20000);
    }

    public static void setupTestDir() throws Exception {

        TestSession.cluster.getFS();

        localJar =
                Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");

    }

    public static void removeTestDir() throws Exception {

    }

    private void setupConfs(SparkRunSparkSubmit appUserDefault) {
        appUserDefault.setWorkerMemory("128m");
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.DynamicResourceAllocationTestTopology");
        appUserDefault.setAppName("TestDynamicAllocation");
        appUserDefault.setJarName(localJar);
        appUserDefault.addConf("spark.dynamicAllocation.enabled", "true");
        appUserDefault.addConf("spark.shuffle.service.enabled", "true");
        appUserDefault.addConf("spark.dynamicAllocation.executorIdleTimeout", "30");
        appUserDefault.addConf("spark.admin.acls", "tgraves,jerrypeng,hitusr_1");
        appUserDefault.setMasterMemory("256m");
        appUserDefault.setShouldPassNumWorkers(false);
        appUserDefault.setShouldPassName(true);
        String[] argsArray = {NUM_EXECUTORS.toString()};
        appUserDefault.setArgs(argsArray);
    }

    @Test
    public void TestDynamicAllocationAndShuffleServiceTurnedOn() {
        TestSession.logger.info("Running TestDynamicAllocationAndShuffleServiceTurnedOn...");

        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
            setupConfs(appUserDefault);
            appUserDefault.setAppName("TestDynamicAllocationAndShuffleServiceTurnedOn");

            //pass in arg to job
            appUserDefault.start();

            assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                    appUserDefault.waitForID(30));
            assertTrue("App ID for sleep app (default user) is invalid.",
                    appUserDefault.verifyID());
            assertEquals("App name for sleep app is invalid.",
                    "TestDynamicAllocationAndShuffleServiceTurnedOn", appUserDefault.getAppName());

	    String HITUSR1_PASSWD = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_1");

            //wait to get executors. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), NUM_EXECUTORS + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (NUM_EXECUTORS + 1) + " executors timeout");
            }

            //wait for all tasks to complete
            if (!appUserDefault.waitForNoActiveTasks(appUserDefault.getID(), "hitusr_1", HITUSR1_PASSWD, 100)) {
                TestSession.logger.warn("Not all tasks finished after timeout period");
            }

            //wait for it to scale down. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), 0 + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (0 + 1) + " executors timeout");
            }

            TestSession.logger.info("Waiting for application to run to completion...");

            int waitTime = 5;
            assertTrue("Job (default user) did not succeed.",
                    appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void TestDynamicAllocationMaxExecutors() {
        TestSession.logger.info("Running TestDynamicAllocationMaxExecutors...");
        //The maximum number of executors able to be acquired
        final Integer MAX_EXECUTORS = 2;

        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
            setupConfs(appUserDefault);
            appUserDefault.addConf("spark.dynamicAllocation.maxExecutors", MAX_EXECUTORS.toString());
            appUserDefault.setAppName("TestDynamicAllocationMaxExecutors");

            appUserDefault.start();

            assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                    appUserDefault.waitForID(30));
            assertTrue("App ID for sleep app (default user) is invalid.",
                    appUserDefault.verifyID());
            assertEquals("App name for sleep app is invalid.",
                    "TestDynamicAllocationMaxExecutors", appUserDefault.getAppName());

	    String HITUSR1_PASSWD = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_1");

            //wait to get executors. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), MAX_EXECUTORS + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (MAX_EXECUTORS + 1) + " executors timeout");
            }

            //wait for all tasks to complete
            if (!appUserDefault.waitForNoActiveTasks(appUserDefault.getID(), "hitusr_1", HITUSR1_PASSWD, 300)) {
                TestSession.logger.warn("Not all tasks finished after timeout period");
            }

            //wait for it to scale down. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), 0 + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (0 + 1) + " executors timeout");
            }

            TestSession.logger.info("Waiting for application to run to completion...");

            int waitTime = 5;
            assertTrue("Job (default user) did not succeed.",
                    appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void TestDynamicAllocationMinExecutors() {
        TestSession.logger.info("Running TestDynamicAllocationMinExecutors...");
        //The minimum number of executors to allocate to application
        final Integer MIN_EXECUTORS = 2;

        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
            setupConfs(appUserDefault);
            appUserDefault.addConf("spark.dynamicAllocation.minExecutors", MIN_EXECUTORS.toString());
            appUserDefault.setAppName("TestDynamicAllocationMinExecutors");

            appUserDefault.start();

            assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                    appUserDefault.waitForID(30));
            assertTrue("App ID for sleep app (default user) is invalid.",
                    appUserDefault.verifyID());
            assertEquals("App name for sleep app is invalid.",
                    "TestDynamicAllocationMinExecutors", appUserDefault.getAppName());

	    String HITUSR1_PASSWD = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_1");

            //wait to get executors. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), NUM_EXECUTORS + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (NUM_EXECUTORS + 1) + " executors timeout");
            }

            //wait for all tasks to complete
            if (!appUserDefault.waitForNoActiveTasks(appUserDefault.getID(), "hitusr_1", HITUSR1_PASSWD, 100)) {
                TestSession.logger.warn("Not all tasks finished after timeout period");
            }

            //wait for it to scale down. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), MIN_EXECUTORS + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (MIN_EXECUTORS + 1) + " executors timeout");
            }

            TestSession.logger.info("Waiting for application to run to completion...");

            int waitTime = 5;
            assertTrue("Job (default user) did not succeed.",
                    appUserDefault.waitForSuccess(waitTime));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void testCachedExecutorIdleTimeout() {
        TestSession.logger.info("Running testCachedExecutorIdleTimeout...");

        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
            appUserDefault = new SparkRunSparkSubmit();
            setupConfs(appUserDefault);

            appUserDefault.setAppName("testCachedExecutorIdleTimeout");
            String[] argsArray = {NUM_EXECUTORS.toString(), "1"};
            appUserDefault.addConf("spark.dynamicAllocation.cachedExecutorIdleTimeout", "30");

            appUserDefault.setArgs(argsArray);

            appUserDefault.start();

            assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                    appUserDefault.waitForID(30));
            assertTrue("App ID for sleep app (default user) is invalid.",
                    appUserDefault.verifyID());
            assertEquals("App name for sleep app is invalid.",
                    "testCachedExecutorIdleTimeout", appUserDefault.getAppName());

	    String HITUSR1_PASSWD = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_1");

            //wait to get executors. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), NUM_EXECUTORS + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (NUM_EXECUTORS + 1) + " executors timeout");
            }

            //wait for all tasks to complete
            if (!appUserDefault.waitForNoActiveTasks(appUserDefault.getID(), "hitusr_1", HITUSR1_PASSWD, 100)) {
                TestSession.logger.warn("Not all tasks finished after timeout period");
            }

            //wait for it to scale down. +1 executor for driver
            if (!appUserDefault.waitForExecutors(appUserDefault.getID(), 0 + 1, "hitusr_1", HITUSR1_PASSWD, 100)) {
                fail("wait to get " + (0 + 1) + " executors timeout");
            }

            TestSession.logger.info("Waiting for application to run to completion...");

            assertTrue("Job (default user) did not succeed.",
                    appUserDefault.waitForSuccess(5));

        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }
}
