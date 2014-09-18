package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;
import org.junit.experimental.categories.Category;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkRunClass;

import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.Util;

@Category(hadooptest.ParallelClassAndMethodTests.class)
public class TestSparkHackCM extends TestSession {

        private static String localJar = null;

        @BeforeClass
        public static void startTestSession() throws Exception {
                TestSession.start();
                setupTestDir();
        }

        public static void setupTestDir() throws Exception {

                TestSession.cluster.getFS();
                localJar = Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
        }

	@Test
	public void runSparkHackCMAuthOff() throws Exception {
		SparkRunClass appUserDefault = new SparkRunClass();

		appUserDefault.setWorkerMemory("2g");
                // can only use 1 worker
		appUserDefault.setNumWorkers(1);
		appUserDefault.setWorkerCores(1);
		//appUserDefault.setClassName("hadooptest.spark.regression.SparkRunHackCM");
		appUserDefault.setClassName("org.apache.spark.examples.SparkRunHackCM");
		appUserDefault.setJarName(localJar);
		appUserDefault.setAppName("runSparkHackCM");
		appUserDefault.setShouldPassName(true);
                String[] argsArray = {"false", "invalid"};
                appUserDefault.setArgs(argsArray);

		appUserDefault.start();

		assertTrue("App (default user) was not assigned an ID within 30 seconds.", 
			appUserDefault.waitForID(30));
		assertTrue("App ID for sleep app (default user) is invalid.", 
			appUserDefault.verifyID());

		int waitTime = 30;
		assertTrue("Job (default user) did not succeed.",
			appUserDefault.waitForSuccess(waitTime));
	}

	@Test
	public void runSparkHackCM() throws Exception {
		SparkRunClass appUserDefault = new SparkRunClass();

		appUserDefault.setWorkerMemory("2g");
                // can only use 1 worker
		appUserDefault.setNumWorkers(1);
		appUserDefault.setWorkerCores(1);
		//appUserDefault.setClassName("hadooptest.spark.regression.SparkRunHackCM");
		appUserDefault.setClassName("org.apache.spark.examples.SparkRunHackCM");
		appUserDefault.setJarName(localJar);
		appUserDefault.setAppName("runSparkHackCM");
		appUserDefault.setShouldPassName(true);
                String[] argsArray = {"true", "invalid"};
                appUserDefault.setArgs(argsArray);

		appUserDefault.start();

		assertTrue("App (default user) was not assigned an ID within 30 seconds.", 
			appUserDefault.waitForID(30));
		assertTrue("App ID for sleep app (default user) is invalid.", 
			appUserDefault.verifyID());

		int waitTime = 30;
		assertTrue("Job (default user) did not succeed.",
			appUserDefault.waitForSuccess(waitTime));
	}
}
