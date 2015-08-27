package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;

import hadooptest.workflow.spark.app.SparkRunClass;
import org.junit.experimental.categories.Category;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;

import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.Util;

// QE cluster isn't large enough to do in parallel
//@Category(hadooptest.ParallelClassAndMethodTests.class)
@Category(SerialTests.class)
public class TestSparkHackJetty extends TestSession {

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

        // requires that authentication is on
	@Test
	public void runSparkHackJettyBroadcast() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

		appUserDefault.setWorkerMemory("2g");
		appUserDefault.setNumWorkers(3);
		appUserDefault.setWorkerCores(1);
		appUserDefault.setClassName("hadooptest.spark.regression.SparkHackJetty");
		appUserDefault.setJarName(localJar);
		appUserDefault.setAppName("runSparkHackJettyBroadcast");
		appUserDefault.setShouldPassName(true);
                String[] argsArray = {"true", "false"};
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
	public void runSparkHackJettyBroadcastAuthOff() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

		appUserDefault.setWorkerMemory("2g");
		appUserDefault.setNumWorkers(3);
		appUserDefault.setWorkerCores(1);
		appUserDefault.setClassName("hadooptest.spark.regression.SparkHackJetty");
		appUserDefault.setJarName(localJar);
		appUserDefault.setAppName("runSparkHackJettyBroadcastAuthOff");
		appUserDefault.setShouldPassName(true);
                String[] argsArray = {"true", "true"};
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
	public void runSparkHackJettyFileServer() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

		appUserDefault.setWorkerMemory("2g");
		appUserDefault.setNumWorkers(3);
		appUserDefault.setWorkerCores(1);
		appUserDefault.setClassName("hadooptest.spark.regression.SparkHackJetty");
		appUserDefault.setJarName(localJar);
		appUserDefault.setAppName("runSparkHackJettyFileServer");
		appUserDefault.setShouldPassName(true);
                String[] argsArray = {"false", "false"};
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
	public void runSparkHackJettyFileServerAuthOff() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

		appUserDefault.setWorkerMemory("2g");
		appUserDefault.setNumWorkers(3);
		appUserDefault.setWorkerCores(1);
		appUserDefault.setClassName("hadooptest.spark.regression.SparkHackJetty");
		appUserDefault.setAppName("runSparkHackJettyFileServerAuthOff");
		appUserDefault.setShouldPassName(true);
		appUserDefault.setJarName(localJar);
                String[] argsArray = {"false", "true"};
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
