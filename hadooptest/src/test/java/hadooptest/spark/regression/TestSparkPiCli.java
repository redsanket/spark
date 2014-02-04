package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkPi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.Util;

public class TestSparkPiCli extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/*
	 * A test for running SparkPi
	 * 
	 */
	@Test
	public void runSparkPiTest() {
		try {
			SparkPi appUserDefault = new SparkPi();

			appUserDefault.setWorkerMemory("2g");
			appUserDefault.setNumWorkers(2);
			appUserDefault.setWorkerCores(1);
            String appName = "SparkTestPiCli";
			appUserDefault.setAppName(appName);
			appUserDefault.setShouldPassName(true);

			appUserDefault.start();

			assertTrue("SparkPi app (default user) was not assigned an ID within 30 seconds.", 
					appUserDefault.waitForID(30));
			assertTrue("SparkPi app ID for sleep app (default user) is invalid.", 
					appUserDefault.verifyID());
			assertEquals("SparkPi app name for sleep app is invalid.", 
					appName, appUserDefault.getAppName());

			int waitTime = 30;
			assertTrue("App (default user) did not succeed.",
				appUserDefault.waitForSuccess(waitTime));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
