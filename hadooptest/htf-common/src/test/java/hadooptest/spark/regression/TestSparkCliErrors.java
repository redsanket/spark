package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;
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

public class TestSparkCliErrors extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/*
	 * A test running number of workers 0
	 * 
	 */
	@Test
	public void runSparkPiTestWorkersZero() {
		try {
			SparkPi appUserDefault = new SparkPi();

			appUserDefault.setWorkerMemory("2g");
			appUserDefault.setNumWorkers(0);
			appUserDefault.setWorkerCores(1);

			appUserDefault.start();

			assertTrue("Error because workers are 0",
					appUserDefault.waitForERROR(10));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	/*
	 * A test running small am memory
	 * 
	 */
	@Test
	public void runSparkPiTestAmMemorySmall() {
		try {
			SparkPi appUserDefault = new SparkPi();

			appUserDefault.setWorkerMemory("2g");
			appUserDefault.setMasterMemory("2m");
			appUserDefault.setNumWorkers(1);
			appUserDefault.setWorkerCores(1);

			appUserDefault.start();

			assertTrue("Error because AM memory to small",
					appUserDefault.waitForERROR(10));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	/*
	 * A test running small worker memory
	 * 
	 */
	@Test
	public void runSparkPiTestWorkerMemorySmall() {
		try {
			SparkPi appUserDefault = new SparkPi();

			appUserDefault.setWorkerMemory("1024");
			appUserDefault.setNumWorkers(1);
			appUserDefault.setWorkerCores(1);

			appUserDefault.start();

			assertTrue("Error because worker memory to small",
					appUserDefault.waitForERROR(10));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
}
