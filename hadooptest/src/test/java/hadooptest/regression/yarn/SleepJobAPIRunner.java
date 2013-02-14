package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.Util;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ToolRunner;

public class SleepJobAPIRunner extends TestSession {
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() {
		TestSession.start();
		cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() {
		cluster.stop();
		cluster.getConf().cleanup();
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test for running a sleep job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runSleepTest() {
		String[] args = { "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"};
		Configuration conf = TestSession.cluster.getConf();
		
		int rc;
		try {
			SecurityUtil.login(conf, "keytab-hadoop1", "user-hadoop1");
			rc = ToolRunner.run(new Configuration(), new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}

			SecurityUtil.login(conf, "keytab-hadoopqa", "user-hadoopqa");
		    rc = ToolRunner.run(new Configuration(), new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}
		}
		catch (Exception e) {
			TestSession.logger.error("Job failed!!!");
			e.printStackTrace();
		}
	}	
	  
	/******************* END TESTS ***********************/	
}
