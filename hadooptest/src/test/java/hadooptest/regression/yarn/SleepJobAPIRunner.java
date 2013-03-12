package hadooptest.regression.yarn;

import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ToolRunner;

public class SleepJobAPIRunner extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
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
			this.cluster.setSecurityAPI("keytab-hadoop1", "user-hadoop1");
			rc = ToolRunner.run(conf, new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}

			this.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
		    rc = ToolRunner.run(conf, new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}
		}
		catch (Exception e) {
			TestSession.logger.error("Job failed!!!");
			e.printStackTrace();
		}
	}	

}
