package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.job.SleepJob;

import org.junit.BeforeClass;
import org.junit.Test;

public class SleepJobRunner extends TestSession {
	
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
		try {
			SleepJob jobUserDefault = new SleepJob();
			SleepJob jobUser1 = new SleepJob();
			SleepJob jobUser2 = new SleepJob();
			SleepJob jobUser3 = new SleepJob();

			jobUserDefault.setNumMappers(1);
			jobUserDefault.setNumReducers(1);
			jobUserDefault.setMapDuration(100);
			jobUserDefault.setReduceDuration(100);

			jobUserDefault.start();

			jobUser1.setNumMappers(1);
			jobUser1.setNumReducers(1);
			jobUser1.setMapDuration(100);
			jobUser1.setReduceDuration(100);
			jobUser1.setUser("hadoop1");

			jobUser1.start();

			jobUser2.setNumMappers(1);
			jobUser2.setNumReducers(1);
			jobUser2.setMapDuration(100);
			jobUser2.setReduceDuration(100);
			jobUser2.setUser("hadoop2");

			jobUser2.start();

			jobUser3.setNumMappers(1);
			jobUser3.setNumReducers(1);
			jobUser3.setMapDuration(100);
			jobUser3.setReduceDuration(100);
			jobUser3.setUser("hadoop3");

			jobUser3.start();

			assertTrue("Sleep job (default user) was not assigned an ID within 10 seconds.", 
					jobUserDefault.waitForID(10));
			assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
					jobUserDefault.verifyID());

			assertTrue("Sleep job (user 1) was not assigned an ID within 10 seconds.", 
					jobUser1.waitForID(10));
			assertTrue("Sleep job ID for sleep job (user 1) is invalid.", 
					jobUser1.verifyID());

			assertTrue("Sleep job (user 2) was not assigned an ID within 10 seconds.", 
					jobUser2.waitForID(10));
			assertTrue("Sleep job ID for sleep job (user 2) is invalid.", 
					jobUser2.verifyID());

			assertTrue("Sleep job (user 3) was not assigned an ID within 10 seconds.", 
					jobUser3.waitForID(10));
			assertTrue("Sleep job ID for sleep job (user 3) is invalid.", 
					jobUser3.verifyID());

			assertTrue("Job (default user) did not succeed.", jobUserDefault.waitForSuccess(1));
			assertTrue("Job (user 1) did not succeed.", jobUser1.waitForSuccess(1));
			assertTrue("Job (user 2) did not succeed.", jobUser2.waitForSuccess(1));
			assertTrue("Job (user 3) did not succeed.", jobUser3.waitForSuccess(1));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	@Test
	public void runSleepJobTestNoID() {
		SleepJob sleepJob = null;
		
		try {
			String ID = "0";
			sleepJob = new SleepJob();

			sleepJob.setNumMappers(1);
			sleepJob.setNumReducers(1);
			sleepJob.setMapDuration(100);
			sleepJob.setReduceDuration(100);
			sleepJob.setJobInitSetID(false);

			sleepJob.start();
			Util.sleep(1);

			try {
				String jobPatternStr = " Running job: (.*)$";
				Pattern jobPattern = Pattern.compile(jobPatternStr);

				BufferedReader reader=new BufferedReader(new InputStreamReader(sleepJob.getProcess().getInputStream())); 
				String line=reader.readLine(); 

				while(line!=null) 
				{ 
					TestSession.logger.info(line);

					Matcher jobMatcher = jobPattern.matcher(line);

					if (jobMatcher.find()) {
						ID = jobMatcher.group(1);
						TestSession.logger.info("JOB ID: " + ID);
						break;
					}

					line=reader.readLine();
				} 
			}
			catch (Exception e) {
				if (sleepJob.getProcess() != null) {
					sleepJob.getProcess().destroy();
				}
				e.printStackTrace();
			}

			assertTrue("The ID of the job was not set and is still 0.", ID != "0");

			String jobPatternStr = "job_(.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);

			Matcher jobMatcher = jobPattern.matcher(ID);

			assertTrue("Job ID did not match expected format.", jobMatcher.find());
		}
		catch (Exception e) {
			if (sleepJob.getProcess() != null) {
				sleepJob.getProcess().destroy();
			}

			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
