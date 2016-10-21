package hadooptest.hadoop.regression.yarn;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.MapredCliCommands.GenericMapredCliResponseBO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMapredQueueCLI extends YarnTestsBaseClass {
	private static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";
	private static String REPLACEMENT_CAP_SCHED_XML_FILE = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/resources/hadooptest/hadoop/regression/yarn"
			+ "/mapredQueueCli/capacity-scheduler_c2QueueStopped.xml";
	private static String Q_NAME = "c2";

	private QueueInfo[] getQueues() {
		QueueInfo[] queueInfoArray = null;
		try {
			// Job job = Job.getInstance(TestSession.cluster.getConf());
			Cluster cluster = new Cluster(TestSession.cluster.getConf());
			// queueInfoArray = job.getCluster().getQueues();
			queueInfoArray = cluster.getQueues();
		} catch (IOException ioEx) {
			TestSession.logger.error("Got an IOException on doing getQueues");
		} catch (InterruptedException intEx) {
			TestSession.logger
					.error("Got an InterruptedException on doing getQueues");
		}
		return queueInfoArray;
	}

	public void copyResMgrConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		TestSession.logger
				.info("Copying over canned cap sched file localted @:"
						+ replacementConfigFile);
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);

		// Bounce node
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(20000);

                // wait up to 5 minutes for NN to be out of safemode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, 
				DfsTestsBaseClass.Report.NO, "get",
				DfsTestsBaseClass.ClearQuota.NO, 
				DfsTestsBaseClass.SetQuota.NO, 
				0, 
				DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 
				0, 
				DfsTestsBaseClass.PrintTopology.NO, null);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    Thread.sleep(10000);
                  }
                }

		Configuration conf = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Iterator iter = conf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			TestSession.logger.trace("Key:[" + entry.getKey() + "] Value["
					+ entry.getValue() + "]");
		}

	}

	@Test
	public void testQueueWithListOption() throws Exception {
		int queueCount = 0;
		copyResMgrConfigAndRestartNodes(REPLACEMENT_CAP_SCHED_XML_FILE);
		QueueInfo[] queueInfoArray = getQueues();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();

		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.queueList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA);

		String[] responseLines = genericMapredCliResponseBO.response
				.split("\n");
		for (String aLine : responseLines) {
			if (aLine.contains("Queue Name :"))
				queueCount++;
		}

		for (QueueInfo qInfo : queueInfoArray) {
			TestSession.logger.info("API reported queue:"
					+ qInfo.getQueueName());
		}
		Assert.assertTrue("Read back queue count:" + queueCount
				+ " while Hadoop API count says:" + queueInfoArray.length,
				queueCount == queueInfoArray.length);

	}

	@Test
	public void testQueueWithInfoOptionWithQueueName() throws Exception {
		copyResMgrConfigAndRestartNodes(REPLACEMENT_CAP_SCHED_XML_FILE);
		QueueInfo[] queueInfoArray = getQueues();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();

		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.queueInfo(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, Q_NAME, false);

		String[] responseLines = genericMapredCliResponseBO.response
				.split("\n");
		for (String aLine : responseLines) {
			if (aLine.contains("Queue Name :"))
				Assert.assertTrue(aLine.contains(Q_NAME));
			if (aLine.contains("Queue State :"))
				Assert.assertTrue(aLine.contains("stopped"));
		}

	}

	@Test
	public void testQueueWithShowJobsInfoForQueueRunningJob() throws Exception {
		copyResMgrConfigAndRestartNodes(REPLACEMENT_CAP_SCHED_XML_FILE);
		QueueInfo[] queueInfoArray = getQueues();
		String queueToSubmitJob = "a2";
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToSubmitJob,
				HadooptestConstants.UserNames.HADOOPQA,
				getDefaultSleepJobProps(queueToSubmitJob), 1, 1, 30, 1, 30, 1,
				"mapredQueueCli", false);
		Thread.sleep(10000);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		Thread.sleep(10000);

		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.queueInfo(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						queueToSubmitJob, true);

		String[] responseLines = genericMapredCliResponseBO.response
				.split("\n");
		for (String aLine : responseLines) {
			if (aLine.contains("Queue Name :"))
				Assert.assertTrue(aLine.contains(queueToSubmitJob));
			if (aLine.contains("job_"))
				Assert.assertTrue(
						"Could not located expected jobId in response:" + jobId,
						aLine.contains(jobId));
		}

		// check if the job succeeded, job should be done in 60 secs so bail after 90
		int timer = 0;
		while ( (! job.isComplete()) && (timer < 90) ) {
			Thread.sleep(2000);
 			timer+=2;
		}
		if ( timer >= 90 ) {
		  TestSession.logger.error("Job " + jobID + " did not complete after 90 seconds!");  
		else {
		  Assert.assertTrue("Job " + jobID + " did not succeed! ", job.isSuccessful() );
		}

	}

	@Test
	@Ignore("Does not seem to be an important test case. We've come a long way since, hence this can be dropped.")
	public void testQueueWithNoOptions() {

	}

	@Test
	@Ignore("Does not seem to be an important test case. We've come a long way since, hence this can be dropped.")
	public void testQueueWithInvalidOptions() {

	}

	@Test
	@Ignore("Does not seem to be an important test case. We've come a long way since, hence this can be dropped.")
	public void testQueueWithInfoOptionWithNoQueueName() {

	}

	@Test
	@Ignore("Does not seem to be an important test case. We've come a long way since, hence this can be dropped.")
	public void testQueueWithInfoOptionWithInvalidQueueName() {

	}

	@Test
	@Ignore("Covered as a part of testQueueWithInfoOptionWithQueueName.")
	public void testQueueWithInfoOptionWithQueueStopped() {

	}

	@After
	public void resetConfig() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		TestSession.cluster.getConf().resetHadoopConfDir();
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
				TestSession.cluster
						.getNodeNames(HadoopCluster.RESOURCE_MANAGER),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

	}

}
