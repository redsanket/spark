package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.Assert;
import org.junit.Test;


public class TestBackwardCompatibility extends YarnTestsBaseClass {
	/**
	 * Test to check the backward compatibility of mapred.job.name and
	 * mapreduce.job.name
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibility10() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOPQA;
		GenericCliResponseBO genericCliResponse;
		String testName = "testBackwardCompatibility10";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + "-" + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/backwardCompatibility/BackwardCompatibility-10/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output1\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output1/*");
		String stringToLookFor = "<property><name>mapreduce.job.name</name><value>"
				+ testName + "</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

		/**
		 * Pass the parameter mapreduce.job.name this time around.
		 */
		commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output4\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output4/*");
		stringToLookFor = "<property><name>mapreduce.job.name</name><value>"
				+ testName + "</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

	}

	/**
	 * Test to check the backward compatibility of jobclient.output.filter and
	 * mapreduce.client.output.filter
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibility20() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOPQA;
		GenericCliResponseBO genericCliResponse;
		String testName = "testBackwardCompatibility20";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + "-" + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/backwardCompatibility/BackwardCompatibility-20/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output1\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"jobclient.output.filter=ALL\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output1/*");

		String stringToLookFor = "<property><name>mapreduce.client.output.filter</name><value>ALL</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

		/**
		 * Pass the parameter mapreduce.client.output.filter=ALL this time
		 * around.
		 */
		commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output4\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.client.output.filter=ALL\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output4/*");
		stringToLookFor = "<property><name>mapreduce.client.output.filter</name><value>ALL</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

	}

	/**
	 * Test to check the backward compatibility of mapred.job.queue.name and
	 * mapreduce.job.queuename
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibility30() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOPQA;
		GenericCliResponseBO genericCliResponse;
		String testName = "testBackwardCompatibility30";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + "-" + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/backwardCompatibility/BackwardCompatibility-30/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output1\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.job.queue.name=grideng\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output1/*");

		String stringToLookFor = "<property><name>mapreduce.job.queuename</name><value>grideng</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

		/**
		 * Pass the parameter mapreduce.job.queue.name this time around.
		 */
		commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output4\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.queuename=grideng\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output4/*");
		stringToLookFor = "<property><name>mapreduce.job.queuename</name><value>grideng</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

	}

	/**
	 * Test to check the backward compatibility of mapred.working.dir and
	 * mapreduce.job.working.dir
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibility40() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOPQA;
		GenericCliResponseBO genericCliResponse;
		String testName = "testBackwardCompatibility40";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + "-" + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/backwardCompatibility/BackwardCompatibility-40/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output1\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml\"");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.working.dir=/tmp\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapred.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output1/*");

		String stringToLookFor = "<property><name>mapreduce.job.working.dir</name><value>/tmp</value><source>because mapred.working.dir is deprecated</source><source>job.xml</source></property>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

		/**
		 * Pass the parameter mapreduce.working.dir=/tmp this time around.
		 */
		commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add("\"" + dirInHdfs + "/input.txt\"");
		commandFrags.add("-output");
		commandFrags.add("\"" + dirInHdfs + "/Output4\"");
		commandFrags.add("-mapper");
		commandFrags.add("\"cat job.xml"\");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.working.dir=/tmp\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.name=" + testName + "\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.acl-view-job=*\"");

		TestSession.logger.debug("\nArg array list sent to Streaming job:");
		TestSession.logger.debug(commandFrags.toString() + "\n");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

		waitTillJobStartsRunning(job);
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/Output4/*");
		stringToLookFor = "<property><name>mapreduce.working.dir</name><value>/tmp</value>";
		Assert.assertTrue(genericCliResponse.response + " DID NOT CONTAIN "
				+ stringToLookFor,
				genericCliResponse.response.contains(stringToLookFor));

	}

//	void waitTillJobSucceeds(Job job) throws IOException, InterruptedException {
//
//		State jobState = job.getStatus().getState();
//		while (jobState != State.SUCCEEDED) {
//			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
//				break;
//			}
//			Thread.sleep(1000);
//			jobState = job.getStatus().getState();
//			TestSession.logger
//					.info(job.getJobName()
//							+ " is in state : "
//							+ jobState
//							+ ", awaiting its state to change to 'SUCCEEDED' hence sleeping for 1 sec");
//		}
//	}

//	void waitTillJobStartsRunning(Job job) throws IOException,
//			InterruptedException {
//		State jobState = job.getStatus().getState();
//		while (jobState != State.RUNNING) {
//			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
//				break;
//			}
//			Thread.sleep(1000);
//			jobState = job.getStatus().getState();
//			TestSession.logger
//					.info(job.getJobName()
//							+ " is in state : "
//							+ jobState
//							+ ", awaiting its state to change to 'RUNNING' hence sleeping for 1 sec");
//		}
//	}
}
