package hadooptest.hadoop.regression.yarn.capacityScheduler.queuePreemption;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CalculatedCapacityLimitsBO;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass;
import hadooptest.hadoop.regression.yarn.capacityScheduler.RuntimeRESTStatsBO;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.MyUserInfo;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
/**
 * Documents referred
 * http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html
 * http://twiki.corp.yahoo.com/view/GridDocumentation/GridDocScheduler
 * Unit Tests from:package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TestProportionalCapacityPreemptionPolicy
 * 
 * In these tests, I have not implemented automatic restarting of RMs. So you would be required to restart RMs
 * after replacing the capacity-scheduler.xml file with the ones used in these tests.
 * @author tiwari
 *
 */
@Category(SerialTests.class)
public class TestQueuePreemption extends CapacitySchedulerBaseClass {
	
	class Triple {
		String userName;
		String queueName;
		String file;
		Queue<Job> runningJobsInQ = new ConcurrentLinkedQueue<Job>();
		ExecutorService execService = Executors.newFixedThreadPool(1);
		List<RunnableWordCount> runnableJobs = new ArrayList<RunnableWordCount>();
	}

/**
 * Works off of {@link testProportionalPreemption.xml} data file
 *   //  /   A   B   C  D
      { 100, 10, 40, 20, 30 },  // abs
      { 100, 100, 100, 100, 100 },  // maxCap
      { 100, 30, 60, 10,  0 },  // used
      {  45, 20,  5, 20,  0 },  // pending
      {   0,  0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1,  0 },  // apps
      {  -1,  1,  1,  1,  1 },  // req granularity
      {   4,  0,  0,  0,  0 },  // subqueues

 * Since 'c' is under-utilized, none of the other queues
 * like 'a' and 'b', despte consuming over their capacity should 
 * not contribute to it.
 * @throws Exception
 */
//	 @Test
	public void testProportionalPreemption() throws Exception {
		List<Triple> triples = new ArrayList<Triple>();
		Triple a = new Triple();
		a.userName = HadooptestConstants.UserNames.HITUSR_1;
		a.queueName = "a";
		a.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a);

		Triple b = new Triple();
		b.userName = HadooptestConstants.UserNames.HITUSR_2;
		b.queueName = "b";
		b.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b);

		Triple c = new Triple();
		c.userName = HadooptestConstants.UserNames.HITUSR_3;
		c.queueName = "c";
		c.file = "/tmp/smallFileForQPreemptionTest.txt";
		triples.add(c);

		consumeTriples(triples);
	}

	/**
	 * Works off of {@link textMaxCap.xml} data file
	 * 
	 * Expected result: Queue 'a' should hog all the cluster resources
	 * leaving 'b' just enough resources to function its max cap of 40.
	 *         //  /   A   B   C
        { 100, 40, 40, 20 },  // abs
        { 100, 100, 45, 100 },  // maxCap
        { 100, 55, 45,  0 },  // used
        {  20, 10, 10,  0 },  // pending
        {   0,  0,  0,  0 },  // reserved
        {   2,  1,  1,  0 },  // apps
        {  -1,  1,  1,  0 },  // req granularity
        {   3,  0,  0,  0 },  // subqueues

	 */
//	 @Test
	public void testMaxCap() throws Exception {
			List<Triple> triples = new ArrayList<Triple>();
			Triple a = new Triple();
			a.userName = HadooptestConstants.UserNames.HITUSR_1;
			a.queueName = "a";
			a.file = "/tmp/bigFileForQPreemptionTest.txt";
			triples.add(a);

			Triple b = new Triple();
			b.userName = HadooptestConstants.UserNames.HITUSR_2;
			b.queueName = "b";
			b.file = "/tmp/bigFileForQPreemptionTest.txt";
			triples.add(b);

			consumeTriples(triples);
	}

	/**
	 * Works off of {@link testPreemptiveCycle.xml} data file
	 * 
	 * Expected result: Resources from queue 'a' get consumed
	 * by other queues 'b' and 'c'. They would each work > 100%
	 * of their capacities.
	 *       //  /   A   B   C
      { 100, 40, 40, 20 },  // abs
      { 100, 100, 100, 100 },  // maxCap
      { 100,  0, 60, 40 },  // used
      {  10, 10,  0,  0 },  // pending
      {   0,  0,  0,  0 },  // reserved
      {   3,  1,  1,  1 },  // apps
      {  -1,  1,  1,  1 },  // req granularity
      {   3,  0,  0,  0 },  // subqueues

	 */
//	 @Test
	public void testPreemptiveCycle() throws Exception {
		List<Triple> triples = new ArrayList<Triple>();
		Triple b = new Triple();
		b.userName = HadooptestConstants.UserNames.HITUSR_1;
		b.queueName = "b";
		b.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b);

		Triple c = new Triple();
		c.userName = HadooptestConstants.UserNames.HITUSR_2;
		c.queueName = "c";
		c.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(c);

		consumeTriples(triples);
	}

	/**
	 * Works off of {@link testHierarchical.xml} data file
	 *       //  /    A   B   C    D   E   F
      { 200, 100, 50, 50, 100, 10, 90 },  // abs
      { 200, 200, 200, 200, 200, 200, 200 },  // maxCap
      { 200, 110, 60, 50,  90, 90,  0 },  // used
      {  10,   0,  0,  0,  10,  0, 10 },  // pending
      {   0,   0,  0,  0,   0,  0,  0 },  // reserved
      {   4,   2,  1,  1,   2,  1,  1 },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1 },  // req granularity
      {   2,   2,  0,  0,   2,  0,  0 },  // subqueues

	 */
//	 @Test
	public void testHierarchical() throws Exception {
		 
		List<Triple> triples = new ArrayList<Triple>();
		Triple a1 = new Triple();
		a1.userName = HadooptestConstants.UserNames.HITUSR_1;
		a1.queueName = "a1";
		a1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a1);

		Triple a2 = new Triple();
		a2.userName = HadooptestConstants.UserNames.HITUSR_2;
		a2.queueName = "a2";
		a2.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a2);

		Triple b1 = new Triple();
		b1.userName = HadooptestConstants.UserNames.HITUSR_3;
		b1.queueName = "b1";
		b1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b1);

		consumeTriples(triples);
	}
	
	/**
	 * Works off of {@link testDisablePreemption.xml} data file
		This is the same data file as testHierarchical.xml, but
		queue a.a1 has its preemption disabled.
	 */

//	 @Test
	public void testDisablePreemption() throws Exception {
		 
		List<Triple> triples = new ArrayList<Triple>();
		Triple a1 = new Triple();
		a1.userName = HadooptestConstants.UserNames.HITUSR_1;
		a1.queueName = "a1";
		a1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a1);

		Triple a2 = new Triple();
		a2.userName = HadooptestConstants.UserNames.HITUSR_2;
		a2.queueName = "a2";
		a2.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a2);

		Triple b1 = new Triple();
		b1.userName = HadooptestConstants.UserNames.HITUSR_3;
		b1.queueName = "b1";
		b1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b1);

		consumeTriples(triples);
	}

/**
 *       /    A   B   C    D   E   F    G   H   I
      { 400, 200, 60, 140, 100, 70, 30, 100, 20, 80  },  // abs
      { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, },  // maxCap
      { 400, 210, 70,140, 100, 50, 50,  90, 90,  0  },  // used
      {  10,   0,  0,  0,   0,  0,  0,   0,  0, 15  },  // pending
      {   0,   0,  0,  0,   0,  0,  0,   0,  0,  0  },  // reserved
      {   6,   2,  1,  1,   2,  1,  1,   2,  1,  1  },  // apps
      {  -1,  -1,  1,  1,  -1,  1,  1,  -1,  1,  1  },  // req granularity
      {   3,   2,  0,  0,   2,  0,  0,   2,  0,  0  },  // subqueues

 * @throws Exception
 */
	 @Test
	public void testHierarchicalLarge() throws Exception {
		 
		List<Triple> triples = new ArrayList<Triple>();
		Triple a1 = new Triple();
		a1.userName = HadooptestConstants.UserNames.HITUSR_1;
		a1.queueName = "a1";
		a1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a1);

		Triple a2 = new Triple();
		a2.userName = HadooptestConstants.UserNames.HITUSR_1;
		a2.queueName = "a2";
		a2.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(a2);

		Triple b1 = new Triple();
		b1.userName = HadooptestConstants.UserNames.HITUSR_2;
		b1.queueName = "b1";
		b1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b1);

		Triple b2 = new Triple();
		b2.userName = HadooptestConstants.UserNames.HITUSR_2;
		b2.queueName = "b2";
		b2.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(b2);

		Triple c1 = new Triple();
		c1.userName = HadooptestConstants.UserNames.HITUSR_3;
		c1.queueName = "c1";
		c1.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(c1);

		Triple c2 = new Triple();
		c2.userName = HadooptestConstants.UserNames.HITUSR_3;
		c2.queueName = "c2";
		c2.file = "/tmp/bigFileForQPreemptionTest.txt";
		triples.add(c2);

		consumeTriples(triples);
	}

	@Test
	@Ignore("How can queue a1 have 0 abs capacity and still have 60% utilization")
	public void testZeroGuar() {

	}

	private void consumeTriples(List<Triple> triples)
			throws ClassNotFoundException, IOException, InterruptedException {
		RunnableWordCount aWordCountToRun;
		for (Triple aTriple : triples) {
			TestSession.logger.info("Submitting triple for user:"
					+ aTriple.userName + " file:" + aTriple.file + " on Q:"
					+ aTriple.queueName);
			aWordCountToRun = new RunnableWordCount(aTriple.userName,
					aTriple.queueName, aTriple.runningJobsInQ, aTriple.file);
			aTriple.runnableJobs.add(aWordCountToRun);
			aTriple.execService = submitWordCount(aTriple.runnableJobs);
			TestSession.logger.info("SubmittED triple for user:"
					+ aTriple.userName + " file:" + aTriple.file + " on Q:"
					+ aTriple.queueName);
			Thread.sleep(60000);
		}
		for (Triple aTriple : triples) {
			if (!aTriple.execService.isTerminated()) {
				Thread.sleep(1000);
			}
		}

	}


	public ExecutorService submitWordCount(List<RunnableWordCount> runMe)
			throws ClassNotFoundException, IOException, InterruptedException {

		ExecutorService wordCountThreadPool = Executors.newFixedThreadPool(1);
		for (RunnableWordCount aRunner : runMe) {
			TestSession.logger.info("Submitting job for " + aRunner.user);
			wordCountThreadPool.submit(aRunner);
		}

		wordCountThreadPool.shutdown();
		return wordCountThreadPool;

	}

	class RunnableWordCount implements Runnable {
		String user;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		RunnableWordCount(String user, String queueToSubmitJob,
				Queue<Job> liveJobsQueue, String inputFile) {
			this.user = user;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;

		}

		public void run() {
			TestSession.logger.info("In run of RunnableWordCount");
			UserGroupInformation ugi = getUgiForUser(user);
			try {
				DoAs doAs = new DoAs(ugi, queueToSubmitJob, liveJobsQueue,
						inputFile);
				doAs.doAction();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private class DoAs {
		UserGroupInformation ugi;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		DoAs(UserGroupInformation ugi, String queueToSubmitJob,
				Queue<Job> liveJobsQueue, String inputFile) throws IOException {
			this.ugi = ugi;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;
		}

		public void doAction() throws AccessControlException, IOException,
				InterruptedException {
			PrivilegedExceptionActionImpl privilegedExceptionAction = new PrivilegedExceptionActionImpl(
					ugi, queueToSubmitJob, liveJobsQueue, inputFile);
			TestSession.logger
					.info("About to submit job to privilegedExceptionAction");
			ugi.doAs(privilegedExceptionAction);

		}

	}

	private class PrivilegedExceptionActionImpl implements
			PrivilegedExceptionAction<String> {
		UserGroupInformation ugi;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		PrivilegedExceptionActionImpl(UserGroupInformation ugi,
				String queueToSubmitJob, Queue<Job> liveJobsQueue,
				String inputFile) throws IOException {
			this.ugi = ugi;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;
		}

		public String run() throws Exception {
			WordCountForQueuePreemmption wordCountForQueuePreemption = new WordCountForQueuePreemmption(
					queueToSubmitJob);
			Configuration conf = new Configuration();
			TestSession.logger
					.info("In privilegedExceptionAction about to run wordCountForQueuePreemption now...");
			wordCountForQueuePreemption.run(
					inputFile,
					"/tmp/qPrempt/" + ugi.getUserName() + "/"
							+ System.currentTimeMillis(), conf);
			TestSession.logger.info("Got job handle:"
					+ wordCountForQueuePreemption.jobHandle);
			Job job = wordCountForQueuePreemption.jobHandle;
			liveJobsQueue.add(job);
			TestSession.logger.info("Added job to queue, size:"
					+ liveJobsQueue.size());
			return "";

		}
	}

	UserGroupInformation getUgiForUser(String aUser) {
		String keyTabLocation = null;
		TestSession.logger.info("Entering UGI....");
		if (aUser.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_1)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_1;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_2)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_2;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_3;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_4;
		}
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(aUser,
					keyTabLocation);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		TestSession.logger.info("Got UGI for user:" + ugi.getUserName());
		return ugi;
	}

	public void restartRMWithTheseArgs(String rmHost, List<String> args)
			throws Exception {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh stop resourcemanager";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);

		StringBuilder sb = new StringBuilder();
		for (String anArg : args) {
			sb.append(" -D" + anArg + " ");
		}
		command = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh start resourcemanager"
				+ sb.toString();
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);

	}

	public String doJavaSSHClientExec(String user, String host, String command,
			String identityFile) {
		JSch jsch = new JSch();
		TestSession.logger.info("SSH Client is about to run command:" + command
				+ " on host:" + host + "as user:" + user
				+ " using identity file:" + identityFile);
		Session session;
		StringBuilder sb = new StringBuilder();
		try {
			session = jsch.getSession(user, host, 22);
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();

			channel.connect();

			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
					TestSession.logger.info(outputFragment);
					sb.append(outputFragment);
				}
				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: "
							+ channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {
				}
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	public class MyUserInfo implements UserInfo {

		public String getPassphrase() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptPassword(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptYesNo(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public void showMessage(String arg0) {
			// TODO Auto-generated method stub

		}

	}

	@AfterClass
	public static void restoreTheConfigFile() throws Exception {
		// Override
	}
}
