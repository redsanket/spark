package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class YarnTestsBaseClass extends TestSession {
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public final String KRB5CCNAME = "KRB5CCNAME";
	protected String localCluster = System.getProperty("CLUSTER_NAME");

	public static enum YarnAdminSubCommand {
		REFRESH_QUEUES, REFRESH_NODES, REFRESH_SUPERUSER_GROUPS_CONFIGURATION, REFRESH_USER_TO_GROUPS_MAPPING, REFRESH_ADMIN_ACLS, REFRESH_SERVICE_ACL, GET_GROUPS
	};

	public void killAllJobs() throws IOException, InterruptedException {
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			cluster.getJob(aJobStatus.getJobID()).killJob();
		}

	}

	public Future<Job> submitSingleSleepJobAndGetHandle(String queueToSubmit,
			String username, HashMap<String, String> jobParamsMap,
			int numMapper, int numReducer, int mapSleepTime, int mapSleepCount,
			int reduceSleepTime, int reduceSleepCount, String jobName,
			boolean expectedToBomb) {
		Future<Job> jobHandle = null;
		if (queueToSubmit.isEmpty() || queueToSubmit.equalsIgnoreCase("")) {
			queueToSubmit = "default";
		}
		if (jobParamsMap == null) {
			jobParamsMap = getDefaultSleepJobProps(queueToSubmit);
		}
		CallableSleepJob callableSleepJob = new CallableSleepJob(jobParamsMap,
				numMapper, numReducer, mapSleepTime, mapSleepCount,
				reduceSleepTime, reduceSleepCount, username, jobName,
				expectedToBomb);

		ExecutorService singleLanePool = Executors.newFixedThreadPool(1);
		jobHandle = singleLanePool.submit(callableSleepJob);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return jobHandle;
	}

	public HashMap<String, String> getDefaultSleepJobProps(String queueToSubmit) {
		HashMap<String, String> defaultSleepJobProps = new HashMap<String, String>();
		defaultSleepJobProps.put("mapreduce.job.acl-view-job", "*");
		defaultSleepJobProps.put("mapreduce.map.memory.mb", "1024");
		defaultSleepJobProps.put("mapreduce.reduce.memory.mb", "1024");
		if (queueToSubmit.isEmpty() || queueToSubmit.equalsIgnoreCase("")) {
			queueToSubmit = "default";
		}
		defaultSleepJobProps.put("mapred.job.queue.name", queueToSubmit);

		return defaultSleepJobProps;
	}

	public class CallableSleepJob implements Callable<Job> {
		HashMap<String, String> jobParamsMap;
		int numMapper;
		int numReducer;
		int mapSleepTime;
		int mapSleepCount;
		int reduceSleepTime;
		int reduceSleepCount;
		String userName;
		String jobName;
		boolean expectedToBomb;

		public CallableSleepJob(HashMap<String, String> jobParamsMap,
				int numMapper, int numReducer, int mapSleepTime,
				int mapSleepCount, int reduceSleepTime, int reduceSleepCount,
				String userName, String jobName, boolean expectedToBomb) {
			this.jobParamsMap = jobParamsMap;
			this.numMapper = numMapper;
			this.numReducer = numReducer;
			this.mapSleepTime = mapSleepTime;
			this.mapSleepCount = mapSleepCount;
			this.reduceSleepTime = reduceSleepTime;
			this.reduceSleepCount = reduceSleepCount;
			this.userName = userName;
			this.jobName = jobName;
			this.expectedToBomb = expectedToBomb;

		}

		public Job call() {
			Job createdSleepJob = null;
			Configuration conf = TestSession.cluster.getConf();
			for (String key : jobParamsMap.keySet()) {
				conf.set(key, jobParamsMap.get(key));
			}

			try {
				TestSession.cluster.setSecurityAPI("keytab-" + userName,
						"user-" + userName);

				SleepJob sleepJob = new SleepJob();
				sleepJob.setConf(TestSession.cluster.getConf());

				createdSleepJob = sleepJob.createJob(numMapper, numReducer,
						mapSleepTime, mapSleepCount, reduceSleepTime,
						reduceSleepCount);
				createdSleepJob.setJobName(jobName);
				TestSession.logger
						.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% "
								+ "submitting " + jobName
								+ " %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

				createdSleepJob.submit();

			} catch (Exception e) {
				if (this.expectedToBomb) {
					TestSession.logger.fatal("YAY the job faile as expected "
							+ jobName);

				} else {
					TestSession.logger.fatal("Submission of jobid [" + jobName
							+ "] via API barfed... !! expected exception "
							+ this.expectedToBomb);
					e.printStackTrace();
					TestSession.logger.info(e.getStackTrace());
					Assert.fail("Submission of jobid [" + jobName
							+ "] via API barfed... !! expected exception "
							+ this.expectedToBomb);
				}

			}
			return createdSleepJob;
		}
	}

	/*
	 * Run a Sleep Job, from org.apache.hadoop.mapreduce
	 */
	public void runStdSleepJob(HashMap<String, String> jobParams, String[] args)
			throws Exception {
		TestSession.logger.info("running SleepJob.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new SleepJob(), args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			TestSession.logger.error("SleepJob barfed...");
			throw e;
		}

	}

	/*
	 * Run a RandomWriter Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopRandomWriter(HashMap<String, String> jobParams,
			String randomWriterOutputDirOnHdfs) throws Exception {
		TestSession.logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new RandomWriter(),
					new String[] { randomWriterOutputDirOnHdfs });
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		TestSession.logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res;

		try {
			res = ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {

			sortInputDataLocation, sortOutputDataLocation });
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	public void runStdHadoopStreamingJob(String... args) throws Exception {
		TestSession.logger.info("running Streaming Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res;

		try {
			StreamJob job = new StreamJob();
			res = ToolRunner.run(conf, job, args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		if (node == null) {
			return "";
		} else {
			return node.getNodeValue();
		}
	}

	public static String lookupValueInBackCopiedCapacitySchedulerXmlFile(
			String filename, String propertyName)
			throws ParserConfigurationException, SAXException, IOException,
			TransformerException, TransformerConfigurationException {

		boolean foundPropName = false;
		String valueToReturn = "";
		TestSession.logger.info("Looking up value in file:" + filename);
		/*
		 * Parse the XML configuration file using a DOM parser
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: "
				+ document.getDocumentElement().getNodeName());

		Element element = null;
		NodeList nodes = document.getElementsByTagName("property");
		for (int index = 0; index < nodes.getLength(); index++) {
			Node node = nodes.item(index);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				element = (Element) node;

				String propName = getValue("name", element);

				TestSession.logger.trace("Config Property Name: "
						+ getValue("name", element));
				TestSession.logger.trace("Config Property Value: "
						+ getValue("value", element));

				if (propName.equals(propertyName)) {
					foundPropName = true;
					TestSession.logger
							.info("Found what I was looking for aka ["
									+ propertyName + "] returning");
					valueToReturn = getValue("value", element);
				}
			}
		}

		if (foundPropName == false) {
			return "";
		} else {
			return valueToReturn;
		}

	}

	void waitTillJobStartsRunning(Job job) throws IOException,
			InterruptedException {
		State jobState = job.getStatus().getState();
		while (jobState != State.RUNNING) {
			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
				break;
			}
			Thread.sleep(1000);
			jobState = job.getStatus().getState();
			TestSession.logger
					.info(job.getJobName()
							+ " is in state : "
							+ jobState
							+ ", awaiting its state to change to 'RUNNING' hence sleeping for 1 sec");
		}
	}

	void waitTillJobSucceeds(Job job) throws IOException, InterruptedException {

		State jobState = job.getStatus().getState();
		while (jobState != State.SUCCEEDED) {
			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
				break;
			}
			Thread.sleep(1000);
			jobState = job.getStatus().getState();
			TestSession.logger
					.info("Job "
							+ job.getJobName()
							+ " is in state : "
							+ jobState
							+ ", awaiting its state to change to 'SUCCEEDED' hence sleeping for 1 sec");
		}
	}

	void waitTillAnyTaskGetsToState(TIPStatus expectedState, Job job,
			TaskType taskType, int maxWaitInSecs) throws IOException,
			InterruptedException {
		boolean done = false;
		int tick = 0;
		do {
			Thread.sleep(1000);
			for (TaskReport aTaskReport : job.getTaskReports(taskType)) {
				if (aTaskReport.getCurrentStatus() == expectedState) {
					done = true;
					TestSession.logger.info("There task " + taskType
							+ " has reached expected state ==" + expectedState);
				} else {
					done = false;
					TestSession.logger.info(taskType + " task "
							+ aTaskReport.getTaskId() + " is in state "
							+ aTaskReport.getCurrentStatus()
							+ " & not expected state of " + expectedState
							+ " hence waiting for (" + (maxWaitInSecs - tick)
							+ " seconds) more....tick!");

				}
			}
			tick++;
		} while (!done && tick < maxWaitInSecs);
	}

	@Override
	public void logTaskReportSummary() {
		// Override the job summary, as it is not required
	}

}
