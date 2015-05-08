package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.tuple.ImmutablePair;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;


/**
 * Class that polls for the data on the grid and starts the oozie job and checks for the status of the oozie job.
 *
 */
public class DataAvailabilityPooler {
	public String directoryPath;
	public int maxPollTime;
	public String filePattern;
	public String clusterName;
	public String operationType;
	private String pipeLineInstance;
	private String oozieJobID;
	private boolean isOozieJobCompleted;
	public SearchDataAvailablity searchDataAvailablity;
	public FullyDistributedExecutor fullyDistributedExecutorObject = new FullyDistributedExecutor();
	private final static int SLA_FREQUENCY=30;
	private final static int POLL_FREQUENCY=1;
	private final static String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String FEED_INSTANCE = "20130309";
	private final static String FEED_NAME = "bidded_clicks";
	private final static String PIPE_LINE_INSTANCE = "/tmp/test_stackint/Pipeline";
	private final static String SCRATCH_PATH = "/tmp/test_stackint/pipeline_scratch";
	private final static String FEED_BASE = "/data/daqdev/abf/data/Integration_Testing_DS_";

	public DataAvailabilityPooler(int maxPollTime , String clusterName , String basePath  , String filePattern , String operationType) {
		this.maxPollTime = maxPollTime;
		this.clusterName = clusterName;
		this.directoryPath = basePath;
		this.filePattern  = filePattern;
		this.operationType = operationType;
		this.searchDataAvailablity = new SearchDataAvailablity(this.clusterName , this.directoryPath , this.filePattern , this.operationType);
	}

	public void poll() throws Exception {
		long durationPollStart=0 ,durationPollEnd=0 , perPollStartTime=0, perPollEndTime=0 , slaPollStart=0 , slaPollEnd=0;
		SimpleDateFormat durationPollSDF = new SimpleDateFormat("yyyyMMddHH");
		durationPollSDF.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar durationCalendar = Calendar.getInstance();

		// total duration for polling
		durationPollStart = Long.parseLong(durationPollSDF.format(durationCalendar.getTime()));
		durationCalendar.add(Calendar.DAY_OF_WEEK_IN_MONTH , 1);
		durationPollEnd = Long.parseLong(durationPollSDF.format(durationCalendar.getTime()));

		SimpleDateFormat perPollSDF = new SimpleDateFormat("yyyyMMddHHmm");
		durationPollSDF.setTimeZone(TimeZone.getTimeZone("UTC"));

		durationPollSDF.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar pollCalendar = Calendar.getInstance();
		perPollStartTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));
		pollCalendar.add(Calendar.MINUTE, 1);
		perPollEndTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));

		SimpleDateFormat slaPollSDF = new SimpleDateFormat("yyyyMMddHHmm");
		Calendar slaCalendar = Calendar.getInstance();
		slaPollStart = Long.parseLong(slaPollSDF.format(slaCalendar.getTime()));
		slaCalendar.add(Calendar.MINUTE, this.SLA_FREQUENCY);
		slaPollEnd = Long.parseLong(slaPollSDF.format(slaCalendar.getTime()));

		System.out.println("durationPollStart  = " + durationPollStart  + " durationPollEnd -   " + durationPollEnd);

		Calendar initialCal = Calendar.getInstance();
		initialCal = Calendar.getInstance();
		long intialMin = Long.parseLong(slaPollSDF.format(initialCal.getTime()));
		initialCal.add(Calendar.HOUR, 1);
		long futureMin =  Long.parseLong(slaPollSDF.format(initialCal.getTime()));

		FullyDistributedExecutor fullyDistributedExecutorObject = new FullyDistributedExecutor();
		boolean isDataAvailable = false , isHiveJobStarted = false;


		System.out.println("setPipeLineInstance - " + this.getPipeLineInstance() + "/" + this.getFeedResult() );
		System.out.println("setScratchPath - " + this.getScratchPath());
		System.out.println("ADD_INSTANCE_PATH - " +getPipeLineInstance() + "/" + getFeedResult() + "/integration_test_files/");
		System.out.println("ADD_INSTANCE_DATE_TIME - " + this.getCurrentFrequencyValue());
		System.out.println("ADD_INSTANCE_OUTPUT_PATH - " + getPipeLineInstance() + "/" + getFeedResult()  );
		System.out.println("ADD_INSTANCE_INPUT_PATH - " + this.getCurrentFeedBasePath());
		
		this.isOozieJobCompleted = false;
		boolean isScratchPathCreated = false;
		int count = 0;
		this.searchDataAvailablity.setState("POLLING");
		while (durationPollStart <= durationPollEnd) {

			Date d = new Date();
			perPollStartTime = Long.parseLong(perPollSDF.format(d));
			slaPollStart = Long.parseLong(slaPollSDF.format(d));
			intialMin = Long.parseLong(slaPollSDF.format(d));

			// poll for data availability for every 2 min
			if (perPollStartTime >= perPollEndTime) {
				pollCalendar = Calendar.getInstance();
				perPollStartTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));
				pollCalendar.add(Calendar.MINUTE, POLL_FREQUENCY);
				perPollEndTime =  Long.parseLong(perPollSDF.format(pollCalendar.getTime()));
				
				if (this.searchDataAvailablity.getState().toUpperCase().equals("POLLING")) {
					this.createJobPropertiesFile("/tmp/integration_test_files/");
					this.searchDataAvailablity.setPipeLineInstance(this.getPipeLineInstance() + "/" + this.getFeedResult());
					this.searchDataAvailablity.setScratchPath(this.getScratchPath());
					this.modifyWorkFlowFile("/tmp/integration_test_files");
					this.searchDataAvailablity.execute();	
				}
				
				isDataAvailable = this.searchDataAvailablity.isDataAvailableOnGrid();
				System.out.println("--- isDataAvailable  = " + isDataAvailable);
				
				if (isDataAvailable == true && this.isOozieJobCompleted == false) {
					TestSession.logger.info("--- Data for the current hour is found..!");
					
					// since data is available on the grid, create the working directory to start oozie job
					this.searchDataAvailablity.setState("WORKING_DIR");
					if (this.searchDataAvailablity.getState().toUpperCase().equals("WORKING_DIR") && this.isOozieJobCompleted == false) {
						this.searchDataAvailablity.execute();
					} 
					
					// if data is avaiable on the grid and scratchPath and pipeline instance folder are created and oozie job is not started , then start the oozie job.
					if (isDataAvailable == true  && this.searchDataAvailablity.isScratchPathCreated() == true && this.searchDataAvailablity.isPipeLineInstanceCreated() == true && this.isOozieJobCompleted == false) {
						this.searchDataAvailablity.setState("START_OOZIE_JOB");
						if ( this.searchDataAvailablity.getState().equals("START_OOZIE_JOB")) {
							this.executeCommand(this.kINIT_COMMAND);

							String oozieCommand = "/home/y/var/yoozieclient/bin/oozie job -run -config " +  this.getScratchPath() + "/integration_test_files/job.properties" + " -oozie http://dense37.blue.ygrid.yahoo.com:4080/oozie -auth kerberos";
							this.oozieJobID = this.executeCommand(oozieCommand);
							TestSession.logger.info("-- oozieJobID = " + this.oozieJobID );
							assertTrue("" , this.oozieJobID.startsWith("job:"));
							String oozieWorkFlowName = "stackint_oozie_RawInput_" + this.getCurrentFrequencyValue();
							this.isOozieJobCompleted = pollOozieJob(this.oozieJobID , oozieWorkFlowName);
							if (this.isOozieJobCompleted == false) {
								this.searchDataAvailablity.setState("DONE");
								this.isOozieJobCompleted = true;
							} else {
								this.searchDataAvailablity.setState("DONE");
								this.isOozieJobCompleted = true;
							}
						}
					}
					if (isHiveJobStarted == true) {
						TestSession.logger.info("Oozie job is started");
					}
				} 
				// reset all the value when the hour starts.
				if (intialMin >= futureMin) {
					initialCal = Calendar.getInstance();
					intialMin = Long.parseLong(durationPollSDF.format(initialCal.getTime()));
					initialCal.add(Calendar.HOUR, 1);
					futureMin =  Long.parseLong(durationPollSDF.format(initialCal.getTime()));
					initialCal = null;
					
					System.out.println("intialMin - " + intialMin   + " futureMin =   " + futureMin);

					// reset data availability flag
					isDataAvailable = false;
					this.isOozieJobCompleted = false;
					this.searchDataAvailablity.setState("POLLING");

					// set sla value for specified frequency
					slaCalendar = Calendar.getInstance();
					slaPollStart = Long.parseLong(perPollSDF.format(slaCalendar.getTime()));
					slaCalendar.add(Calendar.MINUTE, this.SLA_FREQUENCY);
					slaPollEnd =  Long.parseLong(perPollSDF.format(slaCalendar.getTime()));

					TestSession.logger.info("Next SLA will start at " + slaPollEnd);
				}
				// check for SLA
				if (slaPollStart >= slaPollEnd ) {
					if (isDataAvailable == false) {
						System.out.println("=========  Missed SLA ================");	
					}
				}
				System.out.println("perPollStartTime = " + perPollStartTime  + " perPollEndTime =   " + perPollEndTime);
				Thread.sleep(60000);
				durationPollStart = Long.parseLong(durationPollSDF.format(durationCalendar.getTime()));
				System.out.println("durationPollStart = " + durationPollStart  + " durationPollEnd =  " + durationPollEnd);
			}
		}
	}

	/**
	 * Execute a given command and return the output of the command.
	 * @param command
	 * @return
	 */
	public String executeCommand(String command) {
		String output = null;
		TestSession.logger.info("command - " + command);
		ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
		if ((result == null) || (result.getLeft() != 0)) {
			if (result != null) { // save script output to log
				TestSession.logger.info("Command exit value: " + result.getLeft());
				TestSession.logger.info(result.getRight());
			}
			throw new RuntimeException("Exception" );
		} else {
			output = result.getRight();
			TestSession.logger.info("log = " + output);
		}
		return output;
	}

	/**
	 * Returns the scratch path 
	 * @return
	 */
	public String getScratchPath() {
		return  this.SCRATCH_PATH + File.separator + this.FEED_NAME + File.separator + this.getCurrentFrequencyValue() ; 
	}

	/**
	 * Get the current frequency timing value
	 * @return
	 */
	private String getCurrentFrequencyValue() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar durationCalendar = Calendar.getInstance();
		String currentFrequency = sdf.format(durationCalendar.getTime()) + "00";
		return currentFrequency;
	}

	/**
	 * return the current feed base.
	 * @return
	 */
	public String getCurrentFeedBasePath() {
		return this.FEED_BASE + this.getCurrentFrequencyValue() ;
	}

	/**
	 * Set the values for job.properties file
	 * @param propertyFilePath
	 * @throws IOException
	 */
	private void createJobPropertiesFile(String propertyFilePath) throws IOException {
		File file = new File(propertyFilePath);
		if (file.exists() == true) {

			// read the content of the file as string.
			String fileContent = new String(readAllBytes(get(propertyFilePath + "/job.properties.tmp")));
			fileContent = fileContent.replaceAll("ADD_INSTANCE_INPUT_PATH", this.getCurrentFeedBasePath() + "/part*");
			fileContent = fileContent.replaceAll("ADD_INSTANCE_DATE_TIME", this.getCurrentFrequencyValue());
			fileContent = fileContent.replaceAll("ADD_INSTANCE_PATH",getPipeLineInstance() + "/" + getFeedResult()  + "/integration_test_files/");
			fileContent = fileContent.replaceAll("ADD_INSTANCE_OUTPUT_PATH", getPipeLineInstance() + "/" + getFeedResult() + "/"  );
			System.out.println("fileContent  = " + fileContent);

			// write the string into the file.
			java.nio.file.Files.write(java.nio.file.Paths.get(propertyFilePath + "/job.properties"), fileContent.getBytes());

		} else {
			TestSession.logger.info(propertyFilePath + " file does not exists.");
		}
	}

	/**
	 * return the value of current feed input path
	 * @return
	 */
	public String getCurrentFeedInputPath() {
		return this.getCurrentFeedBasePath() + "/" + FEED_INSTANCE + "/PAGE/Valid/News";
	}

	/**
	 * return the value of pipeline instance
	 * @return
	 */
	public String  getPipeLineInstance() {
		return PIPE_LINE_INSTANCE  + "/" + FEED_NAME + "/" + this.getCurrentFrequencyValue() ;   
	}

	/**
	 * return the value of feedResult
	 * @return
	 */
	public String getFeedResult() {
		return this.FEED_NAME + "_"   +   this.getCurrentFrequencyValue()   + "_out";
	}

	/**
	 *   Set oozie workflow name with current frequency value. 
	 */
	private void modifyWorkFlowFile(String workflowFilePath ) throws IOException {
		File f = new File(workflowFilePath);
		if (f.exists()) {
			String fileContent = new String(readAllBytes(get(workflowFilePath + "/workflow.xml.tmp")));
			fileContent = fileContent.replaceAll("stackint_oozie_RawInputETL", "stackint_oozie_RawInput_" + this.getCurrentFrequencyValue() );
			java.nio.file.Files.write(java.nio.file.Paths.get(workflowFilePath + "/workflow.xml"), fileContent.getBytes());
		} else {
			TestSession.logger.info(workflowFilePath + " file does not exists.");
		}
	}
	
	
	/*
	 * polls for oozie status of the oozie job by executing the oozie command line argument.
	 */
	public boolean pollOozieJob(String jobId , String oozieWorkFlowName) throws InterruptedException {
		boolean flag = false;
		long durationPollStart=0 ,durationPollEnd=0 , perPollStartTime=0, perPollEndTime=0 , slaPollStart=0 , slaPollEnd=0;
		Calendar pollCalendar = Calendar.getInstance();
		SimpleDateFormat perPollSDF = new SimpleDateFormat("yyyyMMddHHmm");
		pollCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		perPollStartTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));
		pollCalendar.add(Calendar.MINUTE, 1);
		perPollEndTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));
		
		String id = Arrays.asList(jobId.split(" ")).get(1).toLowerCase();
		String subId = Arrays.asList(id.split("-")).get(0).toLowerCase();
		while (perPollStartTime <= perPollEndTime) {
			String oozieCommand = "/home/y/var/yoozieclient/bin/oozie jobs -len 10 -oozie http://dense37.blue.ygrid.yahoo.com:4080/oozie -auth kerberos | grep " + "\"" + subId.trim() + "\"" ;
			String result = this.executeCommand(oozieCommand);
			TestSession.logger.info("result - " + result);
			int start = result.indexOf(jobId);
			int end = result.indexOf("dfsload");
			String jobStatus = result.substring(start + jobId.length() , end).trim();
			TestSession.logger.info("jobStatus - " + jobStatus);
			start = oozieWorkFlowName.length();
			String jobResult = jobStatus.substring(oozieWorkFlowName.length(), jobStatus.length());
			TestSession.logger.info("Job Result = " + jobResult);
			if (jobResult.equals("KILLED")) {
				TestSession.logger.info("oozie for " + jobId  + "  killed, please check the reason for job killed using http://dense37.blue.ygrid.yahoo.com:4080/oozie ");
				break;
			} else if (jobStatus.equals("SUCCEEDED")) {
				TestSession.logger.info("oozie for " + jobId  + " is SUCCEDED");
				flag = true;
				break;
			}  else {
				Thread.sleep(20000);
				Date d = new Date();
				perPollStartTime = Long.parseLong(perPollSDF.format(d));
				d = null;
				TestSession.logger.info("please wait for next polling of oozie job");
			}
		}
		return flag;
	}
}
