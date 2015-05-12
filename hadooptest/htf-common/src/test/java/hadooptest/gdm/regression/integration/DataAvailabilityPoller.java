package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.tuple.ImmutablePair;

import hadooptest.TestSession;


/**
 * Class that polls for the data on the grid and starts the oozie job and checks for the status of the oozie job.
 *
 */
public class DataAvailabilityPoller {

	public String directoryPath;
	public int maxPollTime;
	public String filePattern;
	public String clusterName;
	public String operationType;
	private String pipeLineInstance;
	private String oozieJobID;
	private boolean isOozieJobCompleted;
	private String oozieJobResult;
	private String currentFrequencyHourlyTimeStamp;
	public SearchDataAvailablity searchDataAvailablity;
	private final static int SLA_FREQUENCY=30;
	private final static int POLL_FREQUENCY=1;
	private final static String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String FEED_INSTANCE = "20130309";
	private final static String FEED_NAME = "bidded_clicks";
	private final static String PIPE_LINE_INSTANCE = "/tmp/test_stackint/Pipeline";
	private final static String SCRATCH_PATH = "/tmp/test_stackint/pipeline_scratch";
	private final static String FEED_BASE = "/data/daqdev/abf/data/Integration_Testing_DS_";

	public DataAvailabilityPoller(int maxPollTime , String clusterName , String basePath  , String filePattern , String operationType) {
		this.maxPollTime = maxPollTime;
		this.clusterName = clusterName;
		this.directoryPath = basePath;
		this.filePattern  = filePattern;
		this.operationType = operationType;
		this.searchDataAvailablity = new SearchDataAvailablity(this.clusterName , this.directoryPath , this.filePattern , this.operationType);
	}

	public void dataPoller() throws InterruptedException, IOException{

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar todayCal = Calendar.getInstance();
		Calendar LastdayCal = Calendar.getInstance();
		Calendar currentCal = Calendar.getInstance();

		long toDay = Long.parseLong(sdf.format(todayCal.getTime()));

		// set the duration for how long the data has to generate.
		LastdayCal.add(Calendar.DAY_OF_WEEK_IN_MONTH , 1);
		long lastDay = Long.parseLong(sdf.format(LastdayCal.getTime()));
		System.out.println(" Current date - "+ sdf.format(todayCal.getTime()));
		System.out.println(" Next date - "+ sdf.format(LastdayCal.getTime()));

		Calendar initialCal = Calendar.getInstance();
		Calendar futureCal = Calendar.getInstance();

		long intialMin = Long.parseLong(sdf.format(initialCal.getTime()));
		initialCal.add(Calendar.MINUTE, 1);
		long futureMin =  Long.parseLong(sdf.format(initialCal.getTime()));
		System.out.println(" intialMin   = " +  intialMin   + "  futureMin  =  "  + futureMin);
		SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
		feed_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat slaSDF = new SimpleDateFormat("yyyyMMddHHmm");
		slaSDF.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar salStartCal = Calendar.getInstance();
		Calendar salEndCal = Calendar.getInstance();
		long slaStart =  Long.parseLong(sdf.format(initialCal.getTime()));
		initialCal.add(Calendar.MINUTE, 1);
		long slaEnd = Long.parseLong(sdf.format(initialCal.getTime()));
		long pollEnd =  Long.parseLong(sdf.format(initialCal.getTime()));
		Calendar pollCalendar = Calendar.getInstance();

		boolean isDataAvailable = false;
		this.isOozieJobCompleted = false;

		while (toDay <= lastDay) {
			Date d = new Date();
			long initTime = Long.parseLong(sdf.format(d));
			long slaStartTime = Long.parseLong(slaSDF.format(d));
			long pollStart = Long.parseLong(slaSDF.format(d));

			if (initTime >= futureMin ) {
				initialCal = Calendar.getInstance();
				intialMin = Long.parseLong(feed_sdf.format(initialCal.getTime()));
				initialCal.add(Calendar.HOUR, 1);
				futureMin =  Long.parseLong(feed_sdf.format(initialCal.getTime()));

				System.out.println("------- hr has started..! -------------");

				Calendar currentTimeStampCal = Calendar.getInstance();
				String currentHrFrequency = feed_sdf.format(currentTimeStampCal.getTime());
				this.setCurrentFrequencyValue(currentHrFrequency);
				TestSession.logger.info("-----  Starting new data availability for Frequency - " + currentHrFrequency);
				this.isOozieJobCompleted = false;

				salStartCal = Calendar.getInstance();
				salStartCal.add(Calendar.MINUTE, 20);
				slaEnd = Long.parseLong(slaSDF.format(salStartCal.getTime()));
				System.out.println("SLA will start at - " + slaEnd  + " now  - " + slaStartTime);

				initialCal = null;
				salStartCal = null;

				// create the working directories
				this.searchDataAvailablity.setState("WORKING_DIR");
				if (this.searchDataAvailablity.getState().toUpperCase().equals("WORKING_DIR") && this.isOozieJobCompleted == false) {

					this.createJobPropertiesFile("/tmp/integration_test_files/");
					this.searchDataAvailablity.setPipeLineInstance(this.getPipeLineInstance() + "/" + this.getFeedResult());
					this.searchDataAvailablity.setScratchPath(this.getScratchPath());
					this.modifyWorkFlowFile("/tmp/integration_test_files");
					this.searchDataAvailablity.setOozieWorkFlowPath(this.getOozieWfApplicationPath());
					this.searchDataAvailablity.execute();

					// once working directory is created successfully for the given hr, set state to polling for data availability 
					this.searchDataAvailablity.setState("POLLING");
				}
			}

			if (slaStartTime >= slaEnd  && isDataAvailable == false) {
				System.out.println("*************************************************************************** ");
				System.out.println(" \t MISSED SLA for " + this.getCurrentFrequencyValue() );
				System.out.println("*************************************************************************** ");
			}
			if (pollStart >= pollEnd) {
				pollCalendar = Calendar.getInstance();
				pollStart = Long.parseLong(slaSDF.format(pollCalendar.getTime()));
				pollCalendar.add(Calendar.MINUTE, 1);
				pollEnd =  Long.parseLong(slaSDF.format(pollCalendar.getTime()));

				// check for data availability
				isDataAvailable = this.searchDataAvailablity.isDataAvailableOnGrid();
				if (isDataAvailable == false) {
					System.out.println("polling for data..!");
					this.searchDataAvailablity.execute();	
				}

				if (isDataAvailable == true &&  this.searchDataAvailablity.getState().toUpperCase().equals("DONE")) {
					if (this.oozieJobResult.toUpperCase().equals("KILLED")) {
						TestSession.logger.info("Data Available on the grid for  "+  this.getCurrentFrequencyValue()  + "  and oozie got processed, but " + this.oozieJobResult);
					} else if (this.oozieJobResult.toUpperCase().equals("SUCCEEDED")) {
						TestSession.logger.info("Data Available on the grid for  "+  this.getCurrentFrequencyValue()  + "  and oozie got processed & " + this.oozieJobResult);
					}
				} 

				if (isDataAvailable == true  && this.searchDataAvailablity.isScratchPathCreated() == true && this.searchDataAvailablity.isPipeLineInstanceCreated() == true && this.isOozieJobCompleted == false) {
					TestSession.logger.info("*** Data for the current hour is found..!  ***");

					// set state to START_OOZIE_JOB
					this.searchDataAvailablity.setState("START_OOZIE_JOB");
					if ( this.searchDataAvailablity.getState().equals("START_OOZIE_JOB")) {
						this.executeCommand(this.kINIT_COMMAND);

						String oozieCommand = "/home/y/var/yoozieclient/bin/oozie job -run -config " +  this.getScratchPath() + "/integration_test_files/job.properties" + " -oozie http://dense37.blue.ygrid.yahoo.com:4080/oozie -auth kerberos";
						this.oozieJobID = this.executeCommand(oozieCommand);
						TestSession.logger.info("-- oozieJobID = " + this.oozieJobID );
						assertTrue("" , this.oozieJobID.startsWith("job:"));
						String oozieWorkFlowName = "stackint_oozie_RawInput_" + this.getCurrentFrequencyValue();
						this.oozieJobResult = pollOozieJob(this.oozieJobID , oozieWorkFlowName);
						if (this.isOozieJobCompleted == false) {
							this.searchDataAvailablity.setState("DONE");
							this.isOozieJobCompleted = true;
						} else {
							this.searchDataAvailablity.setState("DONE");
							this.isOozieJobCompleted = true;
						}
					}
				}
			}
			Thread.sleep(60000);
			d = new Date();
			initTime = Long.parseLong(feed_sdf.format(d));
			d = null;
			toDay = Long.parseLong(feed_sdf.format(currentCal.getTime()));

			TestSession.logger.info("Next data polling will start @ " + futureMin   + "  and  current  time = " + initTime);
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
	 * Set the values for job.properties file
	 * @param propertyFilePath
	 * @throws IOException
	 */
	private void createJobPropertiesFile(String propertyFilePath) throws IOException {
		File file = new File(propertyFilePath);
		if (file.exists() == true) {

			// read the content of the file as string.
			String fileContent = new String(readAllBytes(get(propertyFilePath + "/job.properties.tmp")));
			fileContent = fileContent.replaceAll("ADD_INSTANCE_INPUT_PATH", this.getCurrentFeedBasePath() + "/20130309/PAGE/Valid/News/part*");
			fileContent = fileContent.replaceAll("ADD_INSTANCE_DATE_TIME", this.getCurrentFrequencyValue());
			//fileContent = fileContent.replaceAll("ADD_INSTANCE_PATH",getPipeLineInstance() + "/" + getFeedResult()  + "/integration_test_files/");
			fileContent = fileContent.replaceAll("ADD_INSTANCE_PATH", this.getOozieWfApplicationPath() +  "/integration_test_files/");
			fileContent = fileContent.replaceAll("ADD_INSTANCE_OUTPUT_PATH", this.getPipeLineInstance() + "/" + this.getFeedResult() + "/"  );
			System.out.println("fileContent  = " + fileContent);

			// write the string into the file.
			java.nio.file.Files.write(java.nio.file.Paths.get(propertyFilePath + "/job.properties"), fileContent.getBytes());
		} else {
			TestSession.logger.info(propertyFilePath + " file does not exists.");
		}
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

	/**
	 * polls for oozie status of the oozie job by executing the oozie command line argument.
	 * @param jobId
	 * @param oozieWorkFlowName
	 * @return
	 * @throws InterruptedException
	 */
	public String pollOozieJob(String jobId , String oozieWorkFlowName) throws InterruptedException {
		String oozieJobresult = "FAILED";
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
				oozieJobresult = "KILLED";
				this.isOozieJobCompleted = true;
				break;
			} else if (jobStatus.equals("SUCCEEDED")) {
				TestSession.logger.info("oozie for " + jobId  + " is SUCCEDED");
				this.isOozieJobCompleted = true;
				oozieJobresult = "SUCCEEDED";
				break;
			}  else {
				Thread.sleep(20000);
				Date d = new Date();
				perPollStartTime = Long.parseLong(perPollSDF.format(d));
				d = null;
				TestSession.logger.info("please wait for next polling of oozie job");
			}
		}
		return oozieJobresult;
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
	 * Returns the scratch path 
	 * @return
	 */
	public String getScratchPath() {
		return  this.SCRATCH_PATH + File.separator + this.FEED_NAME + File.separator + this.getCurrentFrequencyValue() ; 
	}
	
	public String getOozieWfApplicationPath() {
		return  this.PIPE_LINE_INSTANCE + File.separator + this.FEED_NAME + File.separator + this.getCurrentFrequencyValue();
	}

	/**
	 * Get the current frequency timing value
	 * @return
	 */
	private String getCurrentFrequencyValue() {
		return currentFrequencyHourlyTimeStamp ;
	}

	private void setCurrentFrequencyValue(String currentFrequencyHourlyTimeStamp) {
		this.currentFrequencyHourlyTimeStamp = currentFrequencyHourlyTimeStamp + "00";
		System.out.println("current currentFrequencyHourlyTimeStamp - " + this.currentFrequencyHourlyTimeStamp);
	}

	/**
	 * return the current feed base.
	 * @return
	 */
	public String getCurrentFeedBasePath() {
		return this.FEED_BASE + this.getCurrentFrequencyValue() ;
	}

}
