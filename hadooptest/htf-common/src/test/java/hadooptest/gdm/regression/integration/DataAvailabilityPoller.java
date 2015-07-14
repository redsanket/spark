package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.base.Splitter;

import hadooptest.TestSession;
import hadooptest.gdm.regression.integration.metrics.NameNodeDFSMemoryDemon;
import hadooptest.gdm.regression.integration.metrics.NameNodeTheadDemon;

/**
 * Class that polls for the data on the grid and starts the oozie job and checks for the status of the oozie job.
 *
 */
public class DataAvailabilityPoller {

	public String directoryPath;
	public int maxPollTime;
	private boolean isOozieJobCompleted;
	private int pullOozieJobLength;
	public String filePattern;
	public String clusterName;
	public String operationType;
	private String oozieJobID;
	private String oozieHostName;
	private String hcatHostName;
	private String oozieJobResult;
	private String currentFrequencyHourlyTimeStamp;
	private String currentFeedName;
	private Connection con;
	public SearchDataAvailablity searchDataAvailablity;
	private NameNodeTheadDemon nameNodeTheadDemonObject;
	private NameNodeDFSMemoryDemon nameNodeDFSMemoryDemonObject;
	private DataBaseOperations dbOperations;
	private final static String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String FEED_INSTANCE = "20130309";
	private final static String FEED_NAME = "bidded_clicks";
	private final static String PIPE_LINE_INSTANCE = "/tmp/test_stackint/Pipeline";
	private final static String SCRATCH_PATH = "/tmp/test_stackint-htf/pipeline_scratch";
	private final static String FEED_BASE = "/data/daqdev/abf/data/Integration_Testing_DS_";
	private final static String OOZIE_COMMAND = "/home/y/var/yoozieclient/bin/oozie";
	private final static String HIVE_SITE_FILE_LOCATION = "/home/y/libexec/hive/conf/hive-site.xml";
	private final static Map <String,String> columnName = new HashMap<String,String>(); 
	
	
	private void setCurrentFeedName(String feedName) {
		this.currentFeedName = "Integration_Testing_DS_" + this.getCurrentFrequencyValue() ;
	}
	
	public DataAvailabilityPoller(int maxPollTime , String clusterName , String basePath  , String filePattern , String operationType , String oozieHostName , String hcatHostName , String pullOozieJobLength) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.maxPollTime = maxPollTime;
		this.clusterName = clusterName;
		this.directoryPath = basePath;
		this.filePattern  = filePattern;
		this.operationType = operationType;
		this.oozieHostName = oozieHostName;
		this.hcatHostName = hcatHostName;
		this.pullOozieJobLength = Integer.parseInt(pullOozieJobLength);
		this.searchDataAvailablity = new SearchDataAvailablity(this.clusterName , this.directoryPath , this.filePattern , this.operationType);
	}

	public void dataPoller() throws InterruptedException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		columnName.put("cleanup_output", "cleanUpOutput");
		columnName.put("check_input", "checkInput");
		columnName.put("pig_raw_processor", "pigRawProcessor");
		columnName.put("hive_storage", "hiveStorage");
		columnName.put("hive_verify", "hiveVerify");
		columnName.put("end", "jobEnded");

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

		long initialMin = Long.parseLong(sdf.format(initialCal.getTime()));
		initialCal.add(Calendar.MINUTE, 1);
		long futureMin =  Long.parseLong(sdf.format(initialCal.getTime()));
		System.out.println(" intialMin   = " +  initialMin   + "  futureMin  =  "  + futureMin);
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
		
		this.dbOperations = new DataBaseOperations();
		this.dbOperations.createDB();
		this.dbOperations.createTable();
		this.dbOperations.createNameNodeThreadInfoTable();
		this.dbOperations.createNameNodeMemoryInfoTable();
		
		// start namenode thread demon
		this.nameNodeTheadDemonObject = new NameNodeTheadDemon();
		this.nameNodeTheadDemonObject.startNameNodeTheadDemon();
		
		// start namenode dfs memory demon
		this.nameNodeDFSMemoryDemonObject = new NameNodeDFSMemoryDemon();
		this.nameNodeDFSMemoryDemonObject.startNameNodeDFSMemoryDemon();

		while (toDay <= lastDay) {
			Date d = new Date();
			long initTime = Long.parseLong(sdf.format(d));
			long slaStartTime = Long.parseLong(slaSDF.format(d));
			long pollStart = Long.parseLong(slaSDF.format(d));

			if (initTime >= futureMin ) {
				initialCal = Calendar.getInstance();
				initialMin = Long.parseLong(feed_sdf.format(initialCal.getTime()));
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
				
				this.currentFeedName = "Integration_Testing_DS_" + this.getCurrentFrequencyValue();
				
				// set the current feed name
			 	this.searchDataAvailablity.setCurrentFeedName(this.currentFeedName);
			 	if (this.con == null) {
			 		this.con = this.dbOperations.getConnection();	
			 	}
			 	
			 	if (this.con != null ) {
			 		this.con.close();
			 		this.con = this.dbOperations.getConnection();
			 	}
				
				this.dbOperations.createDB();
				this.dbOperations.createTable();
				this.dbOperations.createNameNodeThreadInfoTable();
				this.dbOperations.createNameNodeMemoryInfoTable();
				
				// job started.
				this.searchDataAvailablity.setState(IntegrationJobSteps.JOB_STARTED);
				
				// add start state to the user stating that job has started for the current frequency.
				this.dbOperations.insertRecord(this.currentFeedName, "hourly", JobState.STARTED,  String.valueOf(initTime), this.searchDataAvailablity.getState().toUpperCase().trim());

				initialCal = null;
				salStartCal = null;

				// create the working directories
				this.searchDataAvailablity.setState("WORKING_DIR");
				if (this.searchDataAvailablity.getState().toUpperCase().equals("WORKING_DIR") && this.isOozieJobCompleted == false) {
										
					// forcing the system to create the status folder for every hour.
					this.searchDataAvailablity.setCurrentStatusFolderName("");
					
					// copy hive-site.xml file from hcat server to the local host where HTF is running
					this.copyHiveSiteXML( );
					this.createJobPropertiesFile("/tmp/integration_test_files/");
					this.searchDataAvailablity.setPipeLineInstance(this.getPipeLineInstance() + "/" + this.getFeedResult());
					this.searchDataAvailablity.setScratchPath(this.getScratchPath());
					this.modifyWorkFlowFile("/tmp/integration_test_files");
					this.searchDataAvailablity.setOozieWorkFlowPath(this.getOozieWfApplicationPath());
					this.searchDataAvailablity.setCurrentFrequencyValue(this.currentFrequencyHourlyTimeStamp);
					this.searchDataAvailablity.execute();

					// once working directory is created successfully for the given hr, set state to polling for data availability 
					this.searchDataAvailablity.setState("POLLING");
					this.dbOperations.updateRecord(this.con , "dataAvailable" , "POLLING" , "currentStep" , "dataAvailable" , this.currentFeedName);
				}
			}

			if (slaStartTime >= slaEnd  && isDataAvailable == false) {
				System.out.println("*************************************************************************** ");
				System.out.println(" \t MISSED SLA for " + this.getCurrentFrequencyValue() );
				this.dbOperations.updateRecord(this.con , "dataAvailable" ,IntegrationJobSteps.MISSED_SLA , "currentStep" , "dataAvailable" , "result" , "FAIL" ,this.currentFeedName);
				System.out.println("*************************************************************************** ");
			}
			System.out.println("Current state = " + this.searchDataAvailablity.getState());
			System.out.println("pollStart = " + pollStart  + " pollEnd =   " + pollEnd);
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
					String state = this.searchDataAvailablity.getState();
					this.dbOperations.updateRecord(this.con , "dataAvailable" , state , "currentStep" , state , this.currentFeedName);
				}

				if (isDataAvailable == true &&  this.searchDataAvailablity.getState().toUpperCase().equals("DONE")) {
					if (this.oozieJobResult.toUpperCase().equals("KILLED")) {
						TestSession.logger.info("Data Available on the grid for  "+  this.getCurrentFrequencyValue()  + "  and oozie got processed, but " + this.oozieJobResult);
						this.dbOperations.updateRecord(this.con , "result" , "FAIL"  , this.currentFeedName);
					} else if (this.oozieJobResult.toUpperCase().equals("SUCCEEDED")) {
						TestSession.logger.info("Data Available on the grid for  "+  this.getCurrentFrequencyValue()  + "  and oozie got processed & " + this.oozieJobResult);
						this.dbOperations.updateRecord(this.con , "result" , "PASS"  , this.currentFeedName);
					}
				}
				System.out.println("currents state - " + this.searchDataAvailablity.getState());
				System.out.println("isPipeLineInstanceCreated = " + this.searchDataAvailablity.isPipeLineInstanceCreated());
				System.out.println("isOozieJobCompleted - " + this.isOozieJobCompleted);
				if (this.searchDataAvailablity.getState().equals("AVAILABLE") == true  && this.searchDataAvailablity.isPipeLineInstanceCreated() == true && this.isOozieJobCompleted == false) {

					TestSession.logger.info("*** Data for the current hour is found..!  ***");
					
					// update db saying that data is AVAILABLE
					this.dbOperations.updateRecord(this.con , "dataAvailable" , "AVAILABLE" , "currentStep" , "dataAvailable", this.currentFeedName);
					
					TestSession.logger.info("dataCollectorHostName  = " + hcatHostName);
					String command = "scp "  + "/tmp/" + this.currentFrequencyHourlyTimeStamp + "-job.properties"  + "   " + hcatHostName + ":/tmp/";
					String outputResult = this.executeCommand(command);
					if (outputResult.contains(this.currentFrequencyHourlyTimeStamp + "-job.properties")) {
						TestSession.logger.info(this.currentFrequencyHourlyTimeStamp + "-job.properties" + "  file copied successfully.");
					} else {
						TestSession.logger.info("failed to copy " + this.currentFrequencyHourlyTimeStamp + "-job.properties  to " + hcatHostName );
					}

					// set state to START_OOZIE_JOB
					this.searchDataAvailablity.setState("START_OOZIE_JOB");
					if ( this.searchDataAvailablity.getState().equals("START_OOZIE_JOB")) {
						
						String oozieCommand = "ssh " + this.oozieHostName + "   \" " + this.kINIT_COMMAND + ";"  +   OOZIE_COMMAND + " job -run -config " +  "/tmp/" + this.currentFrequencyHourlyTimeStamp + "-job.properties" + " -oozie " + "http://" + this.oozieHostName + ":4080/oozie -auth kerberos"   + " \"";
						TestSession.logger.info("oozieCommand  = " + oozieCommand);

						this.oozieJobID = this.executeCommand(oozieCommand);
						TestSession.logger.info("-- oozieJobID = " + this.oozieJobID );
						assertTrue("" , this.oozieJobID.startsWith("job:"));
						
						// update db saying that oozie job is started.
						this.dbOperations.updateRecord(this.con , "oozieJobStarted" , "STARTED" , "currentStep" , "oozieJobStarted" , this.currentFeedName);
						
						String oozieWorkFlowName = "stackint_oozie_RawInput_" + this.getCurrentFrequencyValue();
						this.oozieJobResult = this.pollOozieJob(this.oozieJobID , oozieWorkFlowName);
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
			
			if (this.searchDataAvailablity.getState().equals("AVAILABLE") == true  && this.searchDataAvailablity.isPipeLineInstanceCreated() == true && this.isOozieJobCompleted == true) {
				if ( this.searchDataAvailablity.getState().toUpperCase().equals("END") ) {
					this.dbOperations.updateRecord(this.con ,   "result" , "PASS"  , this.currentFeedName);
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
			if (result != null) { 
				// save script output to log
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

		String absolutePath = new File("").getAbsolutePath();
		System.out.println("Absolute Path =  " + absolutePath);
		File integrationFilesPath = new File(absolutePath + "/resources/stack_integration/");
		if (integrationFilesPath.exists()) {

			TestSession.logger.info(integrationFilesPath.toString() + " path exists.");
			String tempJobPropertiesFilePath = integrationFilesPath + "/job.properties.tmp";
			File file = new File(tempJobPropertiesFilePath);
			if (file.exists()) {
				String fileContent = new String(readAllBytes(get(tempJobPropertiesFilePath)));
				fileContent = fileContent.replaceAll("ADD_INSTANCE_INPUT_PATH", this.getCurrentFeedBasePath() + "/20130309/PAGE/Valid/News/part*");
				fileContent = fileContent.replaceAll("ADD_INSTANCE_DATE_TIME", this.getCurrentFrequencyValue());
				fileContent = fileContent.replaceAll("ADD_INSTANCE_PATH", this.getOozieWfApplicationPath() );
				fileContent = fileContent.replaceAll("ADD_INSTANCE_OUTPUT_PATH", this.getPipeLineInstance() + "/" + this.getFeedResult() + "/"  );
				System.out.println("fileContent  = " + fileContent);

				// write the string into the file.
				String jobPropertiesFilePath = integrationFilesPath + "/job.properties";

				// check if already job.properties file exists
				File jobPropertyFile = new File(jobPropertiesFilePath);
				if (jobPropertyFile.exists() == true) {
					TestSession.logger.info(jobPropertiesFilePath + "  file exists  ********** " );
					if (jobPropertyFile.delete() == true ) {
						TestSession.logger.info(jobPropertiesFilePath + "  file deleted successfully  **************** ");
						java.nio.file.Files.write(java.nio.file.Paths.get(jobPropertiesFilePath), fileContent.getBytes());
						TestSession.logger.info("Successfully " + jobPropertiesFilePath + " created.   ****************** ");		
					} else {
						TestSession.logger.info("Failed to delete " + jobPropertiesFilePath);
					}
				} else {
					TestSession.logger.info(jobPropertiesFilePath + " does not exists");
					java.nio.file.Files.write(java.nio.file.Paths.get(jobPropertiesFilePath), fileContent.getBytes());
				}
			} else {
				TestSession.logger.info(tempJobPropertiesFilePath + " file does not exist.");
			}
		} else {
			TestSession.logger.info(integrationFilesPath + " file does not exists.");
		}
	}

	/**
	 *   Set oozie workflow name with current frequency value. 
	 */
	private void modifyWorkFlowFile(String workflowFilePath ) throws IOException {
		String absolutePath = new File("").getAbsolutePath();
		System.out.println("Absolute Path =  " + absolutePath);
		File integrationFilesPath = new File(absolutePath + "/resources/stack_integration/");
		if (integrationFilesPath.exists()) {
			TestSession.logger.info(integrationFilesPath.toString() + " path exists.");
			String fileContent = new String(readAllBytes(get(integrationFilesPath + "/workflow.xml.tmp")));
			fileContent = fileContent.replaceAll("stackint_oozie_RawInputETL", "stackint_oozie_RawInput_" + this.getCurrentFrequencyValue() );

			String workFlowFilePath = integrationFilesPath + "/workflow.xml";
			File workFlowFile = new File(workFlowFilePath);
			if (workFlowFile.exists() == true) {
				TestSession.logger.info(workFlowFilePath + "  file exists************** ");
				if (workFlowFile.delete() == true) {
					TestSession.logger.info(workFlowFilePath + "  file deleted successfully *********** ");
					java.nio.file.Files.write(java.nio.file.Paths.get(workFlowFilePath), fileContent.getBytes());
					TestSession.logger.info("Successfully " + workFlowFilePath + " created. *************");
				} else {
					TestSession.logger.info("Failed to delete " + workFlowFilePath);	
				}
			} else {
				TestSession.logger.info(workFlowFilePath + " does not exists");
				java.nio.file.Files.write(java.nio.file.Paths.get(workFlowFilePath), fileContent.getBytes());
			}
		}else {
			TestSession.logger.info(workflowFilePath + " file does not exists.");
		}
	}

	/**
	 * polls for oozie status of the oozie job by executing the oozie command line argument.
	 * @param jobId
	 * @param oozieWorkFlowName
	 * @return
	 * @throws InterruptedException
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public String pollOozieJob(String jobId , String oozieWorkFlowName) throws InterruptedException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String oozieJobresult = "FAILED";
		Date d ;
		long durationPollStart=0 ,durationPollEnd=0 , perPollStartTime=0, perPollEndTime=0 , slaPollStart=0 , slaPollEnd=0;
		Calendar pollCalendar = Calendar.getInstance();
		SimpleDateFormat perPollSDF = new SimpleDateFormat("yyyyMMddHHmm");
		pollCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		perPollStartTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));

		// set the poll timing for max 5 mins 
		pollCalendar.add(Calendar.MINUTE, 5);
		perPollEndTime = Long.parseLong(perPollSDF.format(pollCalendar.getTime()));

		String id = Arrays.asList(jobId.split(" ")).get(1).toLowerCase();
		String subId = Arrays.asList(id.split("-")).get(0).toLowerCase();
		while (perPollStartTime <= perPollEndTime) {
			String oozieCommand = "ssh " + this.oozieHostName +  " \"" + this.OOZIE_COMMAND + " jobs -len " + this.pullOozieJobLength + " -oozie " + "http://" + this.oozieHostName + ":4080/oozie -auth kerberos | grep " + "\"" + subId.trim() + "\"" + "\"" ;
			TestSession.logger.info("oozieCommand poll status command - " + oozieCommand);
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
				TestSession.logger.info("oozie for " + jobId  + "  killed, please check the reason for job killed using" + this.oozieHostName + ":4080/oozie ");
				oozieJobresult = "KILLED";
				this.searchDataAvailablity.setCurrentWorkingState("OOZIE:JOB_KILLED");
				String state = this.searchDataAvailablity.getState();
				String mrValue = this.executeOozieCurlCommand(jobId);
				this.dbOperations.updateRecord(this.con ,  "status" , "KILLED" , this.columnName.get(state) , "KILLED" + "~" + mrValue , "result" , "FAIL"  , this.currentFeedName);
				
				// mark the testcase as failed
				fail( this.currentFeedName + " failed in " + this.columnName.get(state)  + "  step for more debugging informatin " + mrValue);
				
				this.isOozieJobCompleted = true;
				break;
			} else if (jobStatus.equals("SUCCEEDED")) {
				TestSession.logger.info("oozie for " + jobId  + " is SUCCEDED");
				this.searchDataAvailablity.setCurrentWorkingState("OOZIE:JOB_SUCCEDED");
				String state = this.searchDataAvailablity.getState();
				this.dbOperations.updateRecord(this.con ,  "status" , "SUCCEEDED" , this.columnName.get(state) , "SUCCEEDED" , "result" , "PASS"  , this.currentFeedName);
				this.isOozieJobCompleted = true;
				oozieJobresult = "SUCCEEDED";
				break;
			} else if (jobStatus.equals("SUSPENDED")) {
				TestSession.logger.info("oozie for " + jobId  + " is SUSPENDED");
				this.searchDataAvailablity.setCurrentWorkingState("OOZIE:JOB_SUSPENDED");
				String state = this.searchDataAvailablity.getState();
				String mrValue = this.executeOozieCurlCommand(jobId);
				this.dbOperations.updateRecord(this.con ,  "status" , "SUSPENDED" , this.columnName.get(state) , "SUSPENDED" + "~" + mrValue , "result" , "FAIL",  this.currentFeedName);
				this.isOozieJobCompleted = true;
				
				// mark the testcase as failed
				fail( this.currentFeedName + " suspended in " + this.columnName.get(state)  + "  step for more debugging informatin " + mrValue);
				
				oozieJobresult = "SUSPENDED";
				this.executeOozieCurlCommand(jobId);
				break;
			} else if (jobStatus.equals("RUNNING")) {
				TestSession.logger.info("oozie for " + jobId  + " is RUNNING");
				this.searchDataAvailablity.setCurrentWorkingState("OOZIE:JOB_RUNNING");
				String state = this.searchDataAvailablity.getState();
				this.dbOperations.updateRecord(this.con ,  "status" , "RUNNING" , this.columnName.get(state) , "RUNNING" , "result" , "UNKNOWN", this.currentFeedName);
				this.isOozieJobCompleted = false;
				oozieJobresult = "RUNNING";
			}
			
			String currentOozieStep = this.getOoozieJobDetails(jobId);
			Thread.sleep(1000);
			d = new Date();
			perPollStartTime = Long.parseLong(perPollSDF.format(d));
			d = null;
			TestSession.logger.info("please wait for next polling of oozie job");
		}
		return oozieJobresult;
	}
	
	/**
	 * Execute oozie curl command to get the consoleURL ( MP job url ) to know the reason of failure.
	 * @param jobId
	 * @return
	 */
	public String  executeOozieCurlCommand(String jobId) {
		String jobIdValue =  Arrays.asList(jobId.split(" ")).get(1).trim();
		String mrJobValue = null;
		JSONObject finalFailedJSONObject = null;
		boolean flag = false;
		String oozieCommand = "ssh " + this.oozieHostName + "   \" " + this.kINIT_COMMAND + ";"  +  "curl  -s --negotiate -u :  -H \\\"Content-Type: application/xml;charset=UTF-8\\\"  " 
				+ "http://" + this.oozieHostName + ":4080/oozie/v2/job/" + jobIdValue   + "?timezone=GMT" + "\"";
		TestSession.logger.info("command - " + oozieCommand);
		String oozieResult = this.executeCommand(oozieCommand);
		TestSession.logger.info("oozieResult = " + oozieResult); 
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(oozieResult.toString());
		TestSession.logger.info("\n \n \n JSONObject jsonResponse   =  "  + obj.toString() );
		
		JSONArray jsonArray = obj.getJSONArray("actions");
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String status = jsonObject.getString("status");
				if (status.equals("ERROR")) {
					String stepName = jsonObject.getString("name");
					String externalStatus = jsonObject.getString("externalStatus");
					String consoleUrl = jsonObject.getString("consoleUrl");
					mrJobValue = consoleUrl;
					TestSession.logger.info("stepName = " + stepName  + " externalStatus = " + externalStatus   + " consoleUrl =  " + consoleUrl);
					break;
				}
			}
		}
		return mrJobValue;
	}

	public void addNewStatusToDB(String currentStep , String currentStatus) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.dbOperations.getConnection();
		String oldSteps = this.dbOperations.getRecord(con , "steps" , this.currentFeedName);
		con.close();
		int indexOf = oldSteps.indexOf(currentStep);
		if (indexOf == -1) {
			StringBuffer newSteps = new StringBuffer(oldSteps);
			this.dbOperations.updateRecord(this.con , "steps" , newSteps.append(":").append(currentStep).toString() , "currentStep", currentStep, "result" , currentStatus , this.currentFeedName);
		} else {
			System.out.println("Step already exists old step = " + oldSteps.toString()  + "  new step - " + currentStep);
		}
	}
	
	/**
	 * Print complete oozie job info, this will be useful to debug in case failure to know which workflow caused the failure.
	 * @param jobId
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	public String getOoozieJobDetails(String jobId) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		String jobIdValue =  Arrays.asList(jobId.split(" ")).get(1).trim();
		String command = "ssh " + this.oozieHostName +  " \"" + this.OOZIE_COMMAND + "  job -oozie  " +  "http://" + this.oozieHostName + ":4080/oozie -auth kerberos -info  " + jobIdValue  + "\"";
		
		TestSession.logger.info("command - " + command);
		String oozieResult = this.executeCommand(command);
		
		boolean jobDetailStarted = false;
		String currentOozieStep = null;
		List<String> oozieJobDetails = Arrays.asList(oozieResult.split("\n"));
		for ( String jobDetails : oozieJobDetails) {
			if (jobDetails.startsWith("ID")) {
				jobDetailStarted = true;
			}
			if (jobDetailStarted == true) {
				if ( jobDetails.startsWith("00")) {
					List<String> tempList = Arrays.asList(jobDetails.split(" "));

					String temp = tempList.toString();
					Iterable iterable = Splitter.on(',').omitEmptyStrings().split(temp);
					List<String> sss =  com.google.common.collect.Lists.newArrayList(iterable);
					List<String> values = new ArrayList<String>();
					for ( String s : sss) {
						String x = s.trim();
						if (x.length() > 0) {
							int indexOf = x.indexOf("@");
							String workFlowName = x.substring(indexOf + 1, x.length()).trim();
							System.out.println("workFlowName - " + workFlowName);
							values.add(workFlowName.trim());
						}
					}
					System.out.println("---------------------");
					
					System.out.println(values);
					String currentExecutionStep = values.get(0);
					String status = values.get(1);
					System.out.println("currentExecutionStep = " + currentExecutionStep  + "     status " + status   );
					if (currentExecutionStep.equals("cleanup_output")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con , "currentStep", currentExecutionStep , "status" , status , "cleanUpOutput" , status , this.currentFeedName);
					} else if (currentExecutionStep.equals("check_input")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con , "currentStep", currentExecutionStep , "status" , status , "checkInput" , status , this.currentFeedName);	
					} else if (currentExecutionStep.equals("pig_raw_processor")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con ,"currentStep", currentExecutionStep , "status" , status , "pigRawProcessor" , status , this.currentFeedName);	
					} else if (currentExecutionStep.equals("hive_storage")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con ,"currentStep", currentExecutionStep , "status" , status , "hiveStorage" , status , this.currentFeedName);	
					} else if (currentExecutionStep.equals("hive_verify")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con ,"currentStep", currentExecutionStep , "status" , status , "hiveVerify" , status , this.currentFeedName);	
					}  else if (currentExecutionStep.equals("end")) {
						this.searchDataAvailablity.setState(currentExecutionStep);
						this.dbOperations.updateRecord(this.con , "currentStep", currentExecutionStep , "status" , status , "oozieJobCompleted" , status ,  "result" , "PASS" , this.currentFeedName);
					}
					
					System.out.println("---------------------");
				}
			}
		}
		return currentOozieStep;
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

	/**
	 * Copy hive-site.xml file from hcat server to the local host where HTF is running
	 * @return true if hive-site.xml file is copied
	 */
	public boolean copyHiveSiteXML( ) {
		System.out.println("************************************************************************************");

		boolean flag = false;
		String hiveSiteXMLFileLocation = "/tmp/" + this.getCurrentFrequencyValue(); 
		File hiveSiteFile = new File(hiveSiteXMLFileLocation);
		if (!hiveSiteFile.exists() ) {
			if (hiveSiteFile.mkdirs()) {
				TestSession.logger.info(hiveSiteXMLFileLocation + "  created successfully");
				String command = "scp  " + this.oozieHostName + ":" + this.HIVE_SITE_FILE_LOCATION + "   "  + hiveSiteXMLFileLocation ;
				this.executeCommand(command);

				String hiveFilePath = hiveSiteXMLFileLocation + "/hive-site.xml";
				File hiveFile = new File(hiveFilePath);
				if (hiveFile.exists()) {
					TestSession.logger.info(hiveFilePath  + "  is copied successfully.");
					flag = true;
				} else {
					TestSession.logger.info("Failed to  copy " + hiveFilePath);
				}
			}
		} else {
			TestSession.logger.info(hiveSiteXMLFileLocation + " already exists ");
			String command = "scp  " + this.oozieHostName + ":" + this.HIVE_SITE_FILE_LOCATION + "   "  + hiveSiteXMLFileLocation   ;
			this.executeCommand(command);

			String hiveFilePath = hiveSiteXMLFileLocation + "/hive-site.xml";
			File hiveFile = new File(hiveFilePath);
			if (hiveFile.exists()) {
				TestSession.logger.info(hiveFilePath  + "  is copied successfully.");
				flag = true;
			} else {
				TestSession.logger.info("Failed to  copy " + hiveFilePath);
			}
		}
		return flag;
	}	
}
