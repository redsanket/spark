package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static com.jayway.restassured.RestAssured.given;
import static java.lang.System.out;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
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
import org.junit.Test;

import com.google.common.base.Splitter;
import hadooptest.cluster.gdm.ConsoleHandle;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.integration.metrics.NameNodeDFSMemoryDemon;
import hadooptest.gdm.regression.integration.metrics.NameNodeTheadDemon;
import hadooptest.gdm.regression.integration.metrics.NameNodeThreadInfo;

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
	 
	private String currentHadoopVersion;
	private String currentPigVersion;
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
	private final static int DAY=0,HOUR=1,MIN=2,SEC=3,MILL_SEC=4;
	
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
		this.createDB();
		this.searchDataAvailablity = new SearchDataAvailablity(this.clusterName , this.directoryPath , this.filePattern , this.operationType);
	}
	
	public void createDB() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.dbOperations = new DataBaseOperations();
		this.dbOperations.createDB();
		this.dbOperations.createIntegrationResultTable();
		this.dbOperations.createNameNodeThreadInfoTable();
		this.dbOperations.createNameNodeMemoryInfoTable();
		this.dbOperations.createHealthCheckupTable();
	}

	public void dataPoller() throws InterruptedException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		columnName.put("start", "start");
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
			
			 	this.createDB();
				
				// insert record into the health check up table only once per day 
				int healthRowCount = this.dbOperations.isHealthCheckupRecordExits();
				//if (healthRowCount == 0) {
					TestSession.logger.info("******************************  inserting record into health table *************************************************");
					NameNodeThreadInfo nameNodeThreadInfo = new NameNodeThreadInfo();
					ConsoleHandle consoleHandle = new ConsoleHandle();
					String clusterNameNode = consoleHandle.getClusterNameNodeName(clusterName);
					out.println(clusterName  + " 's name node = " + clusterNameNode);
					nameNodeThreadInfo.setNameNodeName(clusterNameNode);
					nameNodeThreadInfo.getNameNodeThreadInfo();
					String nameNodeCurrentState = nameNodeThreadInfo.getNameNodeCurrentState();
					String currentHadoopVersion = Arrays.asList(nameNodeThreadInfo.getHadoopVersion().split(",")).get(0).trim();
					Connection con1  = this.dbOperations.getConnection();
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
				    Date date = new Date();
				    String dt = dateFormat.format(date);
				    DateFormat dateFormat1 = new SimpleDateFormat("yyyy/MM/dd");
				    String updateTime = dateFormat1.format(date);
				    String hbaseMasterResult = this.getHBaseHealthCheck().trim();
				    int regionalServerStatus = this.getHBaseRegionalServerHealthCheckup();
		    		TestSession.logger.info("hbaseResult = " + hbaseMasterResult);
		    		// if either hbase master or hbase regional server are down, mark hbase as down
		    		// TODO : Need to find a way to say that which service is down on the front end 
				    if (hbaseMasterResult.equals("down") || regionalServerStatus == 0) {
				    	String result = hbaseMasterResult + "~0.0";
				    	this.dbOperations.insertHealthCheckInfoRecord(con1 , dt , nameNodeCurrentState + "~" + currentHadoopVersion , result);
				    } else {
				    	this.dbOperations.insertHealthCheckInfoRecord(con1 , dt , nameNodeCurrentState + "~" + currentHadoopVersion , hbaseMasterResult);
				    }
					con1.close();
					TestSession.logger.info("******************************  inserting record into health table *************************************************");
				//}
				
				// TODO check health of all the stack components
				
				// get installed stack components versions
				String hadoopVersion = this.getHadoopVersion();
				String pigVersion = this.getPigVersion();
				String oozieVersion = this.getOozieVersion();
				
				// job started.
				this.searchDataAvailablity.setState(IntegrationJobSteps.JOB_STARTED);
				
				// add start state to the user stating that job has started for the current frequency.
				this.dbOperations.insertRecord(this.currentFeedName, "hourly", JobState.STARTED,  String.valueOf(initTime), this.searchDataAvailablity.getState().toUpperCase().trim() , hadoopVersion , pigVersion , oozieVersion);

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
						TestSession.logger.info("*************************************************************************************************************************************************");
						this.oozieJobResult = this.pollOozieJob(this.oozieJobID , oozieWorkFlowName);
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
	 * Check whethe HBase Master is up or down. If the hbase master is up it returns the value as "active~" + hbase version.
	 * @return
	 */
	private String getHBaseHealthCheck() {
		String hbaseVersion = null;
		boolean flag= false;
		try {
			String hbaseMasterHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
			String hbaseMasterPortNo = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterPort").trim();
			ConsoleHandle consoleHandle = new ConsoleHandle();
			String cookie  = consoleHandle.httpHandle.getBouncerCookie();
			String hbaseJmxUrl = "http://" + hbaseMasterHostName + ":" + hbaseMasterPortNo + "/jmx?qry=java.lang:type=Runtime"; 
			com.jayway.restassured.response.Response response = given().cookie(cookie).get(hbaseJmxUrl);
			String reponseString = response.getBody().asString();
			
			JSONObject obj =  (JSONObject) JSONSerializer.toJSON(reponseString.toString());
			JSONArray beanJsonArray = obj.getJSONArray("beans");
			String str = beanJsonArray.getString(0);
			JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(str.toString());
			TestSession.logger.info("name  = " +obj1.getString("name") );
			JSONArray SystemPropertiesJsonArray = obj1.getJSONArray("SystemProperties");
			if ( SystemPropertiesJsonArray.size() > 0 ) {
				Iterator iterator = SystemPropertiesJsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String key = jsonObject.getString("key");
					if (key.equals("java.class.path")) {
						List<String> paths = Arrays.asList(jsonObject.getString("value").split(":"));
						for ( String value : paths) {
							if (value.startsWith("/home/y/libexec/hbase/bin/../lib/hbase-client-")) {
								
								// mark that hbase is active
								hbaseVersion = "active~" + Arrays.asList(value.split("-")).get(2).replaceAll(".jar", "");
								TestSession.logger.info("hbaseVersion  = " + hbaseVersion);
								flag = true;
								break;
							}
						}
						if (flag == true) break;
					}
					if (flag == true) break;
				}
			}
			TestSession.logger.info("hbaseVersion  = " + hbaseVersion);

		} catch(Exception e) {
			TestSession.logger.info("exception " + e );
			TestSession.logger.info("exception  --------------" + e.getStackTrace());
		} finally {
			if (flag == false) {
				hbaseVersion = "down";	
			}
			TestSession.logger.info("hbaseVersion  = " + hbaseVersion);
			TestSession.logger.info("-----------------");
		}
		return hbaseVersion;
	}
	
	/**
	 * Check whether there is any hbase regional server or up and running.
	 * @return
	 */
	public int getHBaseRegionalServerHealthCheckup() {
		int regionalServersHostSize = 0;
		try{
			List<String> reginalServerHostNameList ;
			String hbaseMasterHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
			String hbaseMasterPortNo = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterPort").trim();
			ConsoleHandle consoleHandle = new ConsoleHandle();
			String cookie  = consoleHandle.httpHandle.getBouncerCookie();
			String hbaseJmxUrl = "http://" + hbaseMasterHostName + ":" + hbaseMasterPortNo + "/jmx?qry=hadoop:service=Group,name=Group"; 
			com.jayway.restassured.response.Response response = given().cookie(cookie).get(hbaseJmxUrl);
			String reponseString = response.getBody().asString();
			TestSession.logger.info("reponseString = " + reponseString);
			
			JSONObject obj =  (JSONObject) JSONSerializer.toJSON(reponseString.toString());
			JSONArray beanJsonArray = obj.getJSONArray("beans");
			String str = beanJsonArray.getString(0);
			JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(str.toString());
			TestSession.logger.info("name  = " +obj1.getString("name") );
			JSONArray SystemPropertiesJsonArray = obj1.getJSONArray("ServersByGroup");
			
			TestSession.logger.info("SystemPropertiesJsonArray = " + SystemPropertiesJsonArray.toString());
			if ( SystemPropertiesJsonArray.size() > 0 ) {
				Iterator iterator = SystemPropertiesJsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String key = jsonObject.getString("key");
					if (key.equals("default")) {
						JSONArray regionalServerHostJsonArray = jsonObject.getJSONArray("value");
						String tempRegionalServersHostNames = regionalServerHostJsonArray.toString();
						if (tempRegionalServersHostNames.length() > 0 && tempRegionalServersHostNames != null) {
							reginalServerHostNameList  = Arrays.asList(tempRegionalServersHostNames.split(","));
							regionalServersHostSize = reginalServerHostNameList.size();
							TestSession.logger.info("regionalServerHostJsonArray   = " + reginalServerHostNameList);
							break;
						} else {
							// if there is no regional server exists or down.
							regionalServersHostSize = 0;
						}
					}
				}
			}
		}catch(Exception e) {
			regionalServersHostSize = 0;
			TestSession.logger.info("** HBase Regional servers are down..! **** " + e);
		}
		return  regionalServersHostSize;
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
		String oozieJobresult = "FAIL";
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
			this.executeOozieCurlCommand(jobId);
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
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public String  executeOozieCurlCommand(String jobId) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
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
		
		String jobStartedTime = obj.getString("startTime");
		String jobEndedTime = obj.getString("endTime");
		
		JSONArray jsonArray = obj.getJSONArray("actions");
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String status = jsonObject.getString("status");
				String stepName = jsonObject.getString("name");
				String startTime = jsonObject.getString("startTime");
				String endTime = jsonObject.getString("endTime");
				String consoleUrl = jsonObject.getString("consoleUrl");
				TestSession.logger.info("stepName = " + stepName + "    startTime = " + startTime  + "   endTime  = " + endTime);
				long total=0;
				if ( (startTime != null) && (endTime != null) ) {
					total = this.getTotalExecutionDuration(startTime, endTime);

					if (stepName.equals("cleanup_output")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con , "currentStep", stepName , "status" , status , "cleanUpOutput" , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , this.currentFeedName);
					} else if (stepName.equals("check_input")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con , "currentStep", stepName , "status" , status , "checkInput" , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , this.currentFeedName);	
					} else if (stepName.equals("pig_raw_processor")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con ,"currentStep", stepName , "status" , status , "pigRawProcessor" , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , this.currentFeedName);	
					} else if (stepName.equals("hive_storage")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con ,"currentStep", stepName , "status" , status , "hiveStorage" , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , this.currentFeedName);	
					} else if (stepName.equals("hive_verify")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con ,"currentStep", stepName , "status" , status , "hiveVerify" , status  + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , this.currentFeedName);	
					}  else if (stepName.equals("end")) {
						this.searchDataAvailablity.setState(stepName);
						this.dbOperations.updateRecord(this.con , "currentStep", stepName , "status" , status , "oozieJobCompleted" , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total ,  "result" , "PASS" , this.currentFeedName);
					}
					
					if (status.equals("ERROR")) {
						String externalStatus = jsonObject.getString("externalStatus");
						// commenting the following statement since i am reading the consoleUrl value above
						//String consoleUrl = jsonObject.getString("consoleUrl");
						mrJobValue = consoleUrl;
						TestSession.logger.info("****************  stepName = " + stepName  + " externalStatus = " + externalStatus   + " consoleUrl =  " + consoleUrl  + "   startTime = " + jobStartedTime   + "   endTime = " + jobEndedTime  +  "  *****************");
						long totalExecution = this.getTotalExecutionDuration(jobStartedTime, jobEndedTime);
						this.dbOperations.updateRecord(this.con, this.columnName.get(stepName) , status + "~" + consoleUrl + "~" + startTime + "~" +  endTime + "~" + "" + total , "result" , status , "startTime" , jobStartedTime , "endTime" ,  jobEndedTime , "totalExecutionTime" ,  "" + totalExecution, this.currentFeedName );
						fail("failed in " + stepName + "  reason  " +  status  + "  additional information " + consoleUrl);
						break;
					}
				}
			}
		}
		return mrJobValue;
	}

	
	private long getTotalExecutionDuration(final String startTimeStr  , final String endTimeStr) {
		
		TestSession.logger.info("startTimeStr  = " + startTimeStr   + "  endTimeStr   " + endTimeStr);
		
		long[] durations = null;
		long total = 0L;
		try {
			durations = this.getTimeDifference(startTimeStr , endTimeStr);
			
			if (durations[MIN] > 0) {
				total = durations[MIN] * 60; 
			}
			if (durations[SEC] > 0) {
				total = total + durations[SEC];
			}
			for ( long d : durations) {
				TestSession.logger.info(d);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("StartTime = " + startTimeStr + "    jobEndTime = " + startTimeStr  + " Total duration - " + total);
		return total;
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
			String command = "scp  " + this.oozieHostName + ":" + this.HIVE_SITE_FILE_LOCATION + "   "  + hiveSiteXMLFileLocation;
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
	
	
	/**
	 * Get the hadoop version by running "hadoop version" command on name node
	 * @return
	 */
	public String getHadoopVersion() {
		String hadoopVersion = "Failed to get hadoop version";
		boolean flag = false;
		String integrationNameNodeHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.nameNodeHostName");
		String getHadoopVersionCommand = "ssh " + integrationNameNodeHostName  + " \"" + kINIT_COMMAND + ";" + "hadoop version\"";
		String outputResult = this.executeCommand(getHadoopVersionCommand);
		TestSession.logger.info("outputResult = " + outputResult);
		java.util.List<String>outputList = Arrays.asList(outputResult.split("\n"));
		for ( String str : outputList) {
			TestSession.logger.info(str);
			
			if ( str.startsWith("Hadoop") == true ) {
				hadoopVersion = Arrays.asList(str.split(" ")).get(1);
				flag = true;
				break;
			}
		}
		TestSession.logger.info("Hadoop Version - " + hadoopVersion);
		return hadoopVersion;
	}
	
	/**
	 * Get the pig version
	 * @return
	 */
	public String getPigVersion() {
		String pigVersion = "Failed to get pig version";
		boolean flag = false;
		String integrationPigHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pigHostName");
		String getPigVersionCommand = "ssh " + integrationPigHostName  + " \"" + kINIT_COMMAND + ";" + "export PIG_HOME=/home/y/share/pig;pig -version\"";
		String outputResult = this.executeCommand(getPigVersionCommand);
		TestSession.logger.info("outputResult = " + outputResult);
		java.util.List<String>outputList = Arrays.asList(outputResult.split("\n"));
		for ( String str : outputList) {
			TestSession.logger.info(str);
			int index = str.indexOf("Pig");
			if ( index > 0) {
				String tempStr = str.substring(index, str.indexOf("(rexported)"));
				List<String> tempList = Arrays.asList(tempStr.split(" "));
				pigVersion = tempList.get(tempList.size() - 1);
				flag = true;
				break;
			}
		}
		TestSession.logger.info("Pig Version - " + pigVersion);
		return pigVersion;
	}
	
	public String getOozieVersion() { 
		String integrationOozieHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.oozieHostName");
		String getOozieVersionCommand = "ssh " + integrationOozieHostName  + " \"" + kINIT_COMMAND + ";" + "/home/y/var/yoozieclient/bin/oozie version\"";
		String outputResult = this.executeCommand(getOozieVersionCommand);
		TestSession.logger.info("outputResult = " + outputResult);
		java.util.List<String>outputList = Arrays.asList(outputResult.split(":"));
		for ( String str : outputList) {
			TestSession.logger.info(str);
		}
		List<String> tempList = Arrays.asList(outputList.get(1).trim());
		String oozieVersion = tempList.get(tempList.size() - 1);
		TestSession.logger.info("Oozie Version - " + oozieVersion);
		return oozieVersion;
	}
	
	private long[] getTimeDifference(final String startTimeStr  , final String endTimeStr) throws ParseException {
		DateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
		Date d1 = null , d2 = null;
		d1 = format.parse(startTimeStr);
		d2 = format.parse(endTimeStr);
		long[] result = new long[5];
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone("GMT"));
		c.setTime(d1);
		
		long  t1 = c.getTimeInMillis();
		c.setTime(d2);
		long diff = Math.abs(c.getTimeInMillis() - t1);
		final int ONE_DAY = 1000 * 60 * 60 * 24;
		final int ONE_HOUR = ONE_DAY / 24;
		final int ONE_MINUTE = ONE_HOUR / 60;
		final int ONE_SECOND = ONE_MINUTE / 60;

		long d = diff / ONE_DAY;
		diff %= ONE_DAY;

		long h = diff / ONE_HOUR;
		diff %= ONE_HOUR;

		long m = diff / ONE_MINUTE;
		diff %= ONE_MINUTE;

		long s = diff / ONE_SECOND;
		long ms = diff % ONE_SECOND;
		result[0] = d;
		result[1] = h;
		result[2] = m;
		result[3] = s;
		result[4] = ms;
		
		for ( long r : result ) {
			TestSession.logger.info("!!!!!!!!!!!!!!!!!!!!!!v getTimeDifference = " + r);
		}

		return result;
	}
}
