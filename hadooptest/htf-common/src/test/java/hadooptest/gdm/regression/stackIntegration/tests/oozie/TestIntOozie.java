package hadooptest.gdm.regression.stackIntegration.tests.oozie;

import static com.jayway.restassured.RestAssured.given;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.json.config.JsonPathConfig;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestIntOozie implements java.util.concurrent.Callable<String>{

	private String hostName;
	private String nameNodeName;
	private StackComponent stackComponent;
	private String oozieJobID;
	org.apache.hadoop.conf.Configuration configuration;
	private CommonFunctions commonFunctions;
	private final static String HADOOPQA_KINIT_COMMAND = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";
	private final static String DFSLOAD_KINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String OOZIE_COMMAND = "/home/y/var/yoozieclient/bin/oozie";

	public TestIntOozie(StackComponent stackComponent , String hostName , org.apache.hadoop.conf.Configuration configuration) {
		this.setHostName(hostName);
		this.setStackComponent(stackComponent);
		this.setConfiguration(configuration);
		this.commonFunctions = new CommonFunctions();
	}
	
	public String getOozieJobID() {
		return oozieJobID;
	}

	public void setOozieJobID(String oozieJobID) {
		this.oozieJobID = oozieJobID;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public org.apache.hadoop.conf.Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(org.apache.hadoop.conf.Configuration configuration) {
		this.configuration = configuration;
	}

	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public StackComponent getStackComponent() {
		return stackComponent;
	}

	public void setStackComponent(StackComponent stackComponent) {
		this.stackComponent = stackComponent;
	}

	@Override
	public String call() throws Exception {
		String sourePath = new File("").getAbsolutePath() + "/resources/stack_integration/oozie";
		String destPath = "/tmp/integration-testing/oozie/" +  this.getCurrentHr() ;
		createWorkFlowFolder(destPath + "/outputDir/");
		copySupportingFilesToScratch(destPath);
		String result = execute();
		return "oozie-" + result;
	}
	
	public String getCurrentHr() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String currentHR = simpleDateFormat.format(calendar.getTime());
		return currentHR;
	}

	public String  execute() {
		TestSession.logger.info(" ---------------------------------------------------------------  TestIntOozie  start ------------------------------------------------------------------------");

		String currentJobName = this.commonFunctions.getDataSetName();
		this.commonFunctions.updateDB(currentJobName, "oozieCurrentState", "RUNNING");

		
		String currentHR = getCurrentHr();
		boolean oozieResult = false;

		String oozieCommand = "ssh " + this.getHostName() + "   \" " + this.DFSLOAD_KINIT_COMMAND + ";"  +   OOZIE_COMMAND + " job -run -config " +  "/tmp/integration-testing/oozie/" + currentHR + "/job.properties" + " -oozie " + "http://" + this.getHostName() + ":4080/oozie -auth kerberos"   + " \"";
		String tempOozieJobID = this.commonFunctions.executeCommand(oozieCommand);
		TestSession.logger.info("-- tempOozieJobID = " + tempOozieJobID );
		int indexOfJobIdOutput = tempOozieJobID.indexOf("job:");
		TestSession.logger.info("indexOfJobIdOutput = " + indexOfJobIdOutput);
		String jobID  = tempOozieJobID.substring(tempOozieJobID.indexOf(":") + 1 , tempOozieJobID.length());
		this.setOozieJobID(jobID);
		String result = getResult();
		if (result.indexOf("KILLED") > -1) {
			this.commonFunctions.updateDB(currentJobName, "oozieResult", "FAIL");
			oozieResult = false;
		} else if ( result.indexOf("SUCCEEDED") > -1 ) {
			this.commonFunctions.updateDB(currentJobName, "oozieResult", "PASS");
			oozieResult = true;
		}
		this.commonFunctions.updateDB(currentJobName, "oozieCurrentState", "COMPLETED");
		TestSession.logger.info(" ---------------------------------------------------------------  TestIntOozie  end ------------------------------------------------------------------------");
		return "oozie"  + "-" + oozieResult;
	}
	
	public String getResult() {
		String status =  null;
		String query = "http://" + this.getHostName() + ":4080/oozie/v1/job/" + this.getOozieJobID();
		com.jayway.restassured.response.Response response = given().contentType(ContentType.JSON).cookie(this.commonFunctions.getCookie()).get(query);
		TestSession.logger.info("response.getStatusCode() = " + response.getStatusCode());
		if (response != null) {
			JsonPath jsonPath = response.jsonPath().using(new JsonPathConfig("UTF-8"));
			status = jsonPath.getString("status");
			while ( status.indexOf("RUNNING") > -1) {
				jsonPath = pollOozieJobResult();
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				status = jsonPath.getString("status");
				if (! (status.indexOf("RUNNING") > -1)   ) {
					break;
				}
			}
		}
		return status;
	}
	
	private JsonPath pollOozieJobResult() {
		String query = "http://" + this.getHostName() + ":4080/oozie/v1/job/" + this.getOozieJobID();
		com.jayway.restassured.response.Response response = given().contentType(ContentType.JSON).cookie(this.commonFunctions.getCookie()).get(query);
		TestSession.logger.info("response.getStatusCode() = " + response.getStatusCode());
		JsonPath jsonPath = response.jsonPath().using(new JsonPathConfig("UTF-8"));
		return jsonPath;
	}
	
	/**
	 * Copies the pig script and other supporting files required to start the oozie job from specified source to destination file.
	 * @param src
	 * @param des
	 * @return
	 * @throws IOException
	 */
	public boolean copySupportingFilesToScratch(String des) throws IOException {
		boolean flag = false;
		FileSystem remoteFS = FileSystem.get(this.configuration);
		String absolutePath = new File("").getAbsolutePath();
		System.out.println("Absolute Path =  " + absolutePath);
		File integrationFilesPath = new File(absolutePath + "/resources/stack_integration/oozie/");
		if (integrationFilesPath.exists()) {
			Path destPath = new Path(des);
			File fileList[] = integrationFilesPath.listFiles();
			for ( File f : fileList) {
				if (f.isFile()) {
					Path scrFilePath = new Path(f.toString());
					remoteFS.copyFromLocalFile(false , true, scrFilePath , destPath);
					System.out.println( scrFilePath + "  files copied sucessfully to " + des);
					flag = true;
				}
			}
		} else {
			System.out.println(integrationFilesPath.toString() + " does not exists...");
		}		
		return flag;
	}
	
	/**
	 * Create a folder on HDFS.
	 * @param path
	 * @throws IOException
	 */
	private void createFolders(String path) throws IOException {
		FileSystem remoteFS = FileSystem.get(this.configuration);
		Path pipeLinePath = new Path(path);
		FsPermission fsPermission = new FsPermission(FsAction.ALL , FsAction.ALL , FsAction.ALL);
		boolean isPipeLineCreated = remoteFS.mkdirs(pipeLinePath,  fsPermission);
		if (isPipeLineCreated) {
			TestSession.logger.info(pipeLinePath.toString() + " is created successfully");
		} else {
			TestSession.logger.info("Failed to create " + pipeLinePath.toString() );
		}
	}

	/**
	 * Method that actually invoke createFolder method to create the folders on HDFS
	 * @param path
	 * @throws IOException
	 */
	public void createWorkFlowFolder(String path) throws IOException {
		FileSystem remoteFS = FileSystem.get(this.configuration);
		Path pipeLinePath = new Path(path);
		if (! remoteFS.exists(pipeLinePath)) {
			createFolders(path);
		} else {
			TestSession.logger.info(pipeLinePath.toString()  + " already exists, deleting and create a new one!");
			navigate(path);
			createFolders(path);
		}
	}

	/**
	 * Navigate the specified path and delete folders and files
	 * @param path
	 * @throws IOException
	 */
	public void navigate(String path) throws IOException {
		File f = new File(path);
		if (f.exists() == true) {
			FileSystem remoteFS = FileSystem.get(this.configuration);
			Path scrPath = new Path(path);
			FileStatus [] fileStatus = remoteFS.listStatus(scrPath);
			if (fileStatus != null) {
				for ( FileStatus file : fileStatus) {
					if (file.isDirectory()) {
						String fileName = file.getPath().toString();
						System.out.println("fileName - " + fileName);
						navigate(fileName);
					} else if (file.isFile()) {
						TestSession.logger.info("deleting " + file.getPath().toString() );
						remoteFS.delete(file.getPath() , true);
					}
				}				
			}
		}
	}
}
