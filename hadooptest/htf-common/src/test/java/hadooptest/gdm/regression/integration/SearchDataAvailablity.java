// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.integration;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *  Class that search for data availablity on the grid, creates folder on HDFS 
 */
public class SearchDataAvailablity implements PrivilegedExceptionAction<String> {

    private String clusterName;
    private String basePath;
    private String dataPath;
    private String dataPattern;
    private String fullPath;
    private String nameNodeName;
    private String result;
    private String feedResultPath;
    private String scratchPath;
    private String oozieWFPath;
    private String currentStatusFolderName;
    private String currentStatusFileName;
    private boolean scratchPathCreated;
    private boolean pipeLineInstanceCreated;
    private boolean oozieWFPathCopied;
    private String pipeLineInstance;
    private String currentFrequencyValue;
    private String currentFeedName;
    private String crcValue;
    private DataBaseOperations dbOperations; 
    private String state;
    private Connection con;
    private StringBuffer currentWorkingState;
    private ConsoleHandle consoleHandle;
    private Configuration configuration;
    private List<String> instanceList;
    private static final String schema = HadooptestConstants.Schema.HDFS;
    private static final String PROCOTOL = "hdfs://";
    private static final String KEYTAB_DIR = "keytabDir";
    private static final String KEYTAB_USER = "keytabUser";
    private static final String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
    private static final String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
    private static final String BASE_STATUS_FOLDER = "/data/integration_status/";
    private static final int NO_OF_INSTANCE = 5;
    private static final int DAYS = 20;
    private static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
    
    public SearchDataAvailablity(String clusterName , String basePath , String dataPattern , String state) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

        this.clusterName = clusterName;
        this.basePath = basePath;
        this.dataPath = dataPath;
        this.dataPattern = dataPattern;
        this.state = state;
        
        this.dbOperations = new DataBaseOperations();
        this.con  = this.dbOperations.getConnection();

        // Populate the details for DFSLOAD
        HashMap<String, String> fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails.put(KEYTAB_DIR, HadooptestConstants.Location.Keytab.DFSLOAD);
        fileOwnerUserDetails.put(KEYTAB_USER, HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM");
        fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"+ HadooptestConstants.UserNames.DFSLOAD + "Dir/" + HadooptestConstants.UserNames.DFSLOAD + "File");
        fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS, HadooptestConstants.UserNames.HADOOPQA);

        this.supportingData.put(HadooptestConstants.UserNames.DFSLOAD,fileOwnerUserDetails);
        TestSession.logger.info("CHECK:" + this.supportingData);
        this.consoleHandle = new ConsoleHandle();
        this.nameNodeName = this.consoleHandle.getClusterNameNodeName(this.clusterName);
        org.apache.commons.configuration.Configuration configuration = this.consoleHandle.getConf();
        this.crcValue = configuration.getString("hostconfig.console.crcValue").trim();
        
        this.instanceList = new ArrayList<String>();

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        TestSession.logger.info(sdf.format(cal.getTime()));

        cal.add(Calendar.DAY_OF_MONTH, -DAYS);
        String instance = sdf.format(cal.getTime());
        this.instanceList.add(instance);

        for (int i = 1 ; i < NO_OF_INSTANCE  ; i++) {
            cal.add(Calendar.DAY_OF_MONTH, i);
            instance = sdf.format(cal.getTime());
            this.instanceList.add(instance);
        }
        
        this.currentWorkingState = new StringBuffer();
    }
    
    public void setCurrentWorkingState(String currentWorkingState) throws IOException {
        this.currentWorkingState.append(currentWorkingState + "-");
        TestSession.logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  currentWorkingState  = " +  currentWorkingState);
        writeJobStatus();
        
    }
    
    public StringBuffer getCurrentWorkingState() {
        return this.currentWorkingState;
    }
    
    public void setCurrentFeedName(String feedName) {
        this.currentFeedName = feedName; 
        TestSession.logger.info("Current feed Name - " + this.currentFeedName);
    }
    
    public String getCurrentFeedName() {
        return this.currentFeedName;
    }
    
    public boolean isScratchPathCreated() {
        return this.scratchPathCreated;
    }

    public boolean isPipeLineInstanceCreated() {
        return this.pipeLineInstanceCreated;
    }

    public String getClusterNameNode() {
        return this.nameNodeName;
    }

    public void setState(String operationType) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
        this.state = operationType;
        TestSession.logger.info("setOperationType   - " + this.state);
        writeJobStatus();
    }

    public String  getState() {
        return this.state;
    }

    public boolean isDataAvailableOnGrid() {
        if(this.result == "AVAILABLE") {
            return true;
        } else {
            return false;
        }
    }

    public void setCurrentFrequencyValue(String currentFrequencyValue) {
        this.currentFrequencyValue = currentFrequencyValue;
    }

    public String getCurrentFrequencyValue() {
        return this.currentFrequencyValue;
    }

    public void setScratchPath(String scratchPath) {
        this.scratchPath =  scratchPath;
    }

    public void setPipeLineInstance(String pipeLineInstance) {
        this.pipeLineInstance = pipeLineInstance;
    }

    public void setOozieWorkFlowPath(String oozieWFPath) {
        this.oozieWFPath = oozieWFPath;
    }

    /*
     * Invoke run method and create the instance files on the specified cluster.
     */
    public void execute() throws IOException, InterruptedException {
        for (String aUser : this.supportingData.keySet()) {
            TestSession.logger.info("aUser = " + aUser);
            this.configuration = getConfForRemoteFS();
            UserGroupInformation ugi = getUgiForUser(aUser);
            Calendar dataSetCal = Calendar.getInstance();
            SimpleDateFormat feed_sdf = new SimpleDateFormat(this.dataPattern);
            long dataSetHourlyTimeStamp =  Long.parseLong(feed_sdf.format(dataSetCal.getTime()));
            this.fullPath = this.basePath + "/" + "Integration_Testing_DS_" + dataSetHourlyTimeStamp + "00" + "/20130309/_SUCCESS";
            TestSession.logger.info("---- pathPattern  = " + this.fullPath);
            this.result = ugi.doAs(this);
            TestSession.logger.info("Result = " + result);
        }
    }

    /**
     * Returns the remote cluster configuration object.
     * @param aUser  - user
     * @param nameNode - name of the cluster namenode. 
     * @return
     */
    public Configuration getConfForRemoteFS() {
        Configuration conf = new Configuration(true);
        String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.nameNodeName + ":" + HadooptestConstants.Ports.HDFS;
        TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
        conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DEV.YGRID.YAHOO.COM");
        conf.set("hadoop.security.authentication", "true");
        if (this.crcValue != null) {
            conf.set("dfs.checksum.type" , this.crcValue);
        } else {
            conf.set("dfs.checksum.type" , "CRC32");
        }
        TestSession.logger.info(conf);
        return conf;
    }

    /**
     * set the hadoop user details , this is a helper method in creating the configuration object.
     */
    public UserGroupInformation getUgiForUser(String aUser) {
        String keytabUser = this.supportingData.get(aUser).get(KEYTAB_USER);
        TestSession.logger.info("Set keytab user=" + keytabUser);
        String keytabDir = this.supportingData.get(aUser).get(KEYTAB_DIR);
        TestSession.logger.info("Set keytab dir=" + keytabDir);
        UserGroupInformation ugi = null;
        try {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabUser, keytabDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ugi;
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
     * Create scratchPath directory on the local file system
     * @param path
     */
    public void createScratchPath(String path) {
        File f = new File(path);
        if (f.exists() == false) {
            f.mkdirs();
        } else {
            TestSession.logger.info(path + " already exists");
        }
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

    public String run() throws Exception {
        String returnValue = "";
        TestSession.logger.info("---------------- OperationType = " + this.getState() );
        if (this.getState().toUpperCase().equals("POLLING") || this.getState().toUpperCase().equals("INCOMPLETE")) {
            this.setCurrentWorkingState("POLLING");
            
            TestSession.logger.info("configuration   =  " + this.configuration.toString());
            FileSystem remoteFS = FileSystem.get(this.configuration);
            Path path = new Path(this.fullPath.trim());

            // check whether path exists on the grid
            boolean basePathExists = remoteFS.exists(path);
            TestSession.logger.info(this.fullPath.trim() + " "  + basePathExists);
            if (basePathExists == true) {
                TestSession.logger.info("Path exists - " + this.fullPath);
                Path doneFilePath = new Path(this.fullPath);
                boolean doneFileExists = remoteFS.exists(doneFilePath);
                if (doneFileExists == true) {
                    this.setState("AVAILABLE");
                    returnValue = "AVAILABLE";
                    this.setCurrentWorkingState("AVAILABLE");
                    this.setState("AVAILABLE");
                    TestSession.logger.info(this.fullPath + " exists, data is transfered.");
                } else if (doneFileExists == false) {
                    TestSession.logger.info("------- "+ path.toString() + " Data is available, but not yet completed. Still loading.");
                    this.setState("INCOMPLETE");
                    returnValue = "INCOMPLETE";
                    this.setCurrentWorkingState("INCOMPLETE");
                    this.setState("INCOMPLETE");
                }
            } else if (basePathExists == false) {
                this.setState("POLLING");
                returnValue = "POLLING";
                this.setCurrentWorkingState("POLLING");
                this.setState("POLLING");
                TestSession.logger.info("Data is still UNAVAILABLE and state is " + returnValue);
            }
        }
        if (this.getState().equals("WORKING_DIR")) {
            this.setCurrentWorkingState("WORKING_DIR:CREATING");
            
            // create status folder and status file
            this.createStatusFolder();
            this.createCurrentStatusFile();
            
            TestSession.logger.info("Pipeline instance path - " + this.pipeLineInstance);
            this.createWorkFlowFolder(this.pipeLineInstance);
            
            // create lib folder
            this.createWorkFlowFolder(this.pipeLineInstance + "/lib");
            this.copyLibFiles("/tmp/integration_test_files/lib/" , this.pipeLineInstance + "/lib/");

            TestSession.logger.info("scratchPad path - " + this.scratchPath);
            this.createScratchPath(this.scratchPath);
            this.pipeLineInstanceCreated = this.copySupportingFilesToScrach("/tmp/integration_test_files" , this.pipeLineInstance);
            this.copyHiveFileToScratch(this.oozieWFPath);
            this.oozieWFPathCopied = this.copySupportingFilesToScrach("/tmp/integration_test_files" , this.oozieWFPath);
            this.copyJobPropertiesFile(this.oozieWFPath);
            
            // create lib folder
            this.createWorkFlowFolder(this.oozieWFPath + "/lib");
            this.copyLibFiles("/tmp/integration_test_files/lib/" , this.oozieWFPath + "/lib/");
        
            this.copySupportJarFilesToScrach("FETLProjector.jar" , this.oozieWFPath + "/lib/" );
            this.copySupportJarFilesToScrach("FETLProjector.jar" , this.pipeLineInstance + "/lib/");

            this.setCurrentWorkingState("WORKING_DIR:CREATED");
            this.setCurrentWorkingState("SETUP:COMPLETED");
            
            // once scratchPath and pipelineInstance folders are created and files are completed operation is changed to startOozieJob state. 
            if (this.scratchPathCreated ==  true && this.pipeLineInstanceCreated == true) {
                this.result = "START_OOZIE_JOB";
                this.setCurrentWorkingState("OOZIE:START_JOB");
            }
        }
        this.result = returnValue;
        return returnValue;
    }
    
    /**
     * Copies supporting files like pig scripts and other files to HDFS so that oozie job can use it.
     * @param src
     * @param des
     * @throws IOException
     */
    private void copySupportingFiles(String src , String des) throws IOException {
        File srcFile = new File(src);
        if (srcFile.exists()) {
            File desFile = new File(des);
            FileUtils.copyDirectoryToDirectory(srcFile , desFile);
            TestSession.logger.info("files and directories copied from " + src  + " to  " + des);
        } else {
            TestSession.logger.info(src + " does not exists.");
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
                        TestSession.logger.info("fileName - " + fileName);
                        navigate(fileName);
                    } else if (file.isFile()) {
                        TestSession.logger.info("deleting " + file.getPath().toString() );
                        remoteFS.delete(file.getPath() , true);
                    }
                }               
            }
        }
    }

    public void setFeedResultPath(String feedPath) {
        this.feedResultPath = feedPath;
    }

    public String createFeedResultFolder(String feedPath) throws IOException {
        String result = "failed";
        TestSession.logger.info("-------    createFeedResultFolder ----------- ");
        FileSystem remoteFS = FileSystem.get(this.configuration);
        Path path = new Path(feedPath.trim());
        boolean feedPathExists = remoteFS.exists(path);
        if (feedPathExists == true) {
            TestSession.logger.info(feedPath  + " already exists.");
            result = "success";
        } else if (feedPathExists ==  false) {
            FsPermission fsPermission = new FsPermission(FsAction.ALL , FsAction.ALL , FsAction.ALL);
            boolean basePathCreated = remoteFS.mkdirs(path,  fsPermission);
            if (basePathCreated == true) {
                TestSession.logger.info(feedPath + " successfully created.");
                result = "success";
            } else {
                TestSession.logger.info("Failed to create " + feedPath + " directories.");
            }
        }
        return result;
    }

    /**
     * Copies the pig script and other supporting files required to start the oozie job from specified source to destination file.
     * @param src
     * @param des
     * @return
     * @throws IOException
     */
    public boolean copySupportingFilesToScrach(String src , String des) throws IOException {
        boolean flag = false;
        FileSystem remoteFS = FileSystem.get(this.configuration);
        String absolutePath = new File("").getAbsolutePath();
        TestSession.logger.info("Absolute Path =  " + absolutePath);
        File integrationFilesPath = new File(absolutePath + "/resources/stack_integration");
        if (integrationFilesPath.exists()) {
            Path destPath = new Path(des);
            File fileList[] = integrationFilesPath.listFiles();
            for ( File f : fileList) {
                if (f.isFile()) {
                    Path scrFilePath = new Path(f.toString());
                    remoteFS.copyFromLocalFile(false , true, scrFilePath , destPath);
                    TestSession.logger.info( scrFilePath + "  files copied sucessfully to " + des);
                    flag = true;
                }
            }
        } else {
            TestSession.logger.info(integrationFilesPath.toString() + " does not exists...");
        }       
        return flag;
    }
    
    // method to copy FETLProjector.jar  & BaseFeed.jar
    public boolean copySupportJarFilesToScrach(String srcFile , String destFile) throws IOException {
        TestSession.logger.info("************************************************************************************");
        boolean flag = false;
        FileSystem remoteFS = FileSystem.get(this.configuration);
        String absolutePath = new File("").getAbsolutePath();
        File integrationFilesPath = new File(absolutePath + "/resources/stack_integration/lib/");
        if (integrationFilesPath.exists()) {
            Path destPath = new Path(destFile);
            File fileList[] = integrationFilesPath.listFiles();
            for ( File f : fileList) {
                if (f.isFile()) {
                    Path scrFilePath = new Path(f.toString());
                    remoteFS.copyFromLocalFile(false , true, scrFilePath , destPath);
                    TestSession.logger.info( scrFilePath + "  files copied sucessfully to " + destPath);
                    flag = true;
                }
            }
        } 
        else {
            TestSession.logger.info(integrationFilesPath.toString() + " does not exists...");
        }
        TestSession.logger.info("************************************************************************************");
        return flag;
    }

    public void copyJobPropertiesFile(String jobPropertiesFilePath) throws IOException {

        FileSystem remoteFS = FileSystem.get(this.configuration);
        String jobPropertyFileFullPath = jobPropertiesFilePath + "/job.properties";
        TestSession.logger.info("job.properties file location - " + jobPropertyFileFullPath);
        String dest = "/tmp/" + this.getCurrentFrequencyValue() + "-job.properties";
        TestSession.logger.info("JobProperties destination path - " + dest);
        Path sourcePath = new Path(jobPropertyFileFullPath);
        boolean flag = remoteFS.exists(sourcePath);
        TestSession.logger.info("jobPropertyFileFullPath  exist = " + flag);
        if (flag == true) {
            Path destPath = new Path(dest);
            remoteFS.copyToLocalFile(sourcePath, destPath);
            TestSession.logger.info("");
        } else {
            TestSession.logger.info("Failed to copy " + jobPropertyFileFullPath   + "  to  " + dest);
        }
    }

    public void copyHiveFileToScratch(String dest ) throws IOException {
        TestSession.logger.info("************************************************************************************");
        TestSession.logger.info("*** copying hive-site.xml file  to " + dest);
        TestSession.logger.info("************************************************************************************");
        String hiveFileLocation = "/tmp/"+ this.currentFrequencyValue + "/hive-site.xml"; 
        File f = new File(hiveFileLocation);
        FileSystem remoteFS = FileSystem.get(this.configuration);
        Path destPath = new Path(dest);
        Path srcPath = new Path(hiveFileLocation);
        boolean flag  = f.exists();
        TestSession.logger.info("flag = " + flag);
        if (flag == true) {

            TestSession.logger.info(hiveFileLocation + "  file exits.");

            // assuming that hive.xml file exists
            remoteFS.copyFromLocalFile(srcPath, destPath);
            TestSession.logger.info("*************************** Copied " + hiveFileLocation + "  to  " + dest);
        }
    }


    public boolean copyLibFiles(String src , String dest) throws IOException {
        boolean flag = false;
        FileSystem remoteFS = FileSystem.get(this.configuration);
        File srcFile = new File(src);
        boolean srcFileExists = srcFile.exists();

        // if source path exist, get the file lists
        if (srcFileExists == true) {
            TestSession.logger.info(srcFileExists + " folder exists.");
            File []fileList = srcFile.listFiles();

            // check whether destination folder exists
            Path destPath = new Path(dest);
            boolean destFolderExists = remoteFS.exists(destPath);
            if (destFolderExists == true) {
                for ( File f : fileList) {
                    String fileName = f.toString();
                    Path p = new Path(fileName);
                    remoteFS.copyFromLocalFile(p, destPath);
                    TestSession.logger.info(fileName + "  copied.");
                }   
            } else if (destFolderExists == false)  {
                TestSession.logger.error(dest + " does not exists.");
                flag = false;
            }
        } else if (srcFileExists == false) {
            TestSession.logger.info(src + "");
            flag = false;
        }
        return flag;
    }
    
    private void createStatusFolder() throws IOException {
        Calendar todayCal = Calendar.getInstance();
        todayCal.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String toDayStatusFolder = "IntegrationStatus_"+ sdf.format(todayCal.getTime());
        this.setCurrentStatusFolderName(BASE_STATUS_FOLDER + toDayStatusFolder);
        TestSession.logger.info("Current Status Folder - " + this.getCurrentStatusFolderName());
        this.createWorkFlowFolder(this.getCurrentStatusFolderName());
    }
    
    private void createCurrentStatusFile() throws IOException {
        FileSystem remoteFS = FileSystem.get(this.configuration);
        Path currentStatusFolderPath = new Path(this.getCurrentStatusFolderName());
        
        // check whether current status folder exists, if not create one.
        if (this.getCurrentStatusFolderName().length() == 0 && remoteFS.exists(currentStatusFolderPath) == false) {
                this.createStatusFolder();
        } else if (this.getCurrentStatusFolderName().length() > 0 && remoteFS.exists(currentStatusFolderPath) == true) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
            Calendar todayCal = Calendar.getInstance();
            todayCal.setTimeZone(TimeZone.getTimeZone("UTC"));
            String statusFileName = "IntegrationStatus_"+ sdf.format(todayCal.getTime());
            this.setCurrentStatusFileName(this.getCurrentStatusFolderName() + "/" +  statusFileName);
            TestSession.logger.info("current Status FileName   = " + this.getCurrentStatusFileName());
            Path currentStatusFilePath = new Path(this.getCurrentStatusFileName());
            boolean isStatusFileCreated = remoteFS.createNewFile(currentStatusFilePath);
            if (isStatusFileCreated == true) {
                TestSession.logger.info(this.getCurrentStatusFileName() + " is successfully created");
            } else {
                TestSession.logger.info("Failed to create status file " + this.getCurrentStatusFileName());
            }
        }
    }
    
    public void setCurrentStatusFolderName(String currentStatusFolderName) {
        this.currentStatusFolderName = currentStatusFolderName;
    }
    
    public String getCurrentStatusFolderName() {
        return this.currentStatusFolderName;
    }
    
    public void setCurrentStatusFileName(String currentStatusFile) {
        this.currentStatusFileName = currentStatusFile;
    }
    
    public String getCurrentStatusFileName() {
        return this.currentStatusFileName;
    }
    
    /**
     * Write current job status to file
     * @throws IOException 
     */
    public void writeJobStatus() throws IOException {
        String reportFilePath = "/tmp/integrationTestResult.txt"; 
        File f = new File(reportFilePath);
        if (f.exists() == false) {
            if (f.createNewFile() == true) {
                TestSession.logger.info(f.toString() + " successfully created................................................................");
            } else {
                TestSession.logger.info(f.toString() + " faile to created................................................................");
            }
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(reportFilePath), this.getCurrentWorkingState().toString().getBytes());
    }
}
