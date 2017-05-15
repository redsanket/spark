package hadooptest.gdm.regression.stress;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Test;

/**
 * Class that creates the instance(s) and instance files on the source cluster.
 * 
 */
public class StressTestingInit   {

	private static final String PROCOTOL = "hdfs://";
	private static final String KEYTAB_DIR = "keytabDir";
	private static final String KEYTAB_USER = "keytabUser";
	private static final String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
	private static final String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
	private static final String PATH = "/data/daqdev";
	private static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
	private List<String> instance ;
	private String schema;
	private String dataSourceFolderName;
	private String noOfInstance;
	private String noOfFilesInInstance;
	private String deploymentSuffixName;
	private String sourceFilePath;
	private String nameNodeName;
	
	public StressTestingInit() {
		
	}

	public StressTestingInit(String deploymentSuffixName , String noOfInstance , String noOfFilesInInstance , String nameNodeName) throws  NumberFormatException, Exception {
		this.deploymentSuffixName =  deploymentSuffixName;
		this.noOfInstance = noOfInstance;
		this.noOfFilesInInstance = noOfFilesInInstance;
		this.nameNodeName = nameNodeName;

		// Populate the details for DFSLOAD
		HashMap<String, String> fileOwnerUserDetails = new HashMap<String, String>();
		fileOwnerUserDetails = new HashMap<String, String>();
		fileOwnerUserDetails.put(KEYTAB_DIR, HadooptestConstants.Location.Keytab.DFSLOAD);
		fileOwnerUserDetails.put(KEYTAB_USER, HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM");
		fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"+ HadooptestConstants.UserNames.DFSLOAD + "Dir/" + HadooptestConstants.UserNames.DFSLOAD + "File");
		fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS, HadooptestConstants.UserNames.HADOOPQA);

		this.supportingData.put(HadooptestConstants.UserNames.DFSLOAD,fileOwnerUserDetails);
		TestSession.logger.info("CHECK:" + this.supportingData);
		this.schema = HadooptestConstants.Schema.HDFS;

		// destination folder name that will be created on the grid.
		this.dataSourceFolderName = "Data_" + System.currentTimeMillis();
		this.instance = new ArrayList<String>();

		// create instances names
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String endDate = sdf.format(cal.getTime());
		TestSession.logger.info("endDate  = "  + endDate );
		this.instance.add(endDate);
		
		for ( int i = 0; i < Integer.parseInt(this.noOfInstance) ; i++ )  {
			cal.add(Calendar.DAY_OF_MONTH, -1);
			this.instance.add(sdf.format(cal.getTime()));	
		}
		
		String startDate = sdf.format(cal.getTime());
		TestSession.logger.info("startDate -  "  + startDate);

		// sort the instance list, so that we can get the start date and end date
		Collections.sort(instance);

		// navigate instances 
		for ( String date : instance) {
			TestSession.logger.info("date = " + date);
		}
	}

	/**
	 * Returns the path of the datasource i,e location of the instance and instance files.
	 * @return
	 */
	public String getDataSourcePath() {
		return this.dataSourceFolderName;
	}

	/**
	 * returns the instances as list
	 * @return
	 */
	public List<String> getInstances() {
		return this.instance;
	}
	
	public HashMap<String, HashMap<String, String>> getSupportingDataMap(){
		return this.supportingData;
	}

	public void execute() throws IOException, InterruptedException {
		for (String aUser : this.supportingData.keySet()) {
			TestSession.logger.info("aUser = " + aUser);
			Configuration configuration = getConfForRemoteFS();
			UserGroupInformation ugi = getUgiForUser(aUser);

			CreateInstancesAndInstanceFiles createInstances = new CreateInstancesAndInstanceFiles(this.PATH , configuration , this.dataSourceFolderName , this.instance , this.sourceFilePath , this.noOfFilesInInstance);
			String result = ugi.doAs(createInstances);
			//PrivilegedExceptionActionImpl privilegedExceptionActioniImplOject = new PrivilegedExceptionActionImpl(this.PATH , configuration , this.dataSourceFolderName , this.instance , this.sourceFilePath , this.noOfFilesInInstance);
			//String result = ugi.doAs(privilegedExceptionActioniImplOject);
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
		conf.set("dfs.checksum.type" , "CRC32C");
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
			TestSession.logger.info("UGI=" + ugi.toString());
			TestSession.logger.info("credentials:" + ugi.getCredentials());
			TestSession.logger.info("group names" + ugi.getGroupNames());
			TestSession.logger.info("real user:" + ugi.getRealUser());
			TestSession.logger.info("short user name:" + ugi.getShortUserName());
			TestSession.logger.info("token identifiers:" + ugi.getTokenIdentifiers());
			TestSession.logger.info("tokens:" + ugi.getTokens());
			TestSession.logger.info("username:" + ugi.getUserName());
			TestSession.logger.info("current user:" + UserGroupInformation.getCurrentUser());
			TestSession.logger.info("login user:" + UserGroupInformation.getLoginUser());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return ugi;
	}

	/**
	 * CreateInstancesAndInstanceFiles class that create a file on the specified cluster.
	 */
	class CreateInstancesAndInstanceFiles implements PrivilegedExceptionAction<String> {
		Configuration configuration;
		String basePath ;
		String destinationFolder;
		String srcFilePath;
		String instanceFileCount;
		List<String>instanceFolderNames;

		public CreateInstancesAndInstanceFiles(String basePath , Configuration configuration , String destinationFolder , List<String>instanceDates , String srcFilePath  , String instanceFileCount) {
			this.configuration = configuration;
			this.basePath = basePath;
			this.destinationFolder = destinationFolder;
			this.instanceFolderNames = instanceDates;
			this.srcFilePath = srcFilePath;
			this.instanceFileCount = instanceFileCount;
		}

		/**
		 * An override methat is creates an instance and gz files 
		 */
		@Override
		public String run() throws Exception {
			String returnValue = "false";

			TestSession.logger.info("configuration   =  " + this.configuration.toString());
			TestSession.logger.info("Instance folder names = " +this.instanceFolderNames   + "  number of files =   "  +  this.instanceFileCount);

			FileSystem remoteFS = FileSystem.get(this.configuration);
			Path path = new Path(this.basePath.trim());

			// check whether remote path exists on the grid
			boolean flag = remoteFS.exists(path);
			if (flag == true) {
				String destFolder = this.basePath.trim() + this.destinationFolder;
				Path path1 = new Path(destFolder);
				boolean dirFlag = remoteFS.mkdirs(path1);
				if ( dirFlag ) {

					assertTrue("Failed to create " +  destFolder  , dirFlag == true);
					TestSession.logger.info(destFolder + " created Sucessfully.");

					// create an instance file
					for (int i = 0 ; i< instanceFolderNames.size() - 1 ; i++) {
						String instance  = instanceFolderNames.get(i).trim();
						Path instancePath = new Path( this.basePath +  "/" + this.destinationFolder + "/" + instance );
						boolean isInstanceCreated =  remoteFS.mkdirs(instancePath);
						TestSession.logger.info( instancePath.toString() + " is ceated " + isInstanceCreated );
						assertTrue("Failed to create instance directory - " + this.basePath + "/" + instance , isInstanceCreated == true);

						// create instance files
						for ( int j=0;j< Integer.parseInt(instanceFileCount) ; j++) {
							String destFile = this.basePath +  "/" + this.destinationFolder + "/" + instance + "/" + "instanceFile_" + j +".gz";
							TestSession.logger.info("destFile  = " + destFile);

							Path destFilePath = new Path(destFile);
							FSDataOutputStream fsDataOutPutStream = remoteFS.create(destFilePath, false);

							// create a byte array of 350MB (367001600) bytes
							int len = 36700;
							byte[] data = new byte[len];
							for (int k = 0; k < len; k++) {
								data[k] = new Integer(k).byteValue();
							}
							fsDataOutPutStream.write(data);
							fsDataOutPutStream.close();
							TestSession.logger.info( destFile  + " succcessfully created.");
							returnValue = "success";
						}
					}
				}
				return returnValue;
			} else { 
				return returnValue; 
			}
		}

	}
}
