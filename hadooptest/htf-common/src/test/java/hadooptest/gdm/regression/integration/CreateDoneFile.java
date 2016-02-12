package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class CreateDoneFile implements PrivilegedExceptionAction<String> {

	private String clusterName;
	private String dataPath;
	private String nameNodeName;
	private String result;
	private ConsoleHandle consoleHandle;
	private Configuration configuration;
	private static final String schema = HadooptestConstants.Schema.HDFS;
	private static final String PROCOTOL = "hdfs://";
	private static final String KEYTAB_DIR = "keytabDir";
	private static final String KEYTAB_USER = "keytabUser";
	private static final String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
	private static final String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
	private static final int NO_OF_INSTANCE = 5;
	private static final int DAYS = 20;
	private static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	public CreateDoneFile(String clusterName , String dataPath ) {
		this.clusterName = clusterName;
		this.dataPath = dataPath;
		
		TestSession.logger.info("clusterName   - " + clusterName    + "  dataPath =     "  + dataPath );

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
	}

	public boolean isDoneFileCreated() {
		if(this.result == "success") {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Invoke run method and create the instance files on the specified cluster.
	 */
	public void execute() throws IOException, InterruptedException {
		for (String aUser : this.supportingData.keySet()) {
			TestSession.logger.info("aUser = " + aUser);
			this.configuration = getConfForRemoteFS();
			UserGroupInformation ugi = getUgiForUser(aUser);
			this.result = ugi.doAs(this);
			TestSession.logger.info("Result = " + this.result);
			if (! this.result.equals("success")) {
				fail("Failed to create DONE file under " + this.dataPath);
			}
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
		conf.set("dfs.checksum.type" , "CRC32");
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

	public String run() throws Exception {
		String returnValue = "false";
		TestSession.logger.info("configuration   =  " + this.configuration.toString());
		FileSystem remoteFS = FileSystem.get(this.configuration);
		TestSession.logger.info("dataPath = " + this.dataPath);
		Path path = new Path(this.dataPath.trim());

		// check whether remote path exists on the grid
		boolean basePathExists = remoteFS.exists(path);
		TestSession.logger.info("basePathExists  = " + basePathExists);
		if(basePathExists == false) {
			fail(this.dataPath.trim() + " does not exist on cluster " + clusterName);
			returnValue = "failed";
		}  
		if (basePathExists == true) {
			TestSession.logger.info(this.dataPath.trim()  + " file exists on the cluster " + clusterName);
			returnValue = "success";
		}
		return returnValue;
	}

}
