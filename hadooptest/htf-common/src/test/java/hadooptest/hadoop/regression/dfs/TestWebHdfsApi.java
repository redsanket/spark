package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestWebHdfsApi extends DfsTestsBaseClass {

	static String KEYTAB_DIR = "keytabDir";
	static String KEYTAB_USER = "keytabUser";
	static String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
	static String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
	static String APPEND_STRING = "This string is appended at the end." + "\n";
	static String TRUE = "true";
	static String DFS_SUPPORT_APPEND = "dfs.support.append";
	static String FILE_GOES_AROUND = "__file_goes_around";
	static String MOVED_FROM_LOCAL = "__moved_from_local";
	static String MOVED_WITHIN_DFS = "__moved_within_dfs";
	static String MOVED_FROM_DFS_TO_LOCAL = "__moved_from_dfs_to_local";

	// Actions
	static String ACTION_COPY_FROM_LOCAL = "copyFromLocal";
	static String ACTION_COPY_TO_LOCAL = "copyToLocal";
	static String ACTION_APPEND_TO_FILE = "appendToFile";
	static String ACTION_RM = "rm";
	static String ACTION_RMDIR = "rmdir";
	static String ACTION_DUMP_STUFF = "dumpStuff";
	static String ACTION_MOVE_FROM_LOCAL = "moveFromLocal";
	static String ACTION_MOVE_WITHIN_DFS = "mv";
	static String ACTION_MOVE_TO_LOCAL = "moveToLocal";
	static String ACTION_CHECKSUM = "chucksum";

	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	private String schema;
	private String nameNode;

	@Test(expected = AccessControlException.class) public void checkPermissions_local() throws AccessControlException { checkPermissions(System.getProperty("CLUSTER_NAME")); }
    @Test(expected = AccessControlException.class) public void checkPermissions_remote() throws AccessControlException { checkPermissions(System.getProperty("REMOTE_CLUSTER")); }
    
	@Test public void appendToFile_local() throws Exception { appendToFile(System.getProperty("CLUSTER_NAME")); }
	@Test public void appendToFile_remote() throws Exception { appendToFile(System.getProperty("REMOTE_CLUSTER")); }
	
	@Test public void testdoAMovesInAndOutOfClusterAndChecksum_local() throws Exception { testdoAMovesInAndOutOfClusterAndChecksum(System.getProperty("CLUSTER_NAME")); }
	@Test public void testdoAMovesInAndOutOfClusterAndChecksum_remote() throws Exception { testdoAMovesInAndOutOfClusterAndChecksum(System.getProperty("REMOTE_CLUSTER")); }
	
	private void setupTest(String cluster) {

		Properties crossClusterProperties = new Properties();
		try {
			crossClusterProperties
					.load(new FileInputStream(
							HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		this.schema = HadooptestConstants.Schema.WEBHDFS;
        this.nameNode = TestSession.getNamenodeURL(cluster);
		logger.info("Invoked test for:[" + cluster + "] Scheme:[" + schema
				+ "] NodeName:[" + nameNode + "]");
		
		copyFilesOntoHadoopFS();
	}

	/*
	 * TODO:350MB is a good size, forces use of multiple blocks. Future
	 * enhancement suggestion, use file sizes of 0, 1, DEFAULT_BLOCKSIZE-1,
	 * DEFAULT_BLOCKSIZE, DEFAULT_BLOCKSIZE+1, DEFAULT_BLOCKSIZEx10 bytes to
	 * alter the filesizes used in testing, and cycle across.
	 */
	@BeforeClass
	public static void testSessionStart() throws Exception {
		TestSession.start();

		// Populate the details for HADOOPQA
		HashMap<String, String> fileOwnerUserDetails = new HashMap<String, String>();
		fileOwnerUserDetails.put(KEYTAB_DIR,
				HadooptestConstants.Location.Keytab.HADOOPQA);
		fileOwnerUserDetails.put(KEYTAB_USER,
				HadooptestConstants.UserNames.HADOOPQA);
		fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"
				+ HadooptestConstants.UserNames.HADOOPQA + "Dir/"
				+ HadooptestConstants.UserNames.HADOOPQA + "File");
		fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS,
				HadooptestConstants.UserNames.DFSLOAD);
		TestWebHdfsApi.supportingData.put(
				HadooptestConstants.UserNames.HADOOPQA, fileOwnerUserDetails);

		// Populate the details for DFSLOAD
		fileOwnerUserDetails = new HashMap<String, String>();
		fileOwnerUserDetails.put(KEYTAB_DIR,
				HadooptestConstants.Location.Keytab.DFSLOAD);
		fileOwnerUserDetails.put(KEYTAB_USER,
				HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM");

		fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"
				+ HadooptestConstants.UserNames.DFSLOAD + "Dir/"
				+ HadooptestConstants.UserNames.DFSLOAD + "File");
		fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS,
				HadooptestConstants.UserNames.HADOOPQA);

		TestWebHdfsApi.supportingData.put(
				HadooptestConstants.UserNames.DFSLOAD, fileOwnerUserDetails);
		logger.info("CHECK:" + TestWebHdfsApi.supportingData);
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aFileName = TestWebHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Create a local file
			createLocalFile(aFileName);
		}
	}

	private void copyFilesOntoHadoopFS() {
		try {
			testSessionStart();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aOwnersFileName = TestWebHdfsApi.supportingData.get(aUser)
					.get(OWNED_FILE_WITH_COMPLETE_PATH);

			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);

			try {
				DoAs doAs;
				// First copy the file to the remote DFS'es
				doAs = new DoAs(ugi, ACTION_COPY_FROM_LOCAL, aConf,
						aOwnersFileName, aOwnersFileName);
				doAs.doAction();

			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void checkPermissions(String cluster) throws AccessControlException {
	    setupTest(cluster);
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aFileName = TestWebHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);

			DoAs doAs;
			try {
				// Check Permissions
				String userWithouPermission = TestWebHdfsApi.supportingData
						.get(aUser).get(USER_WHO_DOESNT_HAVE_PERMISSIONS);
				UserGroupInformation ugiNoPermission = getUgiForUser(userWithouPermission);

				doAs = new DoAs(ugiNoPermission, ACTION_APPEND_TO_FILE, aConf,
						aFileName, null);
				doAs.doAction();
				try {
					cleanupAfterTest();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (AccessControlException acx) {
				throw acx;
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	// see hangs issuing 'test -f' on NN @Monitorable
	private void appendToFile(String cluster) throws Exception {
        setupTest(cluster);
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aFileName = TestWebHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);

			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);
			DoAs doAs;

			// Append to 'em files
			doAs = new DoAs(ugi, ACTION_APPEND_TO_FILE, aConf, aFileName, null);
			doAs.doAction();

			// Dump the FS values
			doAs = new DoAs(ugi, ACTION_DUMP_STUFF, aConf, aFileName, null);
			doAs.doAction();
		}
		cleanupAfterTest();
	}

	// see hangs issuing 'test -f' on NN @Monitorable
	private void testdoAMovesInAndOutOfClusterAndChecksum(String cluster) throws Exception {
        setupTest(cluster);
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aFileName = TestWebHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Re-create the local file as it gets moved
			createLocalFile(aFileName + FILE_GOES_AROUND);
			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);
			DoAs doAs;
			try {
				String previousFileName = aFileName + FILE_GOES_AROUND;
				String newFileName = previousFileName + MOVED_FROM_LOCAL;
				// Move from local
				doAs = new DoAs(ugi, ACTION_MOVE_FROM_LOCAL, aConf,
						previousFileName, newFileName);
				doAs.doAction();

				// Get Checksum, to compare later
				doAs = new DoAs(ugi, ACTION_CHECKSUM, aConf, newFileName, null);
				String chkSumWhenFileJustCopiedToDfs = doAs.doAction();
				logger.info("DoAs did return checksum as:"
						+ chkSumWhenFileJustCopiedToDfs);

				// Move within DFS
				previousFileName = newFileName;
				newFileName += MOVED_WITHIN_DFS;
				doAs = new DoAs(ugi, ACTION_MOVE_WITHIN_DFS, aConf,
						previousFileName, newFileName);
				doAs.doAction();

				// Get Checksum, to compare now
				doAs = new DoAs(ugi, ACTION_CHECKSUM, aConf, newFileName, null);
				String chkSumAfterWithinDfsMove = doAs.doAction();
				logger.info("DoAs did return checksum (2nd time) as:"
						+ chkSumAfterWithinDfsMove);

				Assert.assertEquals("Checksums did not match!",
						chkSumWhenFileJustCopiedToDfs, chkSumAfterWithinDfsMove);

				// Move to local
				previousFileName = newFileName;
				newFileName += MOVED_FROM_DFS_TO_LOCAL;
				doAs = new DoAs(ugi, ACTION_MOVE_TO_LOCAL, aConf,
						previousFileName, newFileName);
				doAs.doAction();

				cleanupAfterTest();

			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	// @After
	public void cleanupAfterTest() throws Exception {
		for (String aUser : TestWebHdfsApi.supportingData.keySet()) {
			String aFileName = TestWebHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);

			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);
			DoAs doAs;

			// Delete 'em files
			doAs = new DoAs(ugi, ACTION_RM, aConf, aFileName, null);
			doAs.doAction();
			// Delete the directory
			doAs = new DoAs(ugi, ACTION_RMDIR, aConf,
					new File(aFileName).getParent(), null);
			doAs.doAction();
		}
	}

	class PrivilegedExceptionActionImpl implements
			PrivilegedExceptionAction<String> {
		UserGroupInformation ugi;
		String action;
		Configuration configuration;
		String oneFile;
		String otherFile;

		PrivilegedExceptionActionImpl(UserGroupInformation ugi, String action,
				Configuration configuration, String firstFile, String secondFile)
				throws IOException {
			this.ugi = ugi;
			this.action = action;
			this.configuration = configuration;
			this.oneFile = firstFile;
			this.otherFile = secondFile;
		}

		String retStr = null;

		public String getReturnString() {
			return retStr;
		}

		public void setReturnString(String returnString) {
			this.retStr = returnString;
		}

		public String run() throws IOException {
			String returnString = null;

			logger.info("Doing action[" + action + "] as [" + ugi.getUserName()
					+ "] with oneFile[" + oneFile + "] & otherFile["
					+ otherFile + "]");
			FileSystem aRemoteFS = FileSystem.get(configuration);
			if (action.equals(ACTION_COPY_FROM_LOCAL)) {
				try {
					aRemoteFS.copyFromLocalFile(false, true, new Path(oneFile),
							new Path(otherFile));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else if (action.equals(ACTION_APPEND_TO_FILE)) {
				configuration.set(DFS_SUPPORT_APPEND, TRUE);
				aRemoteFS = FileSystem.get(configuration);
				FSDataOutputStream fsout;
				try {
					fsout = aRemoteFS.append(new Path(oneFile));
					// create a byte array of 10000 bytes, to append
					int len = 10000;
					byte[] data = new byte[len];
					for (int i = 0; i < len; i++) {
						data[i] = new Integer(i).byteValue();
					}
					fsout.write(data);
					fsout.close();
				} catch (AccessControlException acx) {
					throw acx;
				}
			} else if (action.equals(ACTION_RM)) {
				try {
					aRemoteFS.delete(new Path(oneFile), false);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else if (action.equals(ACTION_RMDIR)) {
				try {
					// Do a recursive delete (should delete the dir name
					// as well)
					aRemoteFS.delete(new Path(oneFile), true);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else if (action.equals(ACTION_DUMP_STUFF)) {
				try {
					System.out
							.println("[file in question <-------------------------------------------------------------------->"
									+ oneFile);
					logger.info("Dumping UGI details ");
					logger.info("UGI Current User (Static)"
							+ UserGroupInformation.getCurrentUser()
									.getUserName());
					logger.info("UGI Login User (Static)"
							+ UserGroupInformation.getLoginUser().getUserName());
					logger.info("UGI Short User Name " + ugi.getShortUserName());
					logger.info("UGI User Name " + ugi.getUserName());
					for (String aGrpName : ugi.getGroupNames()) {
						logger.info(" Grp:" + aGrpName);
					}
					logger.info("UGI Real User " + ugi.getRealUser());
					System.out
							.println("=======================================================================");
					logger.info("Canonical Service name:"
							+ aRemoteFS.getCanonicalServiceName());
					// logger.info("Default Block Size:"
					// + aRemoteFS.getDefaultBlockSize());
					logger.info("Default Block Size Path:"
							+ aRemoteFS.getDefaultBlockSize(new Path(oneFile)));
					// logger.info("Default Replication:"
					// + aRemoteFS.getDefaultReplication());
					logger.info("Default Replication Path:"
							+ aRemoteFS
									.getDefaultReplication(new Path(oneFile)));
					logger.info("Used:" + aRemoteFS.getUsed());
					// for (FileSystem
					// aFileSystem:aRemoteFS.getChildFileSystems()){
					// if (aFileSystem != null){
					// logger.info("Child FS Name:" +
					// aFileSystem.getUri().toString());
					// }
					// }
					ContentSummary cs = aRemoteFS.getContentSummary(new Path(
							oneFile));
					logger.info("Content Summary:" + cs.toString());
					for (String renewer : new String[] { "hadoopqa", "dfsload" }) {
						Token<?> aToken = aRemoteFS
								.getDelegationToken("hadoopqa");
						logger.info("Token Kind:" + aToken.getKind());
						logger.info("Token Service:" + aToken.getService());
					}
					FileStatus fileStatus = aRemoteFS.getFileStatus(new Path(
							oneFile));
					logger.info("Dumping file Status:");
					logger.info(" Access Time:" + fileStatus.getAccessTime());
					logger.info(" Block Size:" + fileStatus.getBlockSize());
					logger.info(" Group:" + fileStatus.getGroup());
					logger.info(" Length:" + fileStatus.getLen());
					logger.info(" Modification time:"
							+ fileStatus.getModificationTime());
					logger.info(" Owner:" + fileStatus.getOwner());
					logger.info(" Replication:" + fileStatus.getReplication());
					logger.info(" Path:" + fileStatus.getPath().toString());
					logger.info(" Permissions:"
							+ fileStatus.getPermission().toString());
					// logger.info(" Symlink:" +
					// fileStatus.getSymlink().toString();
					BlockLocation[] blockLocs = aRemoteFS
							.getFileBlockLocations(fileStatus, 0, 50);
					System.out.println("Dumping file Block locations_____");
					for (BlockLocation blockLok : blockLocs) {
						logger.info(" Block Length:" + blockLok.getLength());
						logger.info(" Block Offset:" + blockLok.getOffset());
						String[] hosts = blockLok.getHosts();
						for (String aHost : hosts) {
							logger.info(" Host:" + aHost);
						}

						String[] names = blockLok.getNames();
						for (String aName : names) {
							logger.info(" Name:" + aName);
						}
						logger.info("Block Offset:"
								+ blockLok.getTopologyPaths());
						String[] topPaths = blockLok.getTopologyPaths();
						for (String aTopPath : topPaths) {
							logger.info(" Topology Path:" + aTopPath);
						}
					}
					logger.info("Home dir:" + aRemoteFS.getHomeDirectory());
					logger.info("URI:" + aRemoteFS.getUri());
				} catch (IOException e) {
					throw (e);
				}
			} else if (action.equals(ACTION_MOVE_FROM_LOCAL)) {
				aRemoteFS.moveFromLocalFile(new Path(oneFile), new Path(
						otherFile));
			} else if (action.equals(ACTION_MOVE_WITHIN_DFS)) {
				aRemoteFS.rename(new Path(oneFile), new Path(otherFile));
			} else if (action.equals(ACTION_MOVE_TO_LOCAL)) {
				aRemoteFS.moveToLocalFile(new Path(oneFile),
						new Path(otherFile));
			} else if (action.equals(ACTION_CHECKSUM)) {
				FileChecksum aChecksum = aRemoteFS.getFileChecksum(new Path(
						oneFile));
				returnString = Hex.encodeHexString(aChecksum.getBytes());
				this.retStr = returnString;
				logger.info("Checksum string[" + returnString + "]");
			}
			return returnString;
		}
	}

	class DoAs {
		UserGroupInformation ugi;
		String action;
		Configuration configuration;
		String oneFile;
		String otherFile;

		DoAs(UserGroupInformation ugi, String action,
				Configuration configuration, String firstFile, String secondFile)
				throws IOException {
			this.ugi = ugi;
			this.action = action;
			this.configuration = configuration;
			this.oneFile = firstFile;
			this.otherFile = secondFile;
		}

		public String doAction() throws AccessControlException, IOException,
				InterruptedException {
			PrivilegedExceptionActionImpl privilegedExceptionActor = new PrivilegedExceptionActionImpl(
					ugi, action, configuration, oneFile, otherFile);
			ugi.doAs(privilegedExceptionActor);
			return privilegedExceptionActor.getReturnString();
		}
	}

	Configuration getConfForRemoteFS(String aUser) {
		Configuration conf = new Configuration(true);

		if (this.schema.equals(HadooptestConstants.Schema.HDFS)) {

			String namenodeWithChangedSchema = this.nameNode.replace(
					HadooptestConstants.Schema.HTTP,
					HadooptestConstants.Schema.HDFS);
			String namenodeWithChangedSchemaAndPort = namenodeWithChangedSchema
					.replace("50070", HadooptestConstants.Ports.HDFS);
			logger.info("For HDFS set the namenode to:["
					+ namenodeWithChangedSchemaAndPort + "]");
			conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
			conf.set("dfs.namenode.kerberos.principal",
					"hdfs/_HOST@DEV.YGRID.YAHOO.COM");
		} else {
			// WEBHDFS
			String namenodeWithChangedSchema = this.nameNode.replace(
					HadooptestConstants.Schema.HTTP,
					HadooptestConstants.Schema.WEBHDFS);
			String namenodeWithChangedSchemaAndNoPort = namenodeWithChangedSchema
					.replace(":50070", "");
			logger.info("For WEBHDFS set the namenode to:["
					+ namenodeWithChangedSchemaAndNoPort + "]");
			conf.set("fs.defaultFS", namenodeWithChangedSchemaAndNoPort);
		}
		if (aUser.equals(HadooptestConstants.UserNames.HADOOPQA)) {
			conf.set("hadoop.job.ugi", HadooptestConstants.UserNames.HADOOPQA);
		} else {
			conf.set("hadoop.job.ugi", HadooptestConstants.UserNames.DFSLOAD);
		}
		conf.set("hadoop.security.authentication", "true");
		logger.info(conf);
		return conf;
	}

	UserGroupInformation getUgiForUser(String aUser) {

		String keytabUser = TestWebHdfsApi.supportingData.get(aUser).get(
				KEYTAB_USER);
		logger.info("Set keytab user=" + keytabUser);
		String keytabDir = TestWebHdfsApi.supportingData.get(aUser).get(
				KEYTAB_DIR);
		logger.info("Set keytab dir=" + keytabDir);
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
					keytabUser, keytabDir);
			logger.info("UGI=" + ugi);
			logger.info("credentials:" + ugi.getCredentials());
			logger.info("group names" + ugi.getGroupNames());
			logger.info("real user:" + ugi.getRealUser());
			logger.info("short user name:" + ugi.getShortUserName());
			logger.info("token identifiers:" + ugi.getTokenIdentifiers());
			logger.info("tokens:" + ugi.getTokens());
			logger.info("username:" + ugi.getUserName());
			logger.info("current user:" + UserGroupInformation.getCurrentUser());
			logger.info("login user:" + UserGroupInformation.getLoginUser());

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return ugi;
	}

	static void createLocalFile(String aFileName) {
		logger.info("!!!!!!! Creating local file:" + aFileName);
		File attemptedFile = new File(aFileName);

		if (attemptedFile.exists()) {
			attemptedFile.delete();
		}
		// create a file on the local fs
		if (!attemptedFile.getParentFile().exists()) {
			attemptedFile.getParentFile().mkdirs();
		}

		FileOutputStream fout;
		try {
			fout = new FileOutputStream(attemptedFile);
			// create a byte array of 350MB (367001600) bytes
			int len = 36700;
			byte[] data = new byte[len];
			for (int i = 0; i < len; i++) {
				data[i] = new Integer(i).byteValue();
			}
			fout.write(data);
			fout.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


}
