package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;

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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestHdfsApi extends DfsTestsBaseClass {

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

	private static HashMap<String, String> versionStore;
	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	private String schema;
	private String nameNode;
	private String cluster;
	private String localHadoopVersion;
	private String remoteHadoopVersion;
	private String parametrizedCluster;

	@Test public void copyFilesOntoHadoopFS_local() throws IOException, InterruptedException { copyFilesOntoHadoopFS(System.getProperty("CLUSTER_NAME")); }
    @Test public void copyFilesOntoHadoopFS_remote() throws IOException, InterruptedException { copyFilesOntoHadoopFS(System.getProperty("REMOTE_CLUSTER")); }
	
    @Test(expected = AccessControlException.class) public void checkPermissions_local() throws IOException, InterruptedException { checkPermissions(System.getProperty("CLUSTER_NAME")); }
    @Test(expected = AccessControlException.class) public void checkPermissions_remote() throws IOException, InterruptedException { checkPermissions(System.getProperty("REMOTE_CLUSTER")); }
    
    @Test public void appendToFile_local() throws Exception { appendToFile(System.getProperty("CLUSTER_NAME")); }
    @Test public void appendToFile_remote() throws Exception { appendToFile(System.getProperty("REMOTE_CLUSTER")); }
    
    @Test public void testdoAMovesInAndOutOfClusterAndChecksum_local() { testdoAMovesInAndOutOfClusterAndChecksum(System.getProperty("CLUSTER_NAME")); }
    @Test public void testdoAMovesInAndOutOfClusterAndChecksum_remote() { testdoAMovesInAndOutOfClusterAndChecksum(System.getProperty("REMOTE_CLUSTER")); }
    
	private void testSetup(String cluster) {
		this.parametrizedCluster = cluster;
		Properties crossClusterProperties = new Properties();
		try {
			crossClusterProperties
					.load(new FileInputStream(
							HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		this.cluster = cluster;
		this.schema = HadooptestConstants.Schema.HDFS;
        this.nameNode = TestSession.getNamenodeURL(cluster);
		logger.info("Invoked test for:[" + cluster + "] Scheme:[" + schema
				+ "] NodeName:[" + nameNode + "]");
		
		getVersions();
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
		TestHdfsApi.supportingData.put(HadooptestConstants.UserNames.HADOOPQA,
				fileOwnerUserDetails);

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

		TestHdfsApi.supportingData.put(HadooptestConstants.UserNames.DFSLOAD,
				fileOwnerUserDetails);
		logger.info("CHECK:" + TestHdfsApi.supportingData);
		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aFileName = TestHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Create a local file
			createLocalFile(aFileName);
		}
		versionStore = new HashMap<String, String>();
	}

	private void getVersions() {
		ResourceManagerHttpUtils rmUtils = new ResourceManagerHttpUtils();
		if (versionStore.containsKey(this.localCluster)) {
			// Do not make an unnecessary call to get the version, if you've
			// already made it once.
			localHadoopVersion = versionStore.get(this.localCluster);
		} else {
			localHadoopVersion = rmUtils.getHadoopVersion(this.localCluster);
			localHadoopVersion = localHadoopVersion.split("\\.")[0];
			versionStore.put(this.localCluster, localHadoopVersion);
		}

		if (versionStore.containsKey(this.parametrizedCluster)) {
			// Do not make an unnecessary call to get the version, if you've
			// already made it once.
			remoteHadoopVersion = versionStore.get(this.parametrizedCluster);
		} else {
			remoteHadoopVersion = rmUtils
					.getHadoopVersion(this.parametrizedCluster);
			remoteHadoopVersion = remoteHadoopVersion.split("\\.")[0];
			versionStore.put(this.parametrizedCluster, remoteHadoopVersion);

		}

	}

	// see hangs issuing 'test -f' on NN  @Monitorable
	private void copyFilesOntoHadoopFS(String cluster) throws IOException,
			InterruptedException {
	    testSetup(cluster);
	    
		logger.info("traceMethod:copyFilesOntoHadoopFS");
		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aOwnersFileName = TestHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);

			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);

			try {
				DoAs doAs;
				// First copy the file to the remote DFS'es
				doAs = new DoAs(ugi, ACTION_COPY_FROM_LOCAL, aConf,
						aOwnersFileName, aOwnersFileName);
				logger.info("traceMethod:copyFilesOntoHadoopFS beginning copyFromLocal for user "
						+ aUser + " cluster " + this.cluster);
				doAs.doAction();
				logger.info("traceMethod:copyFilesOntoHadoopFS after copyFromLocal for user "
						+ aUser + " cluster " + this.cluster);

			} catch (IOException e) {
				logger.error("Localized message:" + e.getLocalizedMessage());
				if (localHadoopVersion.startsWith("2")
						&& remoteHadoopVersion.startsWith("0")) {
					if (e.getLocalizedMessage().contains("Broken pipe")) {
						logger.info("got expected exception (Broken pipe) when using HDFS to "
								+ ACTION_COPY_FROM_LOCAL
								+ " from 2.x to 0.23, for user=" + aUser);

					} else if (e.getClass().isAssignableFrom(
							java.io.EOFException.class)) {
						// Yes, you get a different exception if the user is
						// different (for the same action)....go figure!
						// If you try the same action from command line, you
						// always get a "Broken pipe" exception
						// I think this happens because of the doAs block.
						logger.info("got expected exception (EOFException) when using HDFS to "
								+ ACTION_COPY_FROM_LOCAL
								+ " from 2.x to 0.23, for user=" + aUser);
					}

				} else if (localHadoopVersion.startsWith("0")
						&& remoteHadoopVersion.startsWith("2")) {
					// Trying to copy, via hdfs:// from 0.23 cluster into a 2.0
					// cluster
					if (e.getLocalizedMessage().contains(
							"org.apache.hadoop.ipc.RPC$VersionMismatch")) {
						logger.info("got expected exception (RPCVersionMismatch) when using HDFS to "
								+ ACTION_COPY_FROM_LOCAL
								+ " from 0.23 to 2.x, user=" + aUser);
					}
				} else {
					logger.error("Unexpected exception, on doing "
							+ ACTION_COPY_FROM_LOCAL + " from version "
							+ localHadoopVersion + " to version "
							+ remoteHadoopVersion);
					throw (e);
				}
			} catch (InterruptedException e) {
				throw (e);
			}
		}
	}
	
	// see hangs issuing 'test -f' on NN  @Monitorable
	private void checkPermissions(String cluster) throws IOException, InterruptedException {
        testSetup(cluster);
		logger.info("traceMethod:checkPermissions");
		if (!localHadoopVersion.equals(remoteHadoopVersion)) {
			// Test not valid, because the file would not have got copied to
			// begin with, because of the version mismatch.
			// Throw the Exception that the test is expecting.
			throw new AccessControlException();
		}
		copyFilesOntoHadoopFS(cluster);
		logger.info("traceMethod, after copyFiles");
		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aFileName = TestHdfsApi.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);

			DoAs doAs;
			try {
				// Check Permissions
				String userWithouPermission = TestHdfsApi.supportingData.get(
						aUser).get(USER_WHO_DOESNT_HAVE_PERMISSIONS);
				UserGroupInformation ugiNoPermission = getUgiForUser(userWithouPermission);
				logger.info("In traceMethod:checkPermissions, beginning processing for "
						+ aUser + " for cluster " + this.cluster);
				doAs = new DoAs(ugiNoPermission, ACTION_APPEND_TO_FILE, aConf,
						aFileName, null);
				doAs.doAction();
				logger.info("In traceMethod:checkPermissions, beginning processing for "
						+ aUser + " for cluster " + this.cluster);

			} catch (AccessControlException acx) {
				throw acx;
			} catch (IOException e) {
				throw (e);
			} catch (InterruptedException e) {
				throw (e);
			}
		}
	}

	// see hangs issuing 'test -f' on NN  @Monitorable
	private void appendToFile(String cluster) throws Exception {
        testSetup(cluster);
		logger.info("traceMethod:appendToFile");
		if (!localHadoopVersion.equals(remoteHadoopVersion)) {
			// Test not valid, because the file would not have got copied to
			// begin with, because of the version mismatch
			return;
		}
		// Copy over the files.
		copyFilesOntoHadoopFS(cluster);

		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aFileName = TestHdfsApi.supportingData.get(aUser).get(
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
	}

	// see hangs issuing 'test -f' on NN  @Monitorable
	private void testdoAMovesInAndOutOfClusterAndChecksum(String cluster) {
        testSetup(cluster);
		logger.info("traceMethod:testdoAMovesInAndOutOfClusterAndChecksum");
		if (!localHadoopVersion.equals(remoteHadoopVersion)) {
			// Test not valid, because the file would not have got copied to
			// begin with, because of the version mismatch
			return;
		}

		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aFileName = TestHdfsApi.supportingData.get(aUser).get(
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

			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@After
	public void cleanupAfterTest() throws Exception {
		logger.info("traceMethod:cleanupAfterTest");
		if (!localHadoopVersion.equals(remoteHadoopVersion)) {
			// Test not valid, because the file would not have got copied to
			// begin with, because of the version mismatch
			return;
		}

		for (String aUser : TestHdfsApi.supportingData.keySet()) {
			String aFileName = TestHdfsApi.supportingData.get(aUser).get(
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
					logger.info("In traceMethod:PrivilegedExceptionActionImpl, got exception "
							+ e.getMessage()
							+ " for user "
							+ ugi.getUserName()
							+ " for action " + action);
					throw (e);
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

		String keytabUser = TestHdfsApi.supportingData.get(aUser).get(
				KEYTAB_USER);
		logger.info("Set keytab user=" + keytabUser);
		String keytabDir = TestHdfsApi.supportingData.get(aUser)
				.get(KEYTAB_DIR);
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
