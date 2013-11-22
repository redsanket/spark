package hadooptest.dfs.regression;

import hadooptest.TestSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

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
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import coretest.SerialTests;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestHdfsAPI extends TestSession {

	static String HDFS = "hdfs";
	static String HDFS_PORT = "8020";
	static String WEBHDFS = "webhdfs";
	static String HADOOPQA = "hadoopqa";
	static String DFSLOAD = "dfsload";
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

	static Logger logger = Logger.getLogger(TestHdfsAPI.class);
	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	private String scheme;
	private String nodeName;

	public TestHdfsAPI(String kluster, String scheme, String nodeName,
			String release) {
		this.scheme = scheme;
		this.nodeName = nodeName;
		logger.info("Invoked test for:[" + kluster + "] Scheme:[" + scheme
				+ "] NodeName:[" + nodeName + "]");
	}

	/*
	 * Data Driven HDFS tests... The tests are invoked with the following
	 * parameters.
	 */
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays
				.asList(new Object[][] {
						{ "eomer", WEBHDFS, "gsbl90629.blue.ygrid.yahoo.com",
								"0.23", },
						{ "eomer", HDFS, "gsbl90629.blue.ygrid.yahoo.com",
								"0.23", },
						{ "elrond", WEBHDFS, "gsbl90566.blue.ygrid.yahoo.com",
								"2.2.1.1", },
				// { "elrond", HDFS, "gsbl90566.blue.ygrid.yahoo.com",
				// "2.2.1.1", }, //This one blows up with
				// org.apache.hadoop.ipc.RPC$VersionMismatch
				});
	}

	/*
	 * TODO:350MB is a good size, forces use of multiple blocks. Future
	 * enhancement suggestion, use file sizes of 0, 1, DEFAULT_BLOCKSIZE-1,
	 * DEFAULT_BLOCKSIZE, DEFAULT_BLOCKSIZE+1, DEFAULT_BLOCKSIZEx10 bytes to
	 * alter the filesizes used in testing, and cycle across.
	 */
	@BeforeClass
	public static void testxSessionStart() throws Exception {
		TestHdfsAPI.supportingData.put(HADOOPQA, new HashMap<String, String>() {
			private static final long serialVersionUID = 1L;

			{
				put(KEYTAB_DIR, "/homes/hadoopqa/hadoopqa.dev.headless.keytab");
				put(KEYTAB_USER, HADOOPQA);
				put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/" + HADOOPQA + "Dir/"
						+ HADOOPQA + "File");
				put(USER_WHO_DOESNT_HAVE_PERMISSIONS, DFSLOAD);
			}
		});
		TestHdfsAPI.supportingData.put(DFSLOAD, new HashMap<String, String>() {
			private static final long serialVersionUID = 1L;

			{
				put(KEYTAB_DIR, "/homes/dfsload/dfsload.dev.headless.keytab");
				put(KEYTAB_USER, DFSLOAD + "@DEV.YGRID.YAHOO.COM");
				put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/" + DFSLOAD + "Dir/"
						+ DFSLOAD + "File");
				put(USER_WHO_DOESNT_HAVE_PERMISSIONS, HADOOPQA);
			}
		});

		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Create a local file
			createLocalFile(aFileName);
		}
		TestSession.start();
	}

	@Before
	public void copyFilesOntoHadoopFS() {
		try {
			// testSessionStart();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);

			Configuration aConf = getConfForRemoteFS(aUser);
			UserGroupInformation ugi = getUgiForUser(aUser);

			try {
				DoAs doAs;
				// First copy the file to the remote DFS'es
				doAs = new DoAs(ugi, ACTION_COPY_FROM_LOCAL, aConf, aFileName,
						aFileName);
				doAs.doAction();

			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Test(expected = AccessControlException.class)
	public void checkPermissions() throws AccessControlException {
		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
					OWNED_FILE_WITH_COMPLETE_PATH);
			// Get the config and UGI to make the call as the right user
			Configuration aConf = getConfForRemoteFS(aUser);

			DoAs doAs;
			try {
				// Check Permissions
				String userWithouPermission = TestHdfsAPI.supportingData.get(
						aUser).get(USER_WHO_DOESNT_HAVE_PERMISSIONS);
				UserGroupInformation ugiNoPermission = getUgiForUser(userWithouPermission);

				doAs = new DoAs(ugiNoPermission, ACTION_APPEND_TO_FILE, aConf,
						aFileName, null);
				doAs.doAction();

			} catch (AccessControlException acx) {
				throw acx;
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Test
	public void appendToFile() throws Exception {
		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
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

	@Test
	public void testdoAMovesInAndOutOfClusterAndChecksum() {
		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
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
		for (String aUser : TestHdfsAPI.supportingData.keySet()) {
			String aFileName = TestHdfsAPI.supportingData.get(aUser).get(
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
			logger.info("Doing action[" + action + "]");
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
					logger.info("Default Block Size:"
							+ aRemoteFS.getDefaultBlockSize());
					logger.info("Default Block Size Path:"
							+ aRemoteFS.getDefaultBlockSize(new Path(oneFile)));
					logger.info("Default Replication:"
							+ aRemoteFS.getDefaultReplication());
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
		Configuration conf = new Configuration();
		if (this.scheme.equals(HDFS)) {
			conf.set("fs.defaultFS", this.scheme + "://" + this.nodeName + ":"
					+ HDFS_PORT);
		} else {
			// WEBHDFS
			conf.set("fs.defaultFS", this.scheme + "://" + this.nodeName);
		}
		if (aUser.equals(HADOOPQA)) {
			conf.set("hadoop.job.ugi", HADOOPQA);
		} else {
			conf.set("hadoop.job.ugi", DFSLOAD);
		}
		conf.set("hadoop.security.authentication", "true");
		return conf;
	}

	UserGroupInformation getUgiForUser(String aUser) {

		String keytabUser = TestHdfsAPI.supportingData.get(aUser).get(
				KEYTAB_USER);
		String keytabDir = TestHdfsAPI.supportingData.get(aUser)
				.get(KEYTAB_DIR);
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
					keytabUser, keytabDir);

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