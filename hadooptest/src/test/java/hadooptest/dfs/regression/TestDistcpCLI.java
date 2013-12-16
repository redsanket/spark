package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;

import junit.framework.Assert;

import org.apache.log4j.Logger;
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
public class TestDistcpCLI extends TestSession {

	static Logger logger = Logger.getLogger(TestDistcpCLI.class);
	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	private Set<String> testDataFiles;
	private static boolean isDataCopiedAcrossConcernedClusters = false;
	private String parametrizedCluster;
	private String localCluster;
	private String localHadoopVersion;
	private String remoteHadoopVersion;
	private static Properties crossClusterProperties;

	@BeforeClass
	public static void startTestSession() throws Exception {

		TestSession.start();
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties
					.load(new FileInputStream(
							HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

	}

	/*
	 * Data Driven DISTCP tests... The tests are invoked with the following
	 * parameters.
	 */
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "wilma" }, { "betty" }, });
	}

	public TestDistcpCLI(String cluster) {

		this.parametrizedCluster = cluster;
		this.localCluster = System.getProperty("CLUSTER_NAME");
		testDataFiles = new HashSet<String>();
//		testDataFiles.add("/HTF/testdata/dfs/big_file_10dot7GB");
		testDataFiles.add("/HTF/testdata/dfs/file_128MB");
		testDataFiles.add("/HTF/testdata/dfs/file_1B");
		testDataFiles.add("/HTF/testdata/dfs/file_255MB");
		testDataFiles.add("/HTF/testdata/dfs/file_256MB");
		testDataFiles.add("/HTF/testdata/dfs/file_257MB");
		testDataFiles.add("/HTF/testdata/dfs/file_64MB");
		testDataFiles.add("/HTF/testdata/dfs/file_767MB");
		testDataFiles.add("/HTF/testdata/dfs/file_768MB");
		testDataFiles.add("/HTF/testdata/dfs/file_769MB");
		testDataFiles.add("/HTF/testdata/dfs/file_empty");

		logger.info("Test invoked for local cluster:[" + this.localCluster
				+ "] remote cluster:[" + cluster + "]");
	}

	@Before
	public void getVersions() {
		ResourceManagerHttpUtils rmUtils = new ResourceManagerHttpUtils();
		localHadoopVersion = rmUtils.getHadoopVersion(this.localCluster);
		localHadoopVersion = localHadoopVersion.split("\\.")[0];
		if (this.localCluster.equals(this.parametrizedCluster)) {
			remoteHadoopVersion = localHadoopVersion;
		} else {
			remoteHadoopVersion = rmUtils
					.getHadoopVersion(this.parametrizedCluster);
			remoteHadoopVersion = remoteHadoopVersion.split("\\.")[0];
		}

	}

	@Before
	public void ensureDataPresenceAcrossClusters() throws Exception {
		Set<String> clusters = new HashSet<String>();
		for (Object[] row : TestDistcpCLI.data()) {
			for (Object parameter : row) {
				clusters.add(((String) parameter).trim().toLowerCase());
			}
		}
		// For if you are running this test from a 3rd cluster
		clusters.add(this.localCluster);

		if (isDataCopiedAcrossConcernedClusters == false) {
			for (String aCluster : clusters) {
				for (String completePathToFile : testDataFiles) {
					if (!doesFileExistInDFS(aCluster, completePathToFile)) {
						createDirectoriesToFile(aCluster, completePathToFile);
						copyLocalFileIntoClusterUsingWebhdfs(aCluster,
								completePathToFile);
					}
				}
			}
		}
		isDataCopiedAcrossConcernedClusters = true;

	}

	void printOutResponse(Process process) throws InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line;
		try {
			line = reader.readLine();
			while (line != null) {
				TestSession.logger.debug(line);
				line = reader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();
	}

	boolean doesFileExistInDFS(String cluster, String completePathToFile)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("fs");
		sb.append(" ");
		sb.append("-ls");
		sb.append(" ");
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePathToFile);
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	/*
	 * We use web-hdfs to move files across clusters, because there could be a
	 * version incompatibility. Webhdfs is agnostic to that, unlike Hdfs.
	 */
	void copyLocalFileIntoClusterUsingWebhdfs(String cluster,
			String completePathToFile) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("fs");
		sb.append(" ");
		sb.append("-copyFromLocal");
		sb.append(" ");
		sb.append("/grid/0" + completePathToFile);
		sb.append(" ");
		String nameNodeWithWebddfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebddfsSchema);
		sb.append(completePathToFile);
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}

	/*
	 * 'dfsload' is our superuser who can create directories in DFS. For some
	 * reason it did not work with 'hadoopqa'.
	 */
	void createDirectoriesToFile(String cluster, String completePathToFile)
			throws Exception {

		int lastIndexOfSlash = completePathToFile.lastIndexOf('/');
		String baseDirs = (lastIndexOfSlash == -1) ? completePathToFile
				: completePathToFile.substring(0, lastIndexOfSlash);

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("fs");
		sb.append(" ");
		sb.append("-mkdir -p");
		sb.append(" ");
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(baseDirs);
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.DFSLOAD,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		String parentDirs = "/";
		for (String aDirFrag : baseDirs.split("/")) {
			parentDirs += aDirFrag + "/";
			doChmod(cluster, parentDirs);
		}

	}

	void doChmod(String aCluster, String completePath) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("fs");
		sb.append(" ");
		sb.append("-chmod 777");
		sb.append(" ");
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(aCluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePath);
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.DFSLOAD,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case webhdfs://
	 */
	private String getNNUrlForWebhdfs(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.toLowerCase() + "." + HadooptestConstants.NodeTypes.NAMENODE);

		String nameNodeWithNoPortForWebhdfs = nnReadFromPropFile.replace(
				":50070", "");
		String nameNodeWithNoPortAndSchemaForWebhdfs = nameNodeWithNoPortForWebhdfs
				.replace(HadooptestConstants.Schema.HTTP,
						HadooptestConstants.Schema.WEBHDFS);
		return nameNodeWithNoPortAndSchemaForWebhdfs;

	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hdfs://
	 */
	private String getNNUrlForHdfs(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.trim().toLowerCase()
				+ "."
				+ HadooptestConstants.NodeTypes.NAMENODE);

		String nameNodeWithPortForHdfs = nnReadFromPropFile.replace("50070",
				"8020");
		String nameNodeWithPortAndSchemaForHdfs = nameNodeWithPortForHdfs
				.replace(HadooptestConstants.Schema.HTTP,
						HadooptestConstants.Schema.HDFS);
		return nameNodeWithPortAndSchemaForHdfs;

	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hftp://
	 */
	private String getNNUrlForHftp(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.trim().toLowerCase()
				+ "."
				+ HadooptestConstants.NodeTypes.NAMENODE);
		String nameNodeWithPortAndSchemaForHftp = nnReadFromPropFile.replace(
				HadooptestConstants.Schema.HTTP,
				HadooptestConstants.Schema.HFTP);
		return nameNodeWithPortAndSchemaForHftp;

	}

	/*
	 * Remove the copied over file, at the end of a successful test.
	 */
	void commandRemoveFile(String schemaDecoratedPathToFile) throws Exception {

		logger.info("Initiating removal of " + schemaDecoratedPathToFile);
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("fs -rm");
		sb.append(" ");
		sb.append(schemaDecoratedPathToFile);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}

	/*
	 * Initiate command for a webhdfs://(source) --> webhdfs://(dest) distcp
	 * call
	 */
	void commandDistcpWebhdfsToWebhdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcWebhdfs" + srcCluster + "dstWebhdfs"
				+ dstCluster + ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForWebhdfs(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		// On success, remove the copied file
		commandRemoveFile(schemaDecoratedDestination);
	}

	void commandDistcpWebhdfsToHdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcWebhdfs" + srcCluster + "dstHdfs"
				+ dstCluster + ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForWebhdfs(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		commandRemoveFile(schemaDecoratedDestination);

	}

	void commandDistcpHftpToWebhdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcHftp" + srcCluster + "dstWebhdfs"
				+ dstCluster + ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForHftp(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		// On success, remove the copied file
		commandRemoveFile(schemaDecoratedDestination);

	}

	void commandDistcpHftpToHdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcHftp" + srcCluster + "dstHdfs" + dstCluster
				+ ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForHftp(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		// On success, remove the copied file
		commandRemoveFile(schemaDecoratedDestination);

	}

	void commandDistcpHdfsToWebhdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcHdfs" + srcCluster + "dstWebhdfs"
				+ dstCluster + ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForHdfs(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		// On success, remove the copied file
		commandRemoveFile(schemaDecoratedDestination);

	}

	void commandDistcpHdfsToHdfs(String srcCluster, String dstCluster,
			String completePathToFile) throws Exception {
		String appendString = ".srcHdfs" + srcCluster + "dstHdfs" + dstCluster
				+ ".txt";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Miscellaneous.HADOOP);
		sb.append(" ");
		sb.append("distcp");
		sb.append(" ");
		String nnForWebhdfsSrc = getNNUrlForHdfs(srcCluster);
		sb.append(nnForWebhdfsSrc + completePathToFile);
		sb.append(" ");
		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
				+ completePathToFile + appendString;
		sb.append(schemaDecoratedDestination);
		String commandString = sb.toString();
		String[] commandFrags = commandString.split("\\s+");

		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				envToUnsetHadoopPrefix);
		printOutResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
		// On success, remove the copied file
		commandRemoveFile(schemaDecoratedDestination);

	}

	@Test
	public void testDistcp_WebhdfsToWebhdfs() throws Exception {
		for (String aFile : testDataFiles) {
			// Push
			commandDistcpWebhdfsToWebhdfs(this.localCluster,
					this.parametrizedCluster, aFile);
			// Pull
			commandDistcpWebhdfsToWebhdfs(this.parametrizedCluster,
					this.localCluster, aFile);
		}
	}

	@Test
	public void testDistcp_WebhdfsToHdfs() throws Exception {
		for (String aFile : testDataFiles) {
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {

				// Push
				commandDistcpWebhdfsToHdfs(this.localCluster,
						this.parametrizedCluster, aFile);
			}
			// Pull
			commandDistcpWebhdfsToHdfs(this.parametrizedCluster,
					this.localCluster, aFile);
		}

	}

	@Test
	public void testDistcp_HftpToWebhdfs() throws Exception {
		for (String aFile : testDataFiles) {
			// Push
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {

				commandDistcpHftpToWebhdfs(this.localCluster,
						this.parametrizedCluster, aFile);
			}
			// No Pull, since HFTP is read-only
		}

	}

	@Test
	public void testDistcp_HftpToHdfs() throws Exception {
		if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
				.startsWith("0"))
				|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
						.startsWith("2"))) {

			for (String aFile : testDataFiles) {
				// Push
				commandDistcpHftpToHdfs(this.localCluster,
						this.parametrizedCluster, aFile);
				// No Pull, since HFTP is readonly
			}
		} else {
			logger.info("Skipping test because of possible RPC version mismatch....Local version="
					+ this.localHadoopVersion
					+ " Destination version="
					+ this.remoteHadoopVersion);
		}

	}

	@Test
	public void testDistcp_HdfsToWebhdfs() throws Exception {
		for (String aFile : testDataFiles) {
			// Push
			commandDistcpHdfsToWebhdfs(this.localCluster,
					this.parametrizedCluster, aFile);
			// Pull
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {
				commandDistcpHdfsToWebhdfs(this.parametrizedCluster,
						this.localCluster, aFile);
			}
		}

	}

	@Test
	public void testDistcp_HdfsToHdfs() throws Exception {
		if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
				.startsWith("0"))
				|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
						.startsWith("2"))) {

			for (String aFile : testDataFiles) {
				// Push
				commandDistcpHdfsToHdfs(this.localCluster,
						this.parametrizedCluster, aFile);
				// Pull
				commandDistcpHdfsToHdfs(this.parametrizedCluster,
						this.localCluster, aFile);
			}

		} else {
			logger.info("Skipping test because of possible RPC version mismatch....Local version="
					+ this.localHadoopVersion
					+ " Destination version="
					+ this.remoteHadoopVersion);
		}
	}

}