package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.monitoring.Monitorable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

import org.apache.log4j.Logger;
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
public class TestDistcpCli extends TestSession {

	static Logger logger = Logger.getLogger(TestDistcpCli.class);
	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

	static HashMap<String, Double> fileMetadata = new HashMap<String, Double>();
	private static boolean isDataCopiedAcrossConcernedClusters = false;
	private String parametrizedCluster;
	private String localCluster;
	private String localHadoopVersion;
	private String remoteHadoopVersion;
	private static Properties crossClusterProperties;
	private static HashMap<String, String> versionStore;
	private Set<String> setOfTestDataFilesInHdfs;
	private Set<String> setOfTestDataFilesInLocalFs;

	private static final String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
	private static final String GRID_0 = "/grid/0/";
	private static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/dfs/";

	HashMap<String, Boolean> pathsChmodedSoFar;

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
		versionStore = new HashMap<String, String>();

	}

	/*
	 * Data Driven DISTCP tests... The tests are invoked with the following
	 * parameters.
	 */
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				// Clusters
//				{ "merry" }, // 2.0
//				{ "betty" }, // 0.23
				{System.getProperty("CLUSTER_NAME")},
				{System.getProperty("REMOTE_CLUSTER")},
		});
	}

	public TestDistcpCli(String cluster) {

		this.parametrizedCluster = cluster;
		this.localCluster = System.getProperty("CLUSTER_NAME");
		fileMetadata.put("file_empty", new Double((double) 0));
		fileMetadata.put("file_1B", new Double((double) 1));
		// 64 MB file size variations
		fileMetadata.put("file_1_byte_short_of_64MB", new Double(
				(double) 64 * 1024 * 1024) - 1);
		fileMetadata.put("file_64MB", new Double((double) 64 * 1024 * 1024));
		fileMetadata.put("file_1_byte_more_than_64MB", new Double(
				(double) 64 * 1024 * 1024) + 1);

		// 128 MB file size variations
		fileMetadata.put("file_1_byte_short_of_128MB", new Double(
				(double) 128 * 1024 * 1024) - 1);
		fileMetadata.put("file_128MB", new Double((double) 128 * 1024 * 1024));
		fileMetadata.put("file_1_byte_more_than_128MB", new Double(
				(double) 128 * 1024 * 1024) + 1);

		fileMetadata.put("file_255MB", new Double((double) 255 * 1024 * 1024));
		fileMetadata.put("file_256MB", new Double((double) 256 * 1024 * 1024));
		fileMetadata.put("file_257MB", new Double((double) 257 * 1024 * 1024));

		fileMetadata.put("file_767MB", new Double((double) 767 * 1024 * 1024));
		fileMetadata.put("file_768MB", new Double((double) 768 * 1024 * 1024));
		fileMetadata.put("file_769MB", new Double((double) 769 * 1024 * 1024));
//		// Huge file
		fileMetadata.put("file_11GB",
				new Double(((double) ((double) (double) 10 * (double) 1024
						* 1024 * 1024) + (double) (700 * 1024 * 1024))));

		setOfTestDataFilesInHdfs = new HashSet<String>();
		setOfTestDataFilesInLocalFs = new HashSet<String>();

		for (String aFileName : fileMetadata.keySet()) {
			// Working set of Files on HDFS
			setOfTestDataFilesInHdfs.add(DATA_DIR_IN_HDFS + aFileName);
			// Working set of Files on Local FS
			setOfTestDataFilesInLocalFs.add(DATA_DIR_IN_LOCAL_FS + aFileName);
		}

		logger.info("Test invoked for local cluster:[" + this.localCluster
				+ "] remote cluster:[" + cluster + "]");
	}

	@Before
	public void getVersions() {
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

	@Before
	public void ensureDataPresenceAcrossClusters() throws Exception {
		pathsChmodedSoFar = new HashMap<String, Boolean>();
		createLocalPreparatoryFiles();
		Set<String> clusters = new HashSet<String>();
		for (Object[] row : TestDistcpCli.data()) {
			for (Object parameter : row) {
				clusters.add(((String) parameter).trim().toLowerCase());
			}
		}
		// For if you are running this test from a 3rd cluster
		clusters.add(this.localCluster);
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		if (isDataCopiedAcrossConcernedClusters == false) {
			for (String aCluster : clusters) {
				for (String justTheFile : fileMetadata.keySet()) {
					if (dfsCommonCliCommands.doesFsEntityAlreadyExistOnDfs(
							aCluster, DATA_DIR_IN_HDFS + justTheFile,
							DfsCliCommands.FILE_SYSTEM_ENTITY_FILE) != 0) {
						dfsCommonCliCommands.createDirectoriesOnHdfs(aCluster,
								DATA_DIR_IN_HDFS);
						doChmodRecursively(aCluster, DATA_DIR_IN_HDFS);
						dfsCommonCliCommands
								.copyLocalFileIntoClusterUsingWebhdfs(aCluster,
										DATA_DIR_IN_LOCAL_FS + justTheFile,
										DATA_DIR_IN_HDFS + justTheFile);
					}
				}
			}
		}
		isDataCopiedAcrossConcernedClusters = true;

	}

	private void doChmodRecursively(String cluster, String dirHierarchy)
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty())
				continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir + "/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			if (!pathsChmodedSoFar.containsKey(pathsChmodedSoFar)) {
				dfsCommonCli.doChmod(cluster, pathSoFar, "777");
				pathsChmodedSoFar.put(pathSoFar, true);
			}
		}
	}

	/*
	 * called by @Before
	 */
	void createLocalPreparatoryFiles() {
		for (String aFileName : fileMetadata.keySet()) {
			logger.info("!!!!!!! Checking local file:" + DATA_DIR_IN_LOCAL_FS
					+ aFileName);
			File attemptedFile = new File(DATA_DIR_IN_LOCAL_FS + aFileName);
			if (attemptedFile.exists()) {
				logger.info(attemptedFile
						+ " already exists, not recreating it");
				continue;
			}
			logger.info("!!!!!!! Creating local file:" + DATA_DIR_IN_LOCAL_FS
					+ aFileName);
			// create a file on the local fs
			if (!attemptedFile.getParentFile().exists()) {
				attemptedFile.getParentFile().mkdirs();
			}
			FileOutputStream fout;
			try {
				fout = new FileOutputStream(attemptedFile);
				int macroStepSize = 1;
				int macroLoopCount = 1;
				int microLoopCount = 0;
				if ((int) (fileMetadata.get(aFileName) / (1024 * 1024 * 1024)) > 0) {
					macroStepSize = 1024 * 1024 * 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					logger.info("Processing: "
							+ aFileName
							+ " size:"
							+ fileMetadata.get(aFileName)
							+ " stepSize: "
							+ macroStepSize
							+ " because: "
							+ (int) (fileMetadata.get(aFileName) / (1024 * 1024 * 1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else if ((int) (fileMetadata.get(aFileName) / (1024 * 1024)) > 0) {
					macroStepSize = 1024 * 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					logger.info("Processing: "
							+ aFileName
							+ " size:"
							+ fileMetadata.get(aFileName)
							+ " stepSize: "
							+ macroStepSize
							+ " because: "
							+ (int) (fileMetadata.get(aFileName) / (1024 * 1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else if ((int) (fileMetadata.get(aFileName) / (1024)) > 0) {
					macroStepSize = 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					logger.info("Processing: " + aFileName + " size:"
							+ fileMetadata.get(aFileName) + " stepSize: "
							+ macroStepSize + " because: "
							+ (int) (fileMetadata.get(aFileName) / (1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else {
					macroLoopCount = 0;
					macroStepSize = 0;
					logger.info("Processing: " + aFileName + " size:"
							+ fileMetadata.get(aFileName) + " stepSize: "
							+ macroStepSize);
					microLoopCount = (int) (fileMetadata.get(aFileName) % (1024));
				}
				for (double i = 0; i < macroLoopCount; i++) {
					fout.write(new byte[(int) macroStepSize]);
				}

				for (int i = 0; i < microLoopCount; i++) {
					fout.write(new byte[1]);
				}

				fout.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

	}

	@Test
	public void testWebhdfsToWebhdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;

		for (String justTheFile : fileMetadata.keySet()) {
			// Push
			String appendStringOnCopiedFile = ".srcWebhdfs."
					+ this.localCluster + ".dstWebhdfs."
					+ this.parametrizedCluster;
			destinationFile = DATA_DIR_IN_HDFS + justTheFile
					+ appendStringOnCopiedFile;
			dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
					this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFile,
					destinationFile, HadooptestConstants.Schema.WEBHDFS, HadooptestConstants.Schema.WEBHDFS);
			
//			dfsCommonCliCommands.distcpWebhdfsToWebhdfs(this.localCluster,
//					this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFile,
//					destinationFile);
			dfsCommonCliCommands.removeFile(this.parametrizedCluster,
					destinationFile);

			// Pull
			appendStringOnCopiedFile = ".srcWebhdfs."
					+ this.parametrizedCluster + ".dstWebhdfs."
					+ this.localCluster;
			destinationFile = DATA_DIR_IN_HDFS + justTheFile
					+ appendStringOnCopiedFile;
			dfsCommonCliCommands.distcpFileUsingProtocol(this.parametrizedCluster,
					this.localCluster, DATA_DIR_IN_HDFS + justTheFile,
					destinationFile, HadooptestConstants.Schema.WEBHDFS, HadooptestConstants.Schema.WEBHDFS);

//			dfsCommonCliCommands.distcpWebhdfsToWebhdfs(
//					this.parametrizedCluster, this.localCluster,
//					DATA_DIR_IN_HDFS + justTheFile, destinationFile);
			dfsCommonCliCommands.removeFile(this.localCluster, destinationFile);
		}
	}

	@Test
	public void testWebhdfsToHdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;
		String appendString;

		for (String justTheFile : fileMetadata.keySet()) {
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {

				// Push
				appendString = ".srcWebhdfs." + this.localCluster + ".dstHdfs."
						+ this.parametrizedCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
				dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
						this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFile,
						destinationFile, HadooptestConstants.Schema.WEBHDFS, HadooptestConstants.Schema.HDFS);
				
//				dfsCommonCliCommands.distcpWebhdfsToHdfs(
//						this.localCluster, this.parametrizedCluster,
//						DATA_DIR_IN_HDFS + justTheFile, destinationFile);
				dfsCommonCliCommands.removeFile(this.parametrizedCluster,
						destinationFile);

			}
			// Pull
			appendString = ".srcWebhdfs." + this.parametrizedCluster
					+ ".dstHdfs." + this.localCluster;
			destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
			dfsCommonCliCommands.distcpFileUsingProtocol(this.parametrizedCluster,
					this.localCluster, DATA_DIR_IN_HDFS + justTheFile,
					destinationFile, HadooptestConstants.Schema.WEBHDFS, HadooptestConstants.Schema.HDFS);

//			dfsCommonCliCommands.distcpWebhdfsToHdfs(
//					this.parametrizedCluster, this.localCluster,
//					DATA_DIR_IN_HDFS + justTheFile, destinationFile);
			
			dfsCommonCliCommands.removeFile(this.localCluster, destinationFile);

		}

	}

	@Test
	public void testHftpToWebhdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;
		String appendString;

		for (String justTheFile : fileMetadata.keySet()) {
			// Push
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {
				appendString = ".srcHftp." + this.localCluster + ".dstWebHdfs."
						+ this.parametrizedCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
				dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
						this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFile,
						destinationFile, HadooptestConstants.Schema.HFTP, HadooptestConstants.Schema.WEBHDFS);

				
//				dfsCommonCliCommands.distcpHftpToWebhdfs(
//						this.localCluster, this.parametrizedCluster,
//						DATA_DIR_IN_HDFS + justTheFile, destinationFile);
				
				dfsCommonCliCommands.removeFile(this.parametrizedCluster,
						destinationFile);

			}
			// No Pull, since HFTP is read-only
		}

	}

	@Test
	public void testHftpToHdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;
		String appendString;

		if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
				.startsWith("0"))
				|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
						.startsWith("2"))) {

			for (String justTheFileName : fileMetadata.keySet()) {
				// Push
				appendString = ".srcHftp." + this.localCluster + ".dstHdfs."
						+ this.parametrizedCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFileName
						+ appendString;
				dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
						this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFileName,
						destinationFile, HadooptestConstants.Schema.HFTP, HadooptestConstants.Schema.HDFS);

//				dfsCommonCliCommands.distcpHftpToHdfs(this.localCluster,
//						this.parametrizedCluster, DATA_DIR_IN_HDFS
//								+ justTheFileName, destinationFile);
				
				dfsCommonCliCommands.removeFile(this.parametrizedCluster,
						destinationFile);

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
	public void testHdfsToWebhdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;
		String appendString;

		for (String justTheFileName : fileMetadata.keySet()) {
			// Push
			appendString = ".srcHdfs." + this.localCluster + ".dstWebhdfs."
					+ this.parametrizedCluster;
			destinationFile = DATA_DIR_IN_HDFS + justTheFileName + appendString;
			dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
					this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFileName,
					destinationFile, HadooptestConstants.Schema.HDFS, HadooptestConstants.Schema.WEBHDFS);

//			dfsCommonCliCommands.distcpHdfsToWebhdfs(this.localCluster,
//					this.parametrizedCluster, DATA_DIR_IN_HDFS
//							+ justTheFileName, destinationFile);
			
			dfsCommonCliCommands.removeFile(this.parametrizedCluster,
					destinationFile);

			// Pull
			if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
					.startsWith("0"))
					|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
							.startsWith("2"))) {
				appendString = ".srcHdfs." + this.parametrizedCluster
						+ ".dstWebhdfs." + this.localCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFileName
						+ appendString;

				dfsCommonCliCommands.distcpFileUsingProtocol(this.parametrizedCluster,
						this.localCluster, DATA_DIR_IN_HDFS + justTheFileName,
						destinationFile, HadooptestConstants.Schema.HDFS, HadooptestConstants.Schema.WEBHDFS);
				
//				dfsCommonCliCommands.distcpHdfsToWebhdfs(
//						this.parametrizedCluster, this.localCluster,
//						DATA_DIR_IN_HDFS + justTheFileName, destinationFile);
				dfsCommonCliCommands.removeFile(this.localCluster,
						destinationFile);

			}
		}

	}

	@Test
	public void testHdfsToHdfs() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String destinationFile;
		String appendString;

		if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
				.startsWith("0"))
				|| (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
						.startsWith("2"))) {

			for (String justTheFileName : fileMetadata.keySet()) {
				// Push
				appendString = ".srcHdfs." + this.localCluster + ".dstHdfs."
						+ this.parametrizedCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFileName
						+ appendString;
				dfsCommonCliCommands.distcpFileUsingProtocol(this.localCluster,
						this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFileName,
						destinationFile, HadooptestConstants.Schema.HDFS, HadooptestConstants.Schema.HDFS);

//				dfsCommonCliCommands.distcpHdfsToHdfs(this.localCluster,
//						this.parametrizedCluster, DATA_DIR_IN_HDFS
//								+ justTheFileName, destinationFile);
				
				dfsCommonCliCommands.removeFile(this.parametrizedCluster,
						destinationFile);

				// Pull
				appendString = ".srcHdfs." + this.parametrizedCluster
						+ ".dstHdfs." + this.localCluster;
				destinationFile = DATA_DIR_IN_HDFS + justTheFileName
						+ appendString;

				dfsCommonCliCommands.distcpFileUsingProtocol(this.parametrizedCluster,
						this.localCluster, DATA_DIR_IN_HDFS + justTheFileName,
						destinationFile, HadooptestConstants.Schema.HDFS, HadooptestConstants.Schema.HDFS);

//				dfsCommonCliCommands.distcpHdfsToHdfs(
//						this.parametrizedCluster, this.localCluster,
//						DATA_DIR_IN_HDFS + justTheFileName, destinationFile);
				dfsCommonCliCommands.removeFile(this.localCluster,
						destinationFile);

			}

		} else {
			logger.info("Skipping test because of possible RPC version mismatch....Local version="
					+ this.localHadoopVersion
					+ " Destination version="
					+ this.remoteHadoopVersion);
		}
	}

}