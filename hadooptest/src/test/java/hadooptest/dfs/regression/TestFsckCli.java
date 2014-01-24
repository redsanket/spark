package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.dfs.regression.FsckResponseBO.FsckBlockDetailsBO;
import hadooptest.dfs.regression.FsckResponseBO.FsckFileDetailsBO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.hadoop.io.Text;

import com.jcraft.jsch.*;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class TestFsckCli extends TestSession {

	static Logger logger = Logger.getLogger(TestFsckCli.class);

	public static final String BYTES_PER_MAP = "mapreduce.randomwriter.bytespermap";
	private static final String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
	private static final String GRID_0 = "/grid/0/";
	private static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/dfs/";
	private static final String FSCK_TESTS_DIR_ON_HDFS = DATA_DIR_IN_HDFS
			+ "fsck_tests/";
	private static final String RANDOM_WRITER_DATA_DIR = FSCK_TESTS_DIR_ON_HDFS
			+ "randomWriter/";
	private static final String SORT_JOB_DATA_DIR = FSCK_TESTS_DIR_ON_HDFS
			+ "sortJob/";

	private static final String FSCK_BAD_DATA_TESTS_DIR_ON_HDFS = FSCK_TESTS_DIR_ON_HDFS
			+ "fsck_bad_data_tests";
	private static final String THREE_GB_DIR_ON_HDFS = FSCK_TESTS_DIR_ON_HDFS
			+ "3gb/";
	private static final String THREE_GB_FILE_NAME = "3GbFile.txt";
	private static final String ONE_BYTE_FILE = "file_1B";

	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";

	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
	static HashMap<String, Double> fileMetadata = new HashMap<String, Double>();
	HashMap<String, Boolean> pathsChmodedSoFar;
	static List<String> dataNodes;

	private Set<String> setOfTestDataFilesInHdfs;
	private Set<String> setOfTestDataFilesInLocalFs;

	private String localCluster;
	private String localHadoopVersion;
	private static Properties crossClusterProperties;
	private static HashMap<String, String> versionStore;

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
		dataNodes = new ArrayList<String>(Arrays.asList(TestSession.cluster
				.getNodeNames(HadooptestConstants.NodeTypes.DATANODE)));

	}

	public TestFsckCli() {

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
		// Huge file
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

	/*
	 * called by @Before
	 */
	public void getVersions() {
		logger.info("running getVersions");
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

	}

	/*
	 * called by @Before
	 */
	public void ensureDataPresenceinCluster() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		logger.info("running ensureDataPresenceAcrossClusters");
		String aCluster = System.getProperty("CLUSTER_NAME");

		for (String justTheFileName : fileMetadata.keySet()) {
			if (dfsCommonCli
					.doesFsEntityAlreadyExistOnDfs(aCluster, DATA_DIR_IN_HDFS
							+ justTheFileName, DfsCliCommands.FILE_SYSTEM_ENTITY_FILE) != 0) {
				dfsCommonCli
						.createDirectoriesOnHdfs(aCluster, DATA_DIR_IN_HDFS);
				doChmodRecursively(DATA_DIR_IN_HDFS);
				dfsCommonCli.copyLocalFileIntoClusterUsingWebhdfs(aCluster,
						DATA_DIR_IN_LOCAL_FS + justTheFileName,
						DATA_DIR_IN_HDFS + justTheFileName);
				
			}
		}

	}

	/*
	 * called by @Before
	 */
	public void runStdHadoopRandomWriter(String randomWriterOutputDirOnHdfs)
			throws Exception {
		logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		conf.setLong(BYTES_PER_MAP, 256000);
		int res = ToolRunner.run(conf, new RandomWriter(),
				new String[] { randomWriterOutputDirOnHdfs });
		Assert.assertEquals(0, res);

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res = ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {
				sortInputDataLocation, sortOutputDataLocation });
		Assert.assertEquals(0, res);

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

	/*
	 * Creates a temporary directory before each test run to create a 3gb file.
	 */
	@Rule
	public TemporaryFolder createLocal3GbFileInThisJUnitTempFolder = new TemporaryFolder(
			new File(DATA_DIR_IN_LOCAL_FS));

	@Before
	public void beforeEachTest() throws Throwable {
		pathsChmodedSoFar = new HashMap<String, Boolean>();
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		createLocalPreparatoryFiles();
		// getVersions();
		ensureDataPresenceinCluster();

		dfsCommonCli.createDirectoriesOnHdfs(this.localCluster,
				FSCK_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_TESTS_DIR_ON_HDFS);

		dfsCommonCli.createDirectoriesOnHdfs(this.localCluster,
				FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);

		dfsCommonCli.createDirectoriesOnHdfs(this.localCluster,
				THREE_GB_DIR_ON_HDFS);
		doChmodRecursively(THREE_GB_DIR_ON_HDFS);

		// runStdHadoopRandomWriter(RANDOM_WRITER_DATA_DIR);
		 Assert.assertEquals(true,
		 create3GbFile(createLocal3GbFileInThisJUnitTempFolder));
		 
//		dfsCommonCli.copyLocalFileIntoClusterUsingWebhdfs(this.localCluster,
//				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, DATA_DIR_IN_HDFS
//						+ ONE_BYTE_FILE);
		dfsCommonCli.copyLocalFileIntoClusterUsingWebhdfs(this.localCluster,
		 createLocal3GbFileInThisJUnitTempFolder.getRoot() + "/"
		 + THREE_GB_FILE_NAME, THREE_GB_DIR_ON_HDFS
		 + THREE_GB_FILE_NAME);

	}

	private void doChmodRecursively(String dirHierarchy) throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty()) continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir +"/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			if (!pathsChmodedSoFar.containsKey(pathsChmodedSoFar)) {
				dfsCommonCli.doChmod(this.localCluster, pathSoFar, "777");
				pathsChmodedSoFar.put(pathSoFar, true);
			}
		}
	}

	/*
	 * Cleanup called after each test run
	 */
	@After
	public void afterEachTest() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		dfsCommonCli.deleteDirectoriesFromThisPointOnwardsOnHdfs(
				this.localCluster, FSCK_TESTS_DIR_ON_HDFS);
		dfsCommonCli.deleteDirectoriesFromThisPointOnwardsOnHdfs(
				this.localCluster, DATA_DIR_IN_HDFS + ONE_BYTE_FILE);

		// The 3gb file on the local file system is automatically deleted
		// after every test run, by JUnit
	};

	public boolean create3GbFile(TemporaryFolder tempFolder) throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		tempFolder.newFile(THREE_GB_FILE_NAME);
		StringBuilder sb = new StringBuilder();
		sb.append("/bin/dd");
		sb.append(" ");
		sb.append("if=/dev/zero");
		sb.append(" ");
		sb.append("of=" + tempFolder.getRoot() + "/" + THREE_GB_FILE_NAME);
		sb.append(" ");
		sb.append("bs=10240");
		sb.append(" ");
		sb.append("count=300000");
		sb.append(" ");
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");

		Process process = null;
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();

		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		dfsCommonCli.printAndScanResponse(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	// @Test
	// test_fsck_02
	public void testFsckResultsLeveragingRandomWriterAndSortJobs()
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testFsckResultsLeveragingRandomWriterAndSortJobs");
		String randomWriterOutputDir = "testFsckResultsLeveragingRandomWriterAndSortJobs";
		String sortJobOutputDir = "testFsckResultsLeveragingRandomWriterAndSortJobs";
		logger.info("Hello this is testFsckResultsLeveragingRandomWriterAndSortJobs");
		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS, true, true, true);
		// for (FsckResponseBO.FsckFileDetailsBO fsckFileDetails :
		// fsckResponseBO.fileAndBlockDetails
		// .keySet()) {
		// TestSession.logger.info(fsckFileDetails);
		// }
		Assert.assertNotNull(fsckResponse);

		double totalFileSizeBeforeRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.totalSizeInBytes;
		int numberOfDatanodesBeforeRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.numberOfDatanodes;
		int numberOfRacksBeforeRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.numberOfRacks;

		TestSession.logger
				.info("Number of datanodes before running RandomWriterAndSortJobs: "
						+ numberOfDatanodesBeforeRunningRandomWriterAndSortJobs);
		TestSession.logger
				.info("Number of racks before running RandomWriterAndSortJobs: "
						+ numberOfRacksBeforeRunningRandomWriterAndSortJobs);

		runStdHadoopRandomWriter(RANDOM_WRITER_DATA_DIR + randomWriterOutputDir);
		runStdHadoopSortJob(RANDOM_WRITER_DATA_DIR + randomWriterOutputDir,
				SORT_JOB_DATA_DIR + sortJobOutputDir);

		fsckResponse = dfsCommonCli.executeFsckCommand(null, DATA_DIR_IN_HDFS,
				true, true, true);
		double totalFileSizeAfterRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.totalSizeInBytes;
		int numberOfDatanodesAfterRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.numberOfDatanodes;
		int numberOfRacksAfterRunningRandomWriterAndSortJobs = fsckResponse.fsckSummaryBO.numberOfRacks;
		Assert.assertTrue(totalFileSizeAfterRunningRandomWriterAndSortJobs > totalFileSizeBeforeRunningRandomWriterAndSortJobs);
		TestSession.logger
				.info("Number of datanodes after running RandomWriterAndSortJobs: "
						+ numberOfDatanodesAfterRunningRandomWriterAndSortJobs);
		TestSession.logger
				.info("Number of racks after running RandomWriterAndSortJobs: "
						+ numberOfRacksAfterRunningRandomWriterAndSortJobs);

	}

	// @Test
	// test_fsck_additional_01
	public void testFsckWithSafemodeOn() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testFsckWithSafemodeOn");
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("get"), true);
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("get"), true);
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("enter"), true);
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("get"), true);
		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS, true, true, true);
		Assert.assertNotNull(fsckResponse);
		Assert.assertEquals(fsckResponse.fsckSummaryBO.status, "HEALTHY");
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("leave"), true);
		Assert.assertEquals(dfsCommonCli.executeSafemodeCommand("get"), true);

	}

	// @Test
	// test_fsck_additional_02
	public void testFsckWithBalancer() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger.info("________Beginning test testFsckWithBalancer");
		Assert.assertEquals(dfsCommonCli.executeBalancerCommand(), true);
		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS, true, true, true);
		Assert.assertNotNull(fsckResponse);
		Assert.assertEquals(fsckResponse.fsckSummaryBO.status, "HEALTHY");

	}

	@Test
	public void testManuallyCorruptHdfsBlocksByModifyingContents()
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testManuallyCorruptHdfsBlocksByModifyingContents");
		String command;
		HashMap<String, String> actualBlockPoolLocationsBeforeCorrupting = new HashMap<String, String>();
		HashMap<String, String> actualBlockPoolLocationsAfterCorrupting = new HashMap<String, String>();
		logger.info("Hello this is testManuallyCorruptHdfsBlocks");
		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS + ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);

		ArrayList<FsckBlockDetailsBO> fsckBlockDetailsBOList = null;
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		String blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name:" + blockPoolName);
		String blockName = fsckBlockDetailsBOList.get(0).blockName;
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name:" + blockName);
		ArrayList<String> previousDatanodes = null;
		ArrayList<String> reallocatedDatanodes = null;
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			previousDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : previousDatanodes) {
			TestSession.logger.info("a datanode:" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					actualBlockPoolLocationsBeforeCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);
				}

			}

		}
		ArrayList<Thread> bounceThreads = new ArrayList<Thread>();

		// Corrupt the physical block locations
		boolean oneBlockMessed = false;
		for (final String corruptOnThisDatanodeIP : actualBlockPoolLocationsBeforeCorrupting
				.keySet()) {
			if (!oneBlockMessed) {
				oneBlockMessed = true;

				/*
				 * Note from Kihwal: A block corruption can be only be detected
				 * when the data is read by either the block scanner or a
				 * client. It is kind of similar to disk sector errors. Until
				 * you try to read them you won't know. Most disk controllers
				 * can be configured to scan the sectors, which is sort of
				 * equivalent to our block scanner. The config variable you
				 * mentioned 'dfs.datanode.scan.period.hours' is the right one
				 * for controlling the scan interval.
				 */
				command = " echo \"xx\" > "
						+ actualBlockPoolLocationsBeforeCorrupting
								.get(corruptOnThisDatanodeIP);
				doJavaSSHClientExec(corruptOnThisDatanodeIP, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				Thread aThreadThatWillBounceDataNode = new Thread() {
					@Override
					public void run() {
						try {
							bounceDatanodeAndWaitForTwoMinutes(getHostNameFromIp(corruptOnThisDatanodeIP));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				bounceThreads.add(aThreadThatWillBounceDataNode);
				aThreadThatWillBounceDataNode.start();
			}

		}
		for (Thread t : bounceThreads) {
			t.join();
		}

		// Run FSCK again
		fsckResponse = dfsCommonCli.executeFsckCommand(null, DATA_DIR_IN_HDFS
				+ ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name (2nd time):" + blockPoolName);
		blockName = fsckBlockDetailsBOList.get(0).blockName;
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name(2nd time):" + blockName);
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			reallocatedDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : reallocatedDatanodes) {
			TestSession.logger.info("a datanode(2nd time):" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					TestSession.logger.info("Adding " + aDatanode
							+ " Block Location:"
							+ blockLocationReadOffOfDatanode);
					actualBlockPoolLocationsAfterCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);

				}

			}

		}

		boolean oneBlockRelocated = false;
		if (!reallocatedDatanodes.containsAll(previousDatanodes)) {
			oneBlockRelocated = true;
		}

		if (!oneBlockRelocated) {
			// Blocks were moved within the same nodes
			for (String aPreviousBlock : actualBlockPoolLocationsBeforeCorrupting
					.keySet()) {
				if (!actualBlockPoolLocationsAfterCorrupting
						.containsKey(aPreviousBlock)) {
					oneBlockRelocated = true;
					break;
				}

			}
		}
		Assert.assertTrue(oneBlockRelocated);

	}

	@Test
	public void testManuallyCorruptHdfsBlocksByDeletingPhysicalBlocks()
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testManuallyCorruptHdfsBlocksByDeletingPhysicalBlocks");
		String command;
		HashMap<String, String> actualBlockPoolLocationsBeforeCorrupting = new HashMap<String, String>();
		HashMap<String, String> actualBlockPoolLocationsAfterCorrupting = new HashMap<String, String>();
		String fileContentsBeforeCorrupting = null;
		String fileContentsAfterCorrupting = null;

		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS + ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);

		ArrayList<FsckBlockDetailsBO> fsckBlockDetailsBOList = null;
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		String blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name:" + blockPoolName);
		String blockName = fsckBlockDetailsBOList.get(0).blockName;
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name:" + blockName);
		ArrayList<String> previousDatanodes = null;
		ArrayList<String> reallocatedDatanodes = null;
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			previousDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : previousDatanodes) {
			TestSession.logger.info("a datanode:" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					actualBlockPoolLocationsBeforeCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);
					command = "cat  "
							+ actualBlockPoolLocationsBeforeCorrupting
									.get(aDatanode);
					fileContentsBeforeCorrupting = doJavaSSHClientExec(
							aDatanode, command,
							HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				}

			}

		}
		ArrayList<Thread> bounceThreads = new ArrayList<Thread>();

		// Corrupt the physical block locations
		boolean oneBlockMessed = false;
		for (final String corruptOnThisDatanodeIP : actualBlockPoolLocationsBeforeCorrupting
				.keySet()) {
			if (!oneBlockMessed) {
				oneBlockMessed = true;
				/*
				 * Note from Kihwal: A block corruption can be only be detected
				 * when the data is read by either the block scanner or a
				 * client. It is kind of similar to disk sector errors. Until
				 * you try to read them you won't know. Most disk controllers
				 * can be configured to scan the sectors, which is sort of
				 * equivalent to our block scanner. The config variable you
				 * mentioned 'dfs.datanode.scan.period.hours' is the right one
				 * for controlling the scan interval.
				 */

				command = "rm  "
						+ actualBlockPoolLocationsBeforeCorrupting
								.get(corruptOnThisDatanodeIP);
				doJavaSSHClientExec(corruptOnThisDatanodeIP, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				Thread aThreadThatWillBounceDataNode = new Thread() {
					@Override
					public void run() {
						try {
							bounceDatanodeAndWaitForTwoMinutes(getHostNameFromIp(corruptOnThisDatanodeIP));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				bounceThreads.add(aThreadThatWillBounceDataNode);
				aThreadThatWillBounceDataNode.start();
			}

		}
		for (Thread t : bounceThreads) {
			t.join();
		}

		// Run FSCK again
		fsckResponse = dfsCommonCli.executeFsckCommand(null, DATA_DIR_IN_HDFS
				+ ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name (2nd time):" + blockPoolName);
		blockName = fsckBlockDetailsBOList.get(0).blockName;
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name(2nd time):" + blockName);
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			reallocatedDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : reallocatedDatanodes) {
			TestSession.logger.info("a datanode(2nd time):" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					TestSession.logger.info("Adding " + aDatanode
							+ " Block Location:"
							+ blockLocationReadOffOfDatanode);
					actualBlockPoolLocationsAfterCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);

				}

			}

		}

		for (final String aDatanode : actualBlockPoolLocationsAfterCorrupting
				.keySet()) {
			command = "cat  "
					+ actualBlockPoolLocationsAfterCorrupting.get(aDatanode);
			fileContentsAfterCorrupting = doJavaSSHClientExec(aDatanode,
					command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
			Assert.assertTrue(fileContentsAfterCorrupting
					.equals(fileContentsBeforeCorrupting));
		}

	}

	@Test
	public void testManuallyCorruptHdfsBlockByDeletingMetaFile()
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testManuallyCorruptHdfsBlockByDeletingMetaFile__________");
		String command;
		HashMap<String, String> actualBlockPoolLocationsBeforeCorrupting = new HashMap<String, String>();
		HashMap<String, String> actualBlockPoolLocationsAfterCorrupting = new HashMap<String, String>();
		String fileContentsBeforeCorrupting = null;
		String fileContentsAfterCorrupting = null;
		logger.info("Hello this is testManuallyCorruptHdfsBlocks");
		FsckResponseBO fsckResponse = dfsCommonCli.executeFsckCommand(null,
				DATA_DIR_IN_HDFS + ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);

		ArrayList<FsckBlockDetailsBO> fsckBlockDetailsBOList = null;
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		String blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name:" + blockPoolName);
		String blockName = fsckBlockDetailsBOList.get(0).blockName;
		String blockNamesuffix = blockName.substring(
				blockName.lastIndexOf('_') + 1, blockName.length());
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name:" + blockName);
		ArrayList<String> previousDatanodes = null;
		ArrayList<String> reallocatedDatanodes = null;
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			previousDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : previousDatanodes) {
			TestSession.logger.info("a datanode:" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					actualBlockPoolLocationsBeforeCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);
					command = "cat  "
							+ actualBlockPoolLocationsBeforeCorrupting
									.get(aDatanode);
					fileContentsBeforeCorrupting = doJavaSSHClientExec(
							aDatanode, command,
							HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				}

			}

		}
		ArrayList<Thread> bounceThreads = new ArrayList<Thread>();

		// Corrupt the physical block locations
		boolean oneBlockMessed = false;
		for (final String corruptOnThisDatanodeIP : actualBlockPoolLocationsBeforeCorrupting
				.keySet()) {
			if (!oneBlockMessed) {
				oneBlockMessed = true;
				/*
				 * Note from Kihwal: A block corruption can be only be detected
				 * when the data is read by either the block scanner or a
				 * client. It is kind of similar to disk sector errors. Until
				 * you try to read them you won't know. Most disk controllers
				 * can be configured to scan the sectors, which is sort of
				 * equivalent to our block scanner. The config variable you
				 * mentioned 'dfs.datanode.scan.period.hours' is the right one
				 * for controlling the scan interval.
				 */

				command = "rm  "
						+ actualBlockPoolLocationsBeforeCorrupting
								.get(corruptOnThisDatanodeIP) + "_"
						+ blockNamesuffix;
				doJavaSSHClientExec(corruptOnThisDatanodeIP, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				Thread aThreadThatWillBounceDataNode = new Thread() {
					@Override
					public void run() {
						try {
							bounceDatanodeAndWaitForTwoMinutes(getHostNameFromIp(corruptOnThisDatanodeIP));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				bounceThreads.add(aThreadThatWillBounceDataNode);
				aThreadThatWillBounceDataNode.start();
			}

		}
		for (Thread t : bounceThreads) {
			t.join();
		}

		// Run FSCK again
		fsckResponse = dfsCommonCli.executeFsckCommand(null, DATA_DIR_IN_HDFS
				+ ONE_BYTE_FILE, true, true, true);
		Assert.assertNotNull(fsckResponse);
		for (FsckFileDetailsBO fsckFileDetails : fsckResponse.fileAndBlockDetails
				.keySet()) {
			TestSession.logger.info("Processing file:"
					+ fsckFileDetails.fileName);
			if (fsckFileDetails.fileName.equals(DATA_DIR_IN_HDFS
					+ ONE_BYTE_FILE)) {
				fsckBlockDetailsBOList = fsckResponse.fileAndBlockDetails
						.get(fsckFileDetails);
				TestSession.logger.info("BlockDetailsArray size:"
						+ fsckBlockDetailsBOList.size());
				break;
			}
		}
		blockPoolName = fsckBlockDetailsBOList.get(0).blockPool;
		TestSession.logger.info("Block pool name (2nd time):" + blockPoolName);
		blockName = fsckBlockDetailsBOList.get(0).blockName;
		blockName = blockName.substring(0, blockName.lastIndexOf("_"));
		TestSession.logger.info("Block name(2nd time):" + blockName);
		for (String aRack : fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
				.keySet()) {
			reallocatedDatanodes = fsckBlockDetailsBOList.get(0).dataNodeDistributionAcrossRacks
					.get(aRack);
		}

		// Get the physical block locations
		for (String aDatanode : reallocatedDatanodes) {
			TestSession.logger.info("a datanode(2nd time):" + aDatanode);
			Configuration conf = TestSession.cluster.getConf();
			String[] physicalLocations = conf
					.getStrings("dfs.datanode.data.dir");

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					TestSession.logger.info("Adding " + aDatanode
							+ " Block Location:"
							+ blockLocationReadOffOfDatanode);
					actualBlockPoolLocationsAfterCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);

				}

			}

		}

		for (final String aDatanode : actualBlockPoolLocationsAfterCorrupting
				.keySet()) {
			command = "cat  "
					+ actualBlockPoolLocationsAfterCorrupting.get(aDatanode);
			fileContentsAfterCorrupting = doJavaSSHClientExec(aDatanode,
					command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
			Assert.assertTrue(fileContentsAfterCorrupting
					.equals(fileContentsBeforeCorrupting));
		}

	}

	void bounceDatanodeAndWaitForTwoMinutes(String node) throws Exception {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		int returnCode;
		returnCode = cluster.hadoopDaemon(HadoopCluster.Action.STOP,
				HadooptestConstants.NodeTypes.DATANODE, new String[] { node });
		TestSession.logger.info("After Stopping node" + node
				+ " got return code:" + returnCode
				+ " gonna wait for 30 secs before starting the daemon");
		Thread.sleep(30000);
		cluster.hadoopDaemon(HadoopCluster.Action.START,
				HadooptestConstants.NodeTypes.DATANODE, new String[] { node });
		TestSession.logger.info("After Starting node" + node
				+ " got return code:" + returnCode
				+ " gonna wait for 2 minutes before returning, from function");
		Thread.sleep(120000);
	}

	public String doJavaSSHClientExec(String host, String command,
			String identityFile) {
		JSch jsch = new JSch();
		String user = "hdfsqa";
		TestSession.logger.info("SSH Client is about to run command:" + command
				+ "on host:" + host);
		Session session;
		StringBuilder sb = new StringBuilder();
		try {
			session = jsch.getSession(user, host, 22);
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();

			channel.connect();

			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
					TestSession.logger.info(outputFragment);
					sb.append(outputFragment);
				}
				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: "
							+ channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {
				}
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	public class MyUserInfo implements UserInfo {

		public String getPassphrase() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptPassword(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptYesNo(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public void showMessage(String arg0) {
			// TODO Auto-generated method stub

		}

	}

	String getHostNameFromIp(String ip) throws Exception {
		String hostName = null;
		StringBuilder sb = new StringBuilder();
		sb.append("/usr/bin/nslookup");
		sb.append(" ");
		sb.append(ip);

		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.DFSLOAD,
				envToUnsetHadoopPrefix);

		final String nslookupPattern = "([\\w\\.-]+)\\s+name\\s+=\\s+([\\w\\.]+)\\.";
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String aLineFromNslookupResponse = reader.readLine();
		while (aLineFromNslookupResponse != null) {
			if (aLineFromNslookupResponse.matches(nslookupPattern)) {
				hostName = aLineFromNslookupResponse.replaceAll(
						nslookupPattern, "$2");
				break;
			}
			TestSession.logger.info(aLineFromNslookupResponse
					+ " is not what I am looking for");
			aLineFromNslookupResponse = reader.readLine();
		}

		return hostName;
	}

}