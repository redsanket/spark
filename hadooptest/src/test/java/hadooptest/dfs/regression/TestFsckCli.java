package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.dfs.regression.DfsBaseClass.ClearQuota;
import hadooptest.dfs.regression.DfsBaseClass.ClearSpaceQuota;
import hadooptest.dfs.regression.DfsBaseClass.Force;
import hadooptest.dfs.regression.DfsBaseClass.Recursive;
import hadooptest.dfs.regression.DfsBaseClass.SetQuota;
import hadooptest.dfs.regression.DfsBaseClass.SetSpaceQuota;
import hadooptest.dfs.regression.DfsBaseClass.SkipTrash;
import hadooptest.dfs.regression.DfsCliCommands.GenericCliResponseBO;
import hadooptest.dfs.regression.FsckResponseBO.FsckBlockDetailsBO;
import hadooptest.dfs.regression.FsckResponseBO.FsckFileDetailsBO;
import hadooptest.monitoring.Monitorable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class TestFsckCli extends DfsBaseClass {

	static Logger logger = Logger.getLogger(TestFsckCli.class);

	public static final String BYTES_PER_MAP = "mapreduce.randomwriter.bytespermap";
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

	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";

	// Supporting Data
	static List<String> dataNodes;
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
			GenericCliResponseBO doesFileExistResponseBO = dfsCommonCli.test(
					EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE, aCluster, DATA_DIR_IN_HDFS
							+ justTheFileName,
					DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
			if (doesFileExistResponseBO.process.exitValue() != 0) {
				dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, aCluster,
						DATA_DIR_IN_HDFS);
				doChmodRecursively(DATA_DIR_IN_HDFS);
				dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, aCluster,
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
	 * Creates a temporary directory before each test run to create a 3gb file.
	 */
	@Rule
	public TemporaryFolder createLocal3GbFileInThisJUnitTempFolder = new TemporaryFolder(
			new File(DATA_DIR_IN_LOCAL_FS));

	@Before
	public void beforeEachTest() throws Throwable {
		pathsChmodedSoFar = new HashMap<String, Boolean>();
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		// getVersions();
		ensureDataPresenceinCluster();

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				FSCK_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_TESTS_DIR_ON_HDFS);

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				THREE_GB_DIR_ON_HDFS);
		doChmodRecursively(THREE_GB_DIR_ON_HDFS);

		// runStdHadoopRandomWriter(RANDOM_WRITER_DATA_DIR);
		Assert.assertEquals(true,
				create3GbFile(createLocal3GbFileInThisJUnitTempFolder));

		dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				createLocal3GbFileInThisJUnitTempFolder.getRoot() + "/"
						+ THREE_GB_FILE_NAME, THREE_GB_DIR_ON_HDFS
						+ THREE_GB_FILE_NAME);

	}

	private void doChmodRecursively(String dirHierarchy) throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty())
				continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir + "/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			if (!pathsChmodedSoFar.containsKey(pathsChmodedSoFar)) {
				dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, this.localCluster,
						pathSoFar, "777");
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
		dfsCommonCli.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster, Recursive.YES, Force.YES,
				SkipTrash.YES, FSCK_TESTS_DIR_ON_HDFS);
		dfsCommonCli.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster, Recursive.NO, Force.YES,
				SkipTrash.YES, DATA_DIR_IN_HDFS + ONE_BYTE_FILE);

		// The 3gb file on the local file system is automatically deleted
		// after every test run, by JUnit
	};

//	 @Test
	// test_fsck_02
	public void testFsckResultsLeveragingRandomWriterAndSortJobs()
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testFsckResultsLeveragingRandomWriterAndSortJobs");
		String randomWriterOutputDir = "testFsckResultsLeveragingRandomWriterAndSortJobs";
		String sortJobOutputDir = "testFsckResultsLeveragingRandomWriterAndSortJobs";
		logger.info("Hello this is testFsckResultsLeveragingRandomWriterAndSortJobs");
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS, true,
				true, true);
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

		fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS, true,
				true, true);
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

	 @Test
	// test_fsck_additional_01
	public void testFsckWithSafemodeOn() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger
				.info("________Beginning test testFsckWithSafemodeOn");
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, "get", ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, "enter", ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, "get", ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS, true,
				true, true);
		Assert.assertNotNull(fsckResponse);
		Assert.assertEquals(fsckResponse.fsckSummaryBO.status, "HEALTHY");
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, "leave", ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, "get", ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		

	}

	 @Test
	// test_fsck_additional_02
	public void testFsckWithBalancer() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		TestSession.logger.info("________Beginning test testFsckWithBalancer");
		genericCliResponseBO = dfsCommonCli.balancer(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() ==0);
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS, true,
				true, true);
		Assert.assertNotNull(fsckResponse);
		Assert.assertEquals(fsckResponse.fsckSummaryBO.status, "HEALTHY");

	}

	@Monitorable
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
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
						+ ONE_BYTE_FILE, true, true, true);
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
		fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
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

	@Monitorable
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

		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
						+ ONE_BYTE_FILE, true, true, true);
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
		fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
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

	@Monitorable
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
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
						+ ONE_BYTE_FILE, true, true, true);
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
		fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS
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
	@After
	public void logTaskResportSummary() {
		// Override to hide the Test Session logs
	}

}