package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.FsckResponseBO.FsckBlockDetailsBO;
import hadooptest.hadoop.regression.dfs.FsckResponseBO.FsckFileDetailsBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;
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
public class TestFsckCli extends DfsTestsBaseClass {

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
	public void ensureDataPresenceinClusterBeforeTest() throws Exception {
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
				dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
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
		ensureDataPresenceinClusterBeforeTest();

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				FSCK_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_TESTS_DIR_ON_HDFS);

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);
		doChmodRecursively(FSCK_BAD_DATA_TESTS_DIR_ON_HDFS);

		dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				THREE_GB_DIR_ON_HDFS);
		doChmodRecursively(THREE_GB_DIR_ON_HDFS);

		// runStdHadoopRandomWriter(RANDOM_WRITER_DATA_DIR);
		Assert.assertEquals(true,
				create3GbFile(createLocal3GbFileInThisJUnitTempFolder));

		dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
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
				dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, this.localCluster,
						pathSoFar, "777", Recursive.NO);
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
		dfsCommonCli
				.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, this.localCluster,
						Recursive.YES, Force.YES, SkipTrash.YES,
						FSCK_TESTS_DIR_ON_HDFS);
		dfsCommonCli.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, this.localCluster,
				Recursive.NO, Force.YES, SkipTrash.YES, DATA_DIR_IN_HDFS
						+ ONE_BYTE_FILE);

		// The 3gb file on the local file system is automatically deleted
		// after every test run, by JUnit
	};

	// test_fsck_02
	@Test
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
		HashMap<String, String> jobParams = new HashMap<String, String>();
		jobParams.put("mapreduce.randomwriter.bytespermap", "256000");

		YarnTestsBaseClass yarnTestBaseClass = new YarnTestsBaseClass();
		yarnTestBaseClass.runStdHadoopRandomWriter(jobParams,
				RANDOM_WRITER_DATA_DIR + randomWriterOutputDir);
		
		yarnTestBaseClass.runStdHadoopSortJob(RANDOM_WRITER_DATA_DIR + randomWriterOutputDir,
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
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "enter",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		FsckResponseBO fsckResponse = dfsCommonCli.fsck(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, DATA_DIR_IN_HDFS, true,
				true, true);
		Assert.assertNotNull(fsckResponse);
		Assert.assertEquals(fsckResponse.fsckSummaryBO.status, "HEALTHY");

                // NN was manually 'entered' into safemode so make it 'leave' 
                dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
                dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "leave",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(); 
                responseBO = dfsCommonCli.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		if ( responseBO.response.contains("Safemode is OFF") ) {
		  TestSession.logger.info("Safemode is OFF as expected");
		else
		  TestSession.logger.error("Safemode is still ON!!");
		}
		  
	}

	@Test
	// test_fsck_additional_02
	public void testFsckWithBalancer() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		TestSession.logger.info("________Beginning test testFsckWithBalancer");
		genericCliResponseBO = dfsCommonCli.balancer(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, null, null);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Try direct calling conf method for dfs.datanode.data.dir: " + 
                          TestSession.cluster.getConf().get("dfs.datanode.data.dir"));
                        TestSession.logger.debug("Check value of hadoop.tmp/dir " + 
                          TestSession.cluster.getConf().get("hadoop.tmp.dir"));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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
				doJavaSSHClientExec("hdfsqa", corruptOnThisDatanodeIP, command,
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					actualBlockPoolLocationsBeforeCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);
					command = "cat  "
							+ actualBlockPoolLocationsBeforeCorrupting
									.get(aDatanode);
					fileContentsBeforeCorrupting = doJavaSSHClientExec(
							"hdfsqa", aDatanode, command,
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
				doJavaSSHClientExec("hdfsqa", corruptOnThisDatanodeIP, command,
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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
			fileContentsAfterCorrupting = doJavaSSHClientExec("hdfsqa",
					aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

				if (!blockLocationReadOffOfDatanode.isEmpty()) {
					actualBlockPoolLocationsBeforeCorrupting.put(aDatanode,
							blockLocationReadOffOfDatanode);
					command = "cat  "
							+ actualBlockPoolLocationsBeforeCorrupting
									.get(aDatanode);
					fileContentsBeforeCorrupting = doJavaSSHClientExec(
							"hdfsqa", aDatanode, command,
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
				doJavaSSHClientExec("hdfsqa", corruptOnThisDatanodeIP, command,
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
			//String[] physicalLocations = conf.getStrings("dfs.datanode.data.dir");
                        TestSession.logger.debug("The physicalLocations for dfs.datanode.data.dir we got are: " + 
                          Arrays.toString(conf.getStrings("dfs.datanode.data.dir")));
                        TestSession.logger.debug("Hardwire correct dfs.datanode.data.dir temporarily");
                        String[] physicalLocations = {"/grid/0/tmp/hadoop-hdfsqa/dfs/data"};

			for (String aPhysicalLoc : physicalLocations) {
				command = "find " + aPhysicalLoc + "/current/" + blockPoolName
						+ " -name " + blockName;
				String blockLocationReadOffOfDatanode = doJavaSSHClientExec(
						"hdfsqa", aDatanode, command,
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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
			fileContentsAfterCorrupting = doJavaSSHClientExec("hdfsqa",
					aDatanode, command, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
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

}
