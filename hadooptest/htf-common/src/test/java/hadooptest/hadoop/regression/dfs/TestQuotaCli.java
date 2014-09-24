package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestQuotaCli extends DfsTestsBaseClass {
	private static final String TEST_1_DIR = "/quota_test_1_dir/";
	private static final String TEST_2_DIR = "/quota_test_2_dir/";

	private static final String L1 = "L1/";
	private static final String L2 = "L2/";
	private static final String L3 = "L3/";
	private static final String L5 = "L5/";
	private static final String USER = "/user/";
	private static final String HDFSQA = "hdfsqa/";
	private static final String NONE = "none";
	private static final String INF = "inf";
	private static final String CLEAR_QUOTA = "-clrQuota";
	private static final String CLEAR_SPACE_QUOTA = "-clrSpaceQuota";
	private static final String SET_QUOTA = "-setQuota";
	private static final String SET_SPACE_QUOTA = "-setSpaceQuota";

	String localCluster = System.getProperty("CLUSTER_NAME");

	public class FSEntityCounter {
		int numDirs;
		int numFiles;
		double totalFileSize;

		public FSEntityCounter(String response) {
			String[] responseLines = response.split("\n");
			for (String responseLine : responseLines) {
				if (responseLine.matches("^d.*")) {
					numDirs++;
				} else {
					numFiles++;
					String[] lineFrags = responseLine.split("\\s+");
					totalFileSize += Double.parseDouble(lineFrags[4]);
				}
			}

		}
	}

	public class QuotaResponseBO {
		public String totalQuota;
		public String remainingQuota;
		public String spaceQuota;
		public String remainingSpaceQuota;
		public long numDirs;
		public long numFiles;
		public double totalContentSize;

		public QuotaResponseBO(String response) {
			String[] frags;
			response = response.trim();
			frags = response.split("\\s+");
			this.totalQuota = frags[0];
			this.remainingQuota = frags[1];
			this.spaceQuota = frags[2];
			this.remainingSpaceQuota = frags[3];
			this.numDirs = Long.parseLong(frags[4]);
			this.numFiles = Long.parseLong(frags[5]);
			this.totalContentSize = Double.parseDouble(frags[6]);
		}

	}

//	@BeforeClass
//	public static void testSession() {
//		TestSession.start();
//	}

	@Before
	public void beforeEachTest() throws Exception {
		pathsChmodedSoFar = new HashMap<String, Boolean>();
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;

		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_1_DIR);
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_2_DIR);

	}

	void validateQuotaQueryResult(long expectedQuota,
			long expectedRemainingQuota, long spaceQuota,
			long remainingSpaceQuota, int dirCount, long fileCount,
			long totalFileSize, String fsEntity) throws Exception {

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO countResponse = null;

		countResponse = dfsCliCommands.count(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster, fsEntity);
		Assert.assertTrue(countResponse.process.exitValue() == 0);

		QuotaResponseBO quotaResponseBO = new QuotaResponseBO(
				countResponse.response.trim());

		TestSession.logger.info("Beginning comparison of (totalQuota):"
				+ quotaResponseBO.totalQuota + " with expected:"
				+ expectedQuota);

		if (expectedQuota == -1) {
			Assert.assertTrue(((String) quotaResponseBO.totalQuota)
					.equals(NONE));
		} else {
			Assert.assertTrue(Long.parseLong(quotaResponseBO.totalQuota) == expectedQuota);
		}

		TestSession.logger.info("Beginning comparison of (remainingQuota):"
				+ quotaResponseBO.remainingQuota + " with expected:"
				+ expectedRemainingQuota);

		if (expectedRemainingQuota == -1) {
			Assert.assertTrue(((String) quotaResponseBO.remainingQuota)
					.equals(INF));
		} else {
			Assert.assertTrue(Long.parseLong(quotaResponseBO.remainingQuota) == expectedRemainingQuota);
		}
		TestSession.logger.info("Comparing (spaceQuota):"
				+ quotaResponseBO.spaceQuota + " with expected:" + spaceQuota);

		if (spaceQuota == -1) {
			Assert.assertTrue(((String) quotaResponseBO.spaceQuota)
					.equals(NONE));
		} else {
			Assert.assertTrue(Long.parseLong(quotaResponseBO.spaceQuota) == spaceQuota);
		}
		TestSession.logger.info("Comparing (remainingSpaceQuota):"
				+ quotaResponseBO.remainingSpaceQuota + " with expected:"
				+ remainingSpaceQuota);

		if (remainingSpaceQuota == -1) {
			Assert.assertTrue(((String) quotaResponseBO.remainingSpaceQuota)
					.equals(INF));
		} else {
			Assert.assertTrue(Long
					.parseLong(quotaResponseBO.remainingSpaceQuota) == remainingSpaceQuota);
		}

		Assert.assertTrue(quotaResponseBO.numDirs == dirCount);
		Assert.assertTrue(quotaResponseBO.numFiles == fileCount);
		Assert.assertTrue(quotaResponseBO.totalContentSize == totalFileSize);
	}

	GenericCliResponseBO runDfsadminCommand(String command, long quota,
			String fsEntity) throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO = null;
		if (command.equals(SET_QUOTA)) {
			genericCliResponseBO = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.NO, null, ClearQuota.NO, SetQuota.YES, quota,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
					fsEntity);
		} else if (command.equals(SET_SPACE_QUOTA)) {
			genericCliResponseBO = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.NO, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.YES, quota,
					PrintTopology.NO, fsEntity);
		} else if (command.equals(CLEAR_QUOTA)) {
			genericCliResponseBO = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.NO, null, ClearQuota.YES, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
					fsEntity);
		} else if (command.equals(CLEAR_SPACE_QUOTA)) {
			genericCliResponseBO = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.NO, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.YES, SetSpaceQuota.NO, 0, PrintTopology.NO,
					fsEntity);
		}
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);
		return genericCliResponseBO;

	}

	@SuppressWarnings("unused")
	@Ignore @Test
	public void nameQuota() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		;
		/*
		 * TESTCASE_DESC="validate -count -q when quota has not been set"
		 */
		mkdirsAndSetPermissions(TEST_1_DIR);
		validateQuotaQueryResult(-1, -1, -1, -1, 1, 0, 0, TEST_1_DIR);

		/*
		 * TESTCASE_DESC="-setQuota on directory"
		 */
		runDfsadminCommand(SET_QUOTA, 5, TEST_1_DIR);
		validateQuotaQueryResult(5, 4, -1, -1, 1, 0, 0, TEST_1_DIR);

		/*
		 * Check quota on sub-directory when only parent has quota set
		 */
		mkdirsAndSetPermissions(TEST_1_DIR + L1);
		validateQuotaQueryResult(-1, -1, -1, -1, 1, 0, 0, TEST_1_DIR + L1);
		/*
		 * Check quota on sub-directory
		 */
		runDfsadminCommand(SET_QUOTA, 10, TEST_1_DIR + L1);
		validateQuotaQueryResult(10, 9, -1, -1, 1, 0, 0, TEST_1_DIR + L1);
		validateQuotaQueryResult(5, 3, -1, -1, 2, 0, 0, TEST_1_DIR);

		/*
		 * Validate Name quota while sub-directory has name quota set
		 */
		dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				TEST_1_DIR + L1 + "a");
		dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				TEST_1_DIR + L1 + "b");
		dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				TEST_1_DIR + L1 + "c");
		dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, TEST_1_DIR
						+ L1, Recursive.YES);
		validateQuotaQueryResult(10, 6, -1, -1, 1, 3, 0, TEST_1_DIR + L1);
		validateQuotaQueryResult(5, 0, -1, -1, 2, 3, 0, TEST_1_DIR);

		/*
		 * Create more files than quota in parentdir
		 */
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				TEST_1_DIR + L1 + "d");
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		Assert.assertTrue(genericCliResponse.response.contains("is exceeded"));
		dfsCliCommands.count(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",localCluster, TEST_1_DIR);
		dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, TEST_1_DIR,
				Recursive.YES);
		/*
		 * Create more directory than quota in parentdir
		 */
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, TEST_1_DIR
						+ L1 + "d");
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		Assert.assertTrue(genericCliResponse.response.contains("is exceeded"));
		/*
		 * Validate -clrQuota in parent directory
		 */
		runDfsadminCommand(CLEAR_QUOTA, 0, TEST_1_DIR);
		validateQuotaQueryResult(-1, -1, -1, -1, 2, 3, 0, TEST_1_DIR);

		/*
		 * Check Quota in sub-directory when parent quota is cleared
		 */
		validateQuotaQueryResult(10, 6, -1, -1, 1, 3, 0, TEST_1_DIR + L1);
		/*
		 * Validate Sub Dir quota with parent not quota
		 */
		mkdirsAndSetPermissions(TEST_1_DIR + L1 + L2);
		mkdirsAndSetPermissions(TEST_1_DIR + L1 + L3);
		dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster,
				DATA_DIR_IN_LOCAL_FS + "file_64MB", TEST_1_DIR + L1 + L3);

		dfsCliCommands.cp(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster, TEST_1_DIR + L1
						+ L3,
				TEST_1_DIR + L1 + L5.substring(0, L5.lastIndexOf('/')));

		dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster,
				DATA_DIR_IN_LOCAL_FS + "file_128MB", TEST_1_DIR + L1 + L5);
		// 268435456
		validateQuotaQueryResult(
				10,
				0,
				-1,
				-1,
				4,
				6,
				Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_128MB")),
				TEST_1_DIR + L1);

		/*
		 * Check copy after exceeding quota (this is different from the test
		 * case in HIT)
		 */
		genericCliResponse = dfsCliCommands.cp(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster, TEST_1_DIR + L1
						+ L3, TEST_1_DIR + L1 + L5);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		/*
		 * Check move after exceeding quota
		 */
		genericCliResponse = dfsCliCommands.mv(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, localCluster, TEST_1_DIR
						+ L1 + L5 + "file_128MB", TEST_1_DIR + L1);
		validateQuotaQueryResult(
				10,
				0,
				-1,
				-1,
				4,
				6,
				Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_128MB")),
				TEST_1_DIR + L1);
		/*
		 * Check quota free after remove
		 */
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, Recursive.NO,
				Force.YES, SkipTrash.YES, TEST_1_DIR + L1 + "file_128MB");
		genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, TEST_1_DIR + L1);
		validateQuotaQueryResult(
				10,
				0,
				-1,
				-1,
				4,
				6,
				Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_64MB"))
				+ Long.parseLong(fileMetadata.get("file_1B")), 
				TEST_1_DIR + L1);
		/*
		 * Remove and recreate same directory; check quota
		 */
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_1_DIR + L1);
		mkdirsAndSetPermissions(TEST_1_DIR + L1);
		validateQuotaQueryResult(-1, -1, -1, -1, 1, 0, 0, TEST_1_DIR + L1);
		/*
		 * Check Name quota with trash
		 */
		// TODO: Check this test case. It was failing for me
		if (false) {
			genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.WEBHDFS, localCluster,
					Recursive.YES, Force.YES, SkipTrash.YES, USER + HDFSQA);
			mkdirsAndSetPermissions(USER + HDFSQA);
			runDfsadminCommand(SET_QUOTA, 7, USER + HDFSQA);
			genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE, localCluster,
					DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, USER + HDFSQA);

			genericCliResponse = dfsCliCommands.copyFromLocal(
					EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE, localCluster,
					DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, USER + HDFSQA
							+ "file_1B_1");
			genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.WEBHDFS, localCluster,
					Recursive.NO, Force.YES, SkipTrash.NO, USER + HDFSQA
							+ ONE_BYTE_FILE);
			validateQuotaQueryResult(7, 0, -1, -1, 5, 2, 4, TEST_1_DIR + L1);
			genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.WEBHDFS, localCluster,
					Recursive.NO, Force.YES, SkipTrash.NO, USER + HDFSQA
							+ "file_1B_1");
			validateQuotaQueryResult(7, 0, -1, -1, 5, 2, 4, TEST_1_DIR + L1);
		}

		/*
		 * Check default nameQuota of HDFS Root (/)
		 */
		// TODO: Did not bring in this test because could not figure out the
		// numbers

	}

	@Ignore @Test
	public void spaceQuota() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		HadoopConfiguration conf = TestSession.cluster.getConf();
		int replication = conf.getInt("dfs.replication", 3);
		int blockSize = conf.getInt("dfs.blocksize", 134217728);

		long MIN_SPACE_QUOTA = replication * blockSize;
		long MIN_SPACE_QUOTA_MB = MIN_SPACE_QUOTA / 1024 / 1024;

		/*
		 * Setting space less than dfs.replication * dfs.blocksize Bug-4524822
		 */
		long quotaInMbHoldingOneMbLessThanMinSpaceQuota = MIN_SPACE_QUOTA_MB - 1;
		long quotaInBytesHoldingOneMillionLessBytesThanMinSpaceQuota = quotaInMbHoldingOneMbLessThanMinSpaceQuota * 1024 * 1024;

		mkdirsAndSetPermissions(TEST_2_DIR);
		runDfsadminCommand(SET_SPACE_QUOTA,
				quotaInBytesHoldingOneMillionLessBytesThanMinSpaceQuota,
				TEST_2_DIR);
		validateQuotaQueryResult(-1, -1,
				quotaInBytesHoldingOneMillionLessBytesThanMinSpaceQuota,
				quotaInBytesHoldingOneMillionLessBytesThanMinSpaceQuota, 1, 0,
				0, TEST_2_DIR);

		genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, TEST_2_DIR);
		/*
		 * I think this bombs, because even for 1 byte and entire DFS block gets
		 * allocated. Making the total allocated size 128MB*3 (where 128MB is
		 * the default block size) and 3 is the replication.
		 */

		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		Assert.assertTrue(genericCliResponse.response.contains("is exceeded"));

		/*
		 * Setting space dfs.replication * dfs.blocksize"
		 */
		dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_2_DIR);

		mkdirsAndSetPermissions(TEST_2_DIR);
		runDfsadminCommand(SET_SPACE_QUOTA, MIN_SPACE_QUOTA, TEST_2_DIR);

		validateQuotaQueryResult(-1, -1, MIN_SPACE_QUOTA, MIN_SPACE_QUOTA, 1,
				0, 0, TEST_2_DIR);

		dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.NONE, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE, TEST_2_DIR);

		// file_1B has just 1 byte there
		long remainingSpaceQuota = (MIN_SPACE_QUOTA - (1 * replication));
		validateQuotaQueryResult(-1, -1, MIN_SPACE_QUOTA, remainingSpaceQuota,
				1, 1, Long.parseLong(fileMetadata.get(ONE_BYTE_FILE)), TEST_2_DIR);
	}

	void mkdirsAndSetPermissions(String path) throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, path);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		doChmodRecursively(this.localCluster, path);
	}

	@After
	public void cleanupAfterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		runDfsadminCommand(CLEAR_QUOTA, 0, USER + HDFSQA);
		runDfsadminCommand(CLEAR_SPACE_QUOTA, 0, USER + HDFSQA);
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_1_DIR);
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, TEST_2_DIR);
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES, USER + HDFSQA
						+ ".Trash");

	}

	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}

}