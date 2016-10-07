package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestArchives extends DfsTestsBaseClass {
	String protocol = HadooptestConstants.Schema.HDFS;
	
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DIR = "/user/hadoopqa/archive_tests/";
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR = "/user/hadoopqa/archive_tests/src/";
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR = "/user/hadoopqa/archive_tests/dst/";
	private static String localCluster = System.getProperty("CLUSTER_NAME");

	@Before
	public void beforeEachTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		// Create the source-dir(s)
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Create the destination-dir
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	/**
	 * #Steps: #Archive does not occur in the destination #1. Try to create an
	 * archive with an invalid name # #Expected: #Invalid name for archives.
	 * Should be printed out to the screen #TC ID=1207939
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive2() throws Exception {
		String testCaseDesc = "archive_2/";
		String INVALID_DIR = "gzz_dd/";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				INVALID_DIR + "xx.har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

	}

	/**
	 * Steps: #Archive does not get overwritten without any warning #1. Create
	 * an archive #2. Create another archive but provide the same archive name
	 * as in step 1. # #Expected: #You should not be allowed to overwrite an
	 * existing archive. You should get the following output "Invalid Output"
	 * #TC ID=1207940 *
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive3() throws Exception {
		String testCaseDesc = "archive_3";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Run the command again and ensure that it does not overwrite the
		// archive file
		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

	}

	/**
	 * #Steps: #Same source and destination is specified #1. Create an archive
	 * and the destination should be in the same folder which is being archived.
	 * # #Expected: #Archive should exist in the appropriate folder #TC
	 * ID=1207941
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive4() throws Exception {
		String testCaseDesc = "archive_4";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Check that the file did get created
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc + ".har", Recursive.NO);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response.contains(testCaseDesc
				+ ".har"));

	}

	/**
	 * #Destination is a file #1. Try to create an archive and specify a file as
	 * the location of the archive # #Expected: #Archive will fail as
	 * destination that is specified is a file, archive needs a dir #TC
	 * ID=1207942 *
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive5() throws Exception {
		String testCaseDesc = "archive_5";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + "file_1B",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ "file_1B");
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

		// gridci-1600, 2.8 changed the error response 
		Assert.assertTrue(genericCliResponse.response
				.contains("Permission denied:" || "is not a directory"));

	}

	/**
	 * #Steps: #Deleting a file in an archive #1. Create an archive #2. Delete a
	 * file in the archive # #Expected: #Delete of the file in the archive
	 * should be successful #TC ID=1207943
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive6() throws Exception {
		String testCaseDesc = "archive_6";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster, Recursive.NO,
				Force.YES, SkipTrash.NO,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + "_masterindex");
		Assert.assertTrue(
				"Removing "
						+ TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + "_masterindex FAILED",
				genericCliResponse.response
						.contains(TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR + testCaseDesc +".har/_masterindex' to trash"));


	}

	/**
	 * #Steps: #Renaming a file in an archive #1. create an archive #2. rename
	 * one of the files in the archive # #Expected: #One should be able to
	 * rename a file in the archive #TC ID=1207944
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive7() throws Exception {
		String testCaseDesc = "archive_7";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.mv(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				localCluster, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + "_index",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + "_index2");
		Assert.assertTrue("Wasn't able to move a file withn a .har archive",
				genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + "_index2", Recursive.NO);
		Assert.assertTrue("Wasn't able to 'ls' the moved file",
				genericCliResponse.process.exitValue() == 0);

	}

	/**
	 * #Steps: #Testing count in an archive #1. create an archive #2. run the
	 * hadoop dfs -count command # #Expected: #count should succeed and return
	 * appropriate value #TC ID=1207945 *
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive8() throws Exception {
		String testCaseDesc = "archive_8";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.count(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har");
		Assert.assertTrue("count command failed when run on:"
				+ TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
				+ testCaseDesc + ".har",
				genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue("", genericCliResponse.response
				.contains("1            3                132"));

	}

	/**
	 * #Steps: #Archiving the directory created by the randomwriter #1. Run a
	 * randomwriter job #2. Archive the dir where randomwriter output is stored
	 * # #Expected: #Archive should complete successfully. #TC ID=1207946 *
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive9() throws Exception {
		String testCaseDesc = "archive_9";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		// Run a Random-writer job
		HashMap<String, String> jobParams = new HashMap<String, String>();
		jobParams.put("mapreduce.randomwriter.bytespermap", "256000");
		YarnTestsBaseClass yarnTestBaseClass = new YarnTestsBaseClass();
		yarnTestBaseClass.runStdHadoopRandomWriter(jobParams,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	/**
	 * #Expected: #Sort should complete successfully #JIRA
	 * https://issues.apache.org/jira/browse/MAPREDUCE-1752. Still seems to be
	 * causing issues, Bug 4307735 #TC ID=1207948 *
	 * 
	 * @throws Exception
	 */
	@Ignore @Test
	public void testArchive11() throws Exception {
		String testCaseDesc = "archive_11";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		// Run a Random-writer job
		HashMap<String, String> jobParams = new HashMap<String, String>();
		jobParams.put("mapreduce.randomwriter.bytespermap", "256000");
		YarnTestsBaseClass yarnTestBaseClass = new YarnTestsBaseClass();
		yarnTestBaseClass.runStdHadoopRandomWriter(jobParams,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
				+ testCaseDesc);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har", Recursive.NO);

//		yarnTestBaseClass.runStdHadoopSortJob(
//				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
//						+ testCaseDesc + ".har/" + testCaseDesc
//						+ "/part-m-0000",
//				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
//						+ "sortOutput");

		yarnTestBaseClass.runStdHadoopSortJob(
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har/" + testCaseDesc
						+ "/part-0",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ "sortOutput");

	}

	/**
	 * #Steps: #Destination is not specified #1. Create an archive but do not
	 * specify the destination directory # #Expected: #Command should fail and
	 * appropriate message should be displayed. Bug 4307734 #TC ID=1207949 *
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testArchive12() throws Exception {
		String testCaseDesc = "archive_12";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc, "");
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

	}

	/**
	 * #Steps: #Source files are specififed using regex #1. Create an archive
	 * using regex to determine the source #hadoop archive -archiveName temp.har
	 * -p <parent> <some_src*> <dest> # #Expected: #All appropriate content
	 * should be archived #TC ID=1207951 *
	 * 
	 * @throws Exception
	 */
	@Test
	public void testArchive14() throws Exception {
		String testCaseDesc = "archive_14";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		for (String aLocalFile : setOfTestDataFilesInLocalFs) {
			genericCliResponse = dfsCliCommands.copyFromLocal(
					EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA,
					protocol, localCluster, aLocalFile,
					TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
							+ testCaseDesc + "/");
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		}

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har", Recursive.YES);

	}

	/**
	 * #Steps: #Source directories are specififed using regex #1. Create an
	 * archive using regex to determine the source #hadoop archive -archiveName
	 * temp.har -p <parent> <some_src*> <dest> # #Expected: #All appropriate
	 * content should be archived #TC ID=1207952 *
	 * 
	 * @throws Exception
	 */
	@Test
	public void testArchive15() throws Exception {
		String testCaseDesc = "archive_15";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		
		String[] extensions = new String[]{"_a", "_b", "_c", "_d"};
		for (String anExtension:extensions){
			genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
					TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
							+ testCaseDesc +anExtension);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
			
		}

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc+"_*",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har", Recursive.YES);

	}

	/**
	 * #Steps: #Source directories are specififed using regex #1. Create an
	 * archive using regex to determine the source #hadoop archive -archiveName
	 * temp.har -p <parent> <some_src*> <dest> # #Expected: #All appropriate
	 * content should be archived #TC ID=1207952 *
	 * 
	 * @throws Exception
	 */
	@Test
	public void testArchive16() throws Exception {
		String testCaseDesc = "archive_16";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		
		String[] extensions = new String[]{"_a", "_b", "_c", "_d"};
		for (String anExtension:extensions){
			genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
					TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
							+ testCaseDesc +anExtension);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
			
		}

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".har",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc+"_*",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR
						+ testCaseDesc + ".har", Recursive.YES);

	}

	/**
#Steps:
#Invalid name for archives
#1. Create an archive but give a non .har extension
#
#Expected:
#archive command shouild fail
#TC ID=1207955	 * 
	 * @throws Exception
	 */
	@Test
	public void testArchive19() throws Exception {
		String testCaseDesc = "archive_19";

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR
						+ testCaseDesc);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		
		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".jar",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc+"_*",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

		genericCliResponse = dfsCliCommands.archive(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				testCaseDesc + ".zip",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR,
				testCaseDesc+"_*",
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		// Delete the source dir
		dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_SRC_DIR);

		// Delete the destination dir
		dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DST_DIR);

		genericCliResponse = dfsCliCommands.rmdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DIR);
		Assert.assertTrue("not able to 'rmdir' "
				+ TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_ARCHIVE_TESTS_DIR,
				genericCliResponse.process.exitValue() == 0);

	}

}
