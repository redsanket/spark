package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(SerialTests.class)
public class TestTrashFeatureOfHdfs extends DfsTestsBaseClass {
	String protocol = HadooptestConstants.Schema.HDFS;

	File folderNamedSuitestartOnHdfs;
	private static String TEST_FOLDER_NAME_ON_HDFS = "suitestart";
	private boolean skipRemovingDotTrashInAfterMethod = false;
	private boolean skipRemovingFilesOnHdfsInAfterMethod = false;

	@Rule
	public TemporaryFolder tempTrashFeatureFolder = new TemporaryFolder();
	public TemporaryFolder tempTrashFeatureFolderExpected = new TemporaryFolder();
	public TemporaryFolder tempTrashFeatureFolderResult = new TemporaryFolder();

	@Before
	public void setupTest() throws Exception {
        logger.info("Test invoked for protocol/schema:" + protocol);
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		folderNamedSuitestartOnHdfs = tempTrashFeatureFolder
				.newFolder(TEST_FOLDER_NAME_ON_HDFS);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		skipRemovingDotTrashInAfterMethod = false;
		skipRemovingFilesOnHdfsInAfterMethod = false;
	}

	/*
	 * test_FsTrashIntervalNONZERO_Filedelete
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_Filedelete() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		logger.info("Absolute file:"
				+ folderNamedSuitestartOnHdfs.getAbsoluteFile());
		logger.info("Absolute path:"
				+ folderNamedSuitestartOnHdfs.getAbsolutePath());
		logger.info("Canonical path:"
				+ folderNamedSuitestartOnHdfs.getCanonicalPath());
		logger.info("Name:" + folderNamedSuitestartOnHdfs.getName());
		logger.info("Path:" + folderNamedSuitestartOnHdfs.getPath());

		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern(ONE_BYTE_FILE,
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent(ONE_BYTE_FILE,
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalNONZERO_FileDirDelete [Trash-2] Set fs.trash.interval
	 * to non zero value, Create a file named A in a directory named B, delete
	 * file A, verify it was moved to the trash, delete directory B, verify it
	 * was moved to the trash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_FileDirDelete() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		// touchz
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/B/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm A
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/" + "B/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("B/A",
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("B/A",
						HadooptestConstants.UserNames.HADOOPQA)));

		// rm B
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/" + "B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("B",
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("B",
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalNONZERO_DirWithFiles Set fs.trash.interval to non
	 * zero value, create a directory(with only files) and delete it
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_DirWithFiles() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// touchz
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r A
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/" + "A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("A",
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A",
						HadooptestConstants.UserNames.HADOOPQA)));
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A/B",
						HadooptestConstants.UserNames.HADOOPQA)));
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A/" + ONE_BYTE_FILE,
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalNONZERO_DirWithSubDir
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_DirWithSubDir() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir A/B
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// mkdir A/C
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/C");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r A
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/" + "A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("A",
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A",
						HadooptestConstants.UserNames.HADOOPQA)));
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A/C",
						HadooptestConstants.UserNames.HADOOPQA)));
		Assert.assertTrue(genericCliResponse.response
				.contains(createExpectedDotTrashContent("A/B",
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalZERO_Filedelete Set fs.trash.interval to 10 secs
	 * value, Touch a file, Delete it
	 * 
	 * Not:ported, because do not have permissions to edit core-site.xml
	 */
	@Deprecated
	// \\//\\//@Test
	public void test_FsTrashIntervalZERO_Filedelete() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String coreSiteXml = HadooptestConstants.ConfFileNames.CORE_SITE_XML;
		GenericCliResponseBO genericCliResponse;
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		cluster.getConf(null).setHadoopConfFileProp("fs.trash.interval",
				"10000", coreSiteXml);
		/*
		 * ## This sleep was required, otherwise new config was not visible
		 * (Commented taken directly from HIT)
		 */
		Thread.sleep(10000);

		// touchz
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/new");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm new
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/new");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getDeletedResponsePattern("new")));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		TestSession.logger.info("Exxxjit value:"
				+ genericCliResponse.process.exitValue());
		TestSession.logger.info("Exxjit response:"
				+ genericCliResponse.response);
		// Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	/*
	 * test_FsTrashIntervalNONZERO_Del_Trash Set fs.trash.interval to non zero
	 * value, Delete the Trash"
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_Del_Trash() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir A
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// mkdir B
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO, ".Trash");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains("Deleted .Trash"));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		skipRemovingDotTrashInAfterMethod = true;

	}

	/*
	 * test_FsTrashIntervalNONZERO_FilesInTrash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_FilesInTrash() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// touchz
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// touchz
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// copyFromLocal
		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		skipRemovingFilesOnHdfsInAfterMethod = true;

		// Question: where did it go, after delete?
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		/*
		 * Answer: It went here 14/02/04 22:19:36 DEBUG
		 * hadooptest.TestSessionCore: drwx------ - hadoopqa hdfs 0 2014-02-04
		 * 22:19 .Trash/Current 14/02/04 22:19:36 DEBUG
		 * hadooptest.TestSessionCore: drwx------ - hadoopqa hdfs 0 2014-02-04
		 * 22:19 .Trash/Current/tmp 14/02/04 22:19:36 DEBUG
		 * hadooptest.TestSessionCore: drwx------ - hadoopqa hdfs 0 2014-02-04
		 * 22:19 .Trash/Current/tmp/junit2944988047732710084 14/02/04 22:19:36
		 * DEBUG hadooptest.TestSessionCore: drwx------ - hadoopqa hdfs 0
		 * 2014-02-04 22:19
		 * .Trash/Current/tmp/junit2944988047732710084/suitestart 14/02/04
		 * 22:19:36 DEBUG hadooptest.TestSessionCore: -rw------- 3 hadoopqa hdfs
		 * 0 2014-02-04 22:19
		 * .Trash/Current/tmp/junit2944988047732710084/suitestart/A 14/02/04
		 * 22:19:36 DEBUG hadooptest.TestSessionCore: -rw------- 3 hadoopqa hdfs
		 * 0 2014-02-04 22:19
		 * .Trash/Current/tmp/junit2944988047732710084/suitestart/B 14/02/04
		 * 22:19:36 DEBUG hadooptest.TestSessionCore: -rw------- 3 hadoopqa hdfs
		 * 1 2014-02-04 22:19
		 * .Trash/Current/tmp/junit2944988047732710084/suitestart/file_1B
		 */

		// rm -r .Trash + the temp folder
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO, ".Trash/Current"
						+ folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains("Deleted .Trash"));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		skipRemovingDotTrashInAfterMethod = true;

	}

	/*
	 * test_FsTrashIntervalNONZERO_DirWithFilesInTrash [Trash-10] Delete a
	 * directory(with files only) in the Trash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_DirWithFilesInTrash()
			throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir A
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// touchz A/B
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// copyFromLocal
		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/A
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("A",
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalNONZERO_DirWithSubDirInTrash [Trash-10] Delete a
	 * directory(with files only) in the Trash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_DirWithSubDirInTrash()
			throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// mkdir A/B
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/B");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// mkdir A/C
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A/C");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/A
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/A");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern("A",
						HadooptestConstants.UserNames.HADOOPQA)));

	}

	/*
	 * test_FsTrashIntervalNONZERO_Filedelete_skipTrash [Trash-12] Set
	 * fs.trash.interval to non zero, value, Create a file, delete it with
	 * -skipTrash, verify it is not moved to the trash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_Filedelete_skipTrash()
			throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// copyFromLocal
		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/ONE_BYTE_FILE
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(getDeletedResponsePattern(ONE_BYTE_FILE)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);

		Assert.assertFalse(genericCliResponse.response
				.contains(folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE));
		skipRemovingDotTrashInAfterMethod = true;
	}

	/*
	 * test_FsTrashIntervalNONZERO_FileDirDel_Namenode [Trash-13] Set
	 * fs.trash.interval to non zero value in Namenode, Create a file, delete,
	 * verify it is moved from the trash after give time
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_FileDirDel_Namenode()
			throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String[] componentsToUpdate = new String[] { HadooptestConstants.NodeTypes.NAMENODE };
		String coreSiteXml = HadooptestConstants.ConfFileNames.CORE_SITE_XML;

		for (String aComponentToUpdate : componentsToUpdate) {

			cluster.getConf(aComponentToUpdate).backupConfDir();

			// Update the values
			cluster.getConf(aComponentToUpdate).setHadoopConfFileProp(
					"fs.trash.interval", "1", coreSiteXml); // The Unit is
															// Minutes
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);

		}
		// wait up to 5 minutes for NN to be out of safemode 
                for (waitCounter = 0; waitCounter < 30; waitCounter++) {
		  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds"); 
                    break; 
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail(); 
                  }
                  else
                  {
                    sleep(10000);
                  }
                }

		// copyFromLocal
		genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/ONE_BYTE_FILE
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Check that it is in trash, ..now
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);

		Assert.assertTrue(genericCliResponse.response
				.contains(folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE));

		int sleepTime = 100000;
		TestSession.logger.info("Waiting for " + sleepTime
				+ " milliseconds, before checking trash again");
		Thread.sleep(sleepTime); // Sleep for, greater than a minute

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.NO);

		Assert.assertFalse(genericCliResponse.response
				.contains(folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE));

		// Restore the values
		for (String aComponentToUpdate : componentsToUpdate) {

			cluster.getConf(aComponentToUpdate).resetHadoopConfDir();
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);
		}
		skipRemovingDotTrashInAfterMethod = true;

		// wait up to 5 minutes for NN to be out of safemode 
                for (waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    sleep(10000);
                  }
                }


	}

	/*
	 * test_FsTrashIntervalNONZERO_JIRA_HADOOP1665 [Trash-12] Set
	 * fs.trash.interval to non zero, value, Create a file, delete it with
	 * -skipTrash, verify it is not moved to the trash
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_JIRA_HADOOP1665() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// put
		genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/ONE_BYTE_FILE
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern(ONE_BYTE_FILE,
						HadooptestConstants.UserNames.HADOOPQA)));

		// put, again
		genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DATA_DIR_IN_LOCAL_FS + ONE_BYTE_FILE,
				folderNamedSuitestartOnHdfs.getPath());
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// rm -r suitestart/ONE_BYTE_FILE
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO,
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		Assert.assertTrue(genericCliResponse.response
				.contains(getMovedResponsePattern(ONE_BYTE_FILE,
						HadooptestConstants.UserNames.HADOOPQA)));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				".Trash", Recursive.YES);
		Assert.assertTrue(genericCliResponse.response
				.contains(folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE));
		TestSession.logger.info("Response BEFORE replacement"
				+ genericCliResponse.response);
		// Since there would be 2 occurances, check the 2nd
		genericCliResponse.response = genericCliResponse.response.replace(
				folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE, "");
		TestSession.logger.info("Response AFTER replacement"
				+ genericCliResponse.response);
		Assert.assertFalse(genericCliResponse.response
				.contains(folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
						+ ONE_BYTE_FILE));

	}

	/*
	 * test_FsTrashIntervalNONZERO_JIRA_HADOOP3561 With trash enabled, Do
	 * 'hadoop fs -rm -R .' Check working directory is not removed
	 */
	@Test
	public void test_FsTrashIntervalNONZERO_JIRA_HADOOP3561() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		String savedResponseLsMinusROnUsersHadoopqa;
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				"/user/" + HadooptestConstants.UserNames.HADOOPQA + "/",
				Recursive.YES);
		savedResponseLsMinusROnUsersHadoopqa = genericCliResponse.response;

		// rm -r .
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.NO, " " + ".");
		Assert.assertTrue(genericCliResponse.process.exitValue() != 0);
		Assert.assertTrue(genericCliResponse.response
				.contains("/user/hadoopqa\" to the trash, as it contains the trash. Consider using -skipTrash option"));

		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
				"/user/" + HadooptestConstants.UserNames.HADOOPQA + "/",
				Recursive.YES);

		Assert.assertTrue(genericCliResponse.response
				.equals(savedResponseLsMinusROnUsersHadoopqa));
		skipRemovingFilesOnHdfsInAfterMethod = true;
		skipRemovingDotTrashInAfterMethod = true;

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;

		/*
		 * Because JUnit names the temp folders /tmp/junit*
		 */
		if (!skipRemovingFilesOnHdfsInAfterMethod) {
			genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, protocol,
					localCluster, Recursive.YES, Force.NO, SkipTrash.YES,
					"/tmp/junit*");
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		}
		if (!skipRemovingDotTrashInAfterMethod) {
			String dotTrashFolder = ".Trash/Current/"
					+ folderNamedSuitestartOnHdfs.getAbsolutePath();
			genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, "", localCluster,
					Recursive.YES, Force.NO, SkipTrash.NO, dotTrashFolder);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		}

	}

	/*
	 * Helper functions
	 */
	String getMovedResponsePattern(String fileName, String user) {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		String pattern = "Moved: '"
				+ dfsCliCommands.getNNUrlForHdfs(localCluster)
				+ folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
				+ fileName + "' to trash at: "
				+ dfsCliCommands.getNNUrlForHdfs(localCluster) + "/user/"
				+ user + "/.Trash/Current";
		TestSession.logger.info("getMovedResponsePattern is returning:"
				+ pattern);
		return pattern;
	}

	String getDeletedResponsePattern(String fileName) {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		String pattern = "Deleted "
				+ dfsCliCommands.getNNUrlForHdfs(localCluster)
				+ folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
				+ fileName;
		TestSession.logger.info("getDeletedResponsePattern is returning:"
				+ pattern);
		return pattern;
	}

	String createExpectedDotTrashContent(String fileName, String userName) {
		String dotTrashFolder = ".Trash/Current"
				+ folderNamedSuitestartOnHdfs.getAbsolutePath() + "/"
				+ fileName;
		TestSession.logger.info("getDotTrashContent is returning:"
				+ dotTrashFolder);
		return dotTrashFolder;
	}

}
