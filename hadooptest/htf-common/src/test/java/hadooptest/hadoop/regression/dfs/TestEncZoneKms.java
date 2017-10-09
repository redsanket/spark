package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;


/* 
 *  TestEncZoneKms.java
 *    
 *  This class uses KMS/EZ support methods in DfsCliCommands to exercise Encryption Zones in hdfs. 
 *  Nominal case is path creation by normal user 'hadoop3', EZ created and listed by hdfsqa, and 
 *  performs basic r/w operations in the EZ as hadoop3, using sm/med/lg files in /HTF/testdata. 
 *
 *  NOTES:
 *  1. KMS/EZ does not currently support webhdfs protocol (will/should it?)
 *  2. KMS/EZ Core issue using fully qualified hdfs/nn protocol (YHADOOP-1961/HDFS-12586))
 *
*/

public class TestEncZoneKms extends DfsTestsBaseClass {
        String protocol;
	String pathToEz;
	String kmsKeyToUseForEzCreate = "FlubberKmsKey1";

        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1 = "/tmp/hadoop3/BaseDirInEz1/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2 = "/tmp/hadoop3/BaseDirInEz2/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR3 = "/tmp/hadoop3/BaseDirInEz3/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR4 = "/tmp/hadoop3/BaseDirInEz4/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR5 = "/tmp/hadoop3/BaseDirInEz5/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR6 = "/tmp/hadoop3/BaseDirInEz6/";
        private static String localCluster = System.getProperty("CLUSTER_NAME");
        private static String SSH_OPTS_1 = "-o StrictHostKeyChecking=no";
        private static String SSH_OPTS_2 = "-o UserKnownHostsFile=/dev/null";

        @Parameters
        public static Collection<Object[]> data() {
                return Arrays.asList(new Object[][] {
                                // Schemas
                                { HadooptestConstants.Schema.WEBHDFS }, { "" },
                                { HadooptestConstants.Schema.HDFS }, });
        }


	/*
	 * create/verify/usage of EZ without qualified protocol/nn (use hdfs '/' as root fs path)
	 *
	*/
	@Test public void test_CopyFilesToEz_none() throws Exception { test_CopyFilesToEz(""); }
	@Test public void test_GetEzFileMetadata_none() throws Exception { test_GetEzFileMetadata(""); }
	@Test public void test_CopyFilesFromEz_none() throws Exception { test_CopyFilesFromEz(""); }
	@Test public void test_CopyFilesToEzFromLocal_none() throws Exception { test_CopyFilesToEzFromLocal(""); }
	@Test public void test_RunYarn_RW_Sort_UsingEz_none() throws Exception { test_RunYarn_RW_Sort_UsingEz(""); }

	/*
	 * create/verify/usage of EZ with qualified protocol/nn (hdfs://<namenode_host>)
	 * NOTE:  ignored for now due to product bug YHADOOP-1961
	 *
	*/
	@Ignore
	@Test public void test_CopyFilesToEz_hdfs() throws Exception { 
		test_CopyFilesToEz("HadooptestConstants.Schema.HDFS"); }
	@Ignore
	@Test public void test_GetEzFileMetadata_hdfs() throws Exception {
		 test_GetEzFileMetadata("HadooptestConstants.Schema.HDFS"); }
	@Ignore
	@Test public void test_CopyFilesFromEz_hdfs() throws Exception { 
		test_CopyFilesFromEz("HadooptestConstants.Schema.HDFS"); }
	@Ignore
	@Test public void test_CopyFilesToEzFromLocal_hdfs() throws Exception { 
		test_CopyFilesToEzFromLocal("HadooptestConstants.Schema.HDFS"); }
	@Ignore
	@Test public void test_RunYarn_RW_Sort_UsingEz_hdfs() throws Exception {
		 test_RunYarn_RW_Sort_UsingEz("HadooptestConstants.Schema.HDFS"); }


	/* utility method used by tests to setup an EZ from given hdfs path, this does
	 * the basic actions needed to establish an encryption zone, making it available
	 * for use by normal users such as r/w hdfs data or exec jobs aginst the filesystem
	 *
	 * This method does the following:
	 *   as normal user, delete given hdfs path, can already be an EZ or not
	 *   as normal user, create hdfs path
	 *   as normal user, list the new hdfs path
	 *   as hdfs priv user, create EZ from given path 
	 *   as hdfs priv user, list EZs and verify given path is included
	*/
        private void setupTest(String protocol, String pathToEz) throws Exception {
        	this.protocol = protocol;
		this.pathToEz = pathToEz;

        	logger.info("Test invoked for protocol/schema:" + protocol);
        	logger.info("Test invoked using path to Encryption Zone:" + pathToEz);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;


                // Delete dir if already exists
                genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                Recursive.YES, Force.YES, SkipTrash.YES,
                                pathToEz);

                Util.sleep(10);

                // create user base hdfs path that was passed in 
                genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                pathToEz);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // list base hdfs path 
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                pathToEz, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// as hdfsqa, create encryption zone
		genericCliResponse = dfsCliCommands.createZone(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
                                pathToEz, kmsKeyToUseForEzCreate);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// as hdfsqa, list the encryption zones
                genericCliResponse = dfsCliCommands.listZones(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA, protocol, localCluster);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
        }


	/*
	 * test_CopyFilesToEz
	 *
	 * Copy the HTF test data from non-EZ hdfs path to an EZ, files range from 
	 * 0 byte to 11GB
	*/
        private void test_CopyFilesToEz(String protocol) throws Exception {

		String completePathOfSource = "/HTF/testdata";

            	setupTest(protocol, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// copy all data from /HTF/testdata/dfs to the EZ path
                genericCliResponse = dfsCliCommands.cp(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfSource, 
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// list EZ path again, should have the test data in place now
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                TestSession.logger.info("Finished test_CopyFilesToEz");

        }

        /*
         * test_GetEzFileMetadata
         *
         * Get the EZ metadata for a file in an hdfs EZ
	 *
	 * This uses file 'file_256MB' copied from test case test_CopyFilesToEz
         * 
        */
        private void test_GetEzFileMetadata(String protocol) throws Exception {

            	setupTest(protocol, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2);

		String completePathOfSource = "/HTF/testdata";
                String completePathOfEzFile1 = TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2 +
			"testdata/dfs/file_256MB";

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

                TestSession.logger.info("File in EZ that we are fetching metadata for: " + 
			completePathOfEzFile1);

                // copy all data from /HTF/testdata/dfs to the EZ path
                genericCliResponse = dfsCliCommands.cp(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfSource, 
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // get the file's EZ metadata info 
                genericCliResponse = dfsCliCommands.getFileEncryptionInfo(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                               	completePathOfEzFile1); 
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                TestSession.logger.info("Finished test_GetEzFileMetadata");

        }


        /*
         * test_CopyFilesFromEz
         *
         * Copy the HTF test data from EZ path to a non-EZ path, files range from
         * 0 byte to 11GB
        */
        private void test_CopyFilesFromEz(String protocol) throws Exception {

                String completePathOfDest = "/tmp/testdata_from_ez";

		// setup our source ez, this is important since junit will run tests
		// in parallel, with random launching we can't be sure another 
		// EZ is available yet
                setupTest(protocol, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR3);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;


                // delete the dest path
                genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
                                completePathOfDest);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// create the dest path
                genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfDest);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// list the dest, should be empty
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfDest, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // copy all data from EZ to completePathOfDest 
                genericCliResponse = dfsCliCommands.cp(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR3,
                                completePathOfDest);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // list EZ path again, should have the test data still in place now
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR3, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // list the dest, should have HTF testdata 
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfDest, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                TestSession.logger.info("Finished test_CopyFilesFromEz");
        }


        /*
         * test_CopyFilesToEzFromLocal
         *
         * Copy the HTF test data from local GW path to an EZ, files range from
         * 0 byte to 11GB
        */
        private void test_CopyFilesToEzFromLocal(String protocol) throws Exception {
                
                String completePathOfLocalSource = "/grid/0/tmp/HTF/testdata";
                
                setupTest(protocol, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR4);
                
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR4, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
                
                // copy all data from local GW to the EZ path
                genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfLocalSource, 
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR4);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
                
                // list EZ path again, should have the test data in place now
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR4, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
                
                TestSession.logger.info("Finished test_CopyFilesToEzFromLocal");
        
        }

        /*
         * test_RunYarn_RW_Sort_UsingEz
         *
	 * Run a randomwriter job using EZ path to output data, then run a sort job using
	 * the randomwriter job's output as input data
	 *
        */
        private void test_RunYarn_RW_Sort_UsingEz(String protocol) throws Exception {

                String completePathOfLocalSource = "/grid/0/tmp/HTF/testdata";
		String randomWriterBase = "/user/hadoop3/KmsEzDfsTest/"; 
		String randomWriterOutDir = randomWriterBase + "rw_job1"; 
		String sortJobOutputDir = randomWriterBase + "sort_job1"; 

		setupTest(protocol, TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR5);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

                // chmod /user/hadoop3 to allow hadoopqa to rm it's job output paths 
                genericCliResponse = dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                "/user/hadoop3", "777", Recursive.NO);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                HashMap<String, String> jobParams = new HashMap<String, String>();
                jobParams.put("mapreduce.randomwriter.bytespermap", "512000");

                // delete the randomWriterBase in case job was run previously 
                genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
                                Recursive.YES, Force.YES, SkipTrash.YES,
                                randomWriterBase);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// create job's output base path as an EZ
                setupTest(protocol, randomWriterBase);

                // chmod to allow hadoopqa to write the output data in EZ
                genericCliResponse = dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR5, "777", Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // copy all data from local GW to the EZ path
                genericCliResponse = dfsCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                completePathOfLocalSource,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR5);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // chmod to allow other users to read the src data from EZ
		// NOTE: get perm denied for .Trash folder because it's owned by hdfsqa, so
		// can't chmod it but can remove it (folder is 777 with sticky bit)
                genericCliResponse = dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR5 + "testdata", "755", Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// chmod to allow other users to read the job output data in EZ
                genericCliResponse = dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
                                randomWriterBase, "777", Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);


		// setup objects and run the Yarn randomwriter job
                YarnTestsBaseClass yarnTestBaseClass = new YarnTestsBaseClass();
                yarnTestBaseClass.runStdHadoopRandomWriter(jobParams, randomWriterOutDir);

		// setup and run sort job using randomwriter job's result as input
                yarnTestBaseClass.runStdHadoopSortJob(randomWriterOutputDir + "/*",
                                sortJobOutputDir);

	}

}




