package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;


/* 
 *  TestEncZoneKms.java
 *    
 *  This class uses KMS/EZ support methods in DfsCliCommands to create a basic Encryption Zone in hdfs 
 *  at '/tmp/ez_hadoop3', the path is created and owned by user 'hadoop3', EZ is created by hdfsqa, and 
 *  then performs basic r/w operations in the EZ as hadoop3. 
 *
 *  NOTES:
 *  1. KMS/EZ does not currently support webhdfs protocol
 *
*/

public class TestEncZoneKms extends DfsTestsBaseClass {
        String protocol;

        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1 = "/tmp/hadoop3/BaseDirInEz1/";
        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2 = "/tmp/hadoop3/BaseDirInEz2/";
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


	@Test public void test_FilesInEz1_none() throws Exception { test_FilesInEz1(""); }
	@Test public void test_FilesInEz2_hdfs() throws Exception { test_FilesInEz2(HadooptestConstants.Schema.HDFS); }


	String kmsKeyToUseForEzCreate = "FlubberKmsKey1";

        private void setupTest(String protocol) throws Exception {
        	this.protocol = protocol;
        	logger.info("Test invoked for protocol/schema:" + protocol);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;


                // Delete dir if already exists
                genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                Recursive.YES, Force.YES, SkipTrash.YES,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1);

                Util.sleep(10);

                // create user base hdfs path /tmp/ez_hadoop3/FilesInEz/
                genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // list base hdfs path /tmp/ez_hadoop3/FilesInEz/
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1, Recursive.YES);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// as hdfsqa, create encryption zone
		genericCliResponse = dfsCliCommands.createZone(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR1, kmsKeyToUseForEzCreate);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
        }


        private void test_FilesInEz1(String protocol) throws Exception {
            	setupTest(protocol);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2, Recursive.NO);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                TestSession.logger.info("Finished test test_FilesInEz1");

        }

        private void test_FilesInEz2(String protocol) throws Exception {
                setupTest(protocol);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR2, Recursive.NO);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                TestSession.logger.info("Finished test test_FilesInEz2");

        }


}
