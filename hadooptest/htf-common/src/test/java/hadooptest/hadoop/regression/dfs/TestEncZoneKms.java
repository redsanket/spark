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

import java.net.InetAddress;


/* 
 *  TestEncZoneKms.java
 *    
 *  This class uses KMS to create a basic Encryption Zone in hdfs at '/tmp/ez_hadoop3',
 *  owned by user 'hadoop3', and performs basic r/w operations in the EZ. 
 *
*/

public class TestEncZoneKms extends DfsTestsBaseClass {
        String protocol;

        private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR = "/user/hadoop3/FilesInEz/";
        private static String PROPERTY = "FilesInGetListingOps";
        private static String SERVICE = "Hadoop:name=NameNodeActivity,service=NameNode";

        private String namenodeHostname;
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


public class TestEncZoneKms {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	public static String KRB5CCNAME = "KRB5CCNAME";

	public TestEncZoneKms() {
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties.load(new FileInputStream(
			        HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

	}


	@Test public void test_FilesInEz1_hdfs() throws Exception { test_FilesInEz1(HadooptestConstants.Schema.HDFS); }


        private void setupTest(String protocol) throws Exception {
        this.protocol = protocol;
        logger.info("Test invoked for protocol/schema:" + protocol);

                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;

/*
                // Stop the History server
                FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
                cluster.hadoopDaemon(Action.STOP,
                                HadooptestConstants.NodeTypes.HISTORY_SERVER);
*/

                // Delete dir if already exists
                genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                Recursive.YES, Force.YES, SkipTrash.YES,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR);

                Util.sleep(10);

                // create user base hdfs path /tmp/ez_hadoop3/FilesInEz/
                genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

                // list base hdfs path /tmp/ez_hadoop3/FilesInEz/
                genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HADOOP3, protocol, localCluster,
                                TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_BASE_DIR);
                Assert.assertTrue(genericCliResponse.process.exitValue() == 0);


}
