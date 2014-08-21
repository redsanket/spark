package hadooptest.hadoop.regression;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.SleepJobNNCheck;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

public class TestSleepJobWithNamenodeCheck extends TestSession {
	
	//private DFS dfs = new hadooptest.cluster.hadoop.dfs.DFS();
	DfsCliCommands dfsCommonCli = new DfsCliCommands();
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}
	@Before
	public void setup() throws FileNotFoundException, IOException {
		//create a file on the namenode
		DFS.createLocalFile("/tmp/testFileForNamenodeCheck");
	}

	@Test
	public void testNewSleepJob() throws Exception {
		dfsCommonCli.rm(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"),
				Recursive.NO,
				Force.NO,
				SkipTrash.NO,
				"/tmp/testFileForNamenodeCheck");
		
		GenericCliResponseBO cliResponse = dfsCommonCli.copyFromLocal(
				new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"),
				"/tmp/testFileForNamenodeCheck", "/tmp/");
		Assert.assertTrue("Cannot copy file from local to hdfs" ,cliResponse.process.exitValue()==0);

		dfsCommonCli.chmod(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"),
				"/tmp/testFileForNamenodeCheck", "777", Recursive.NO);

		TestSession.logger.info("Submit Sleep Job.");
		
		SleepJobNNCheck job = new SleepJobNNCheck();
		job.setNumMappers(
                Integer.parseInt(System.getProperty("SLEEP_JOB_NUM_MAPPER", "10")));
        job.setNumReducers(
                Integer.parseInt(System.getProperty("SLEEP_JOB_NUM_REDUCER", "10")));
        job.setMapDuration(
                Integer.parseInt(System.getProperty("SLEEP_JOB_MAP_SLEEP_TIME", "5000")));
        job.setReduceDuration(
                Integer.parseInt(System.getProperty("SLEEP_JOB_RED_SLEEP_TIME", "5000")));
        job.setUser("hadoopqa");
        job.start();
        
	}
}
