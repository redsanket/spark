package hadooptest.yarn.regression;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.dfs.DFS;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMR1178 extends TestSession {

	private static DFS hdfs = new DFS();
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}
	
	@Test
	public void testJobWithMultipleInputFiles() throws Exception {

		String nameNodeAddress = hdfs.getBaseUrl();
		
		//TODO: Get the path for the directory
		//Delete the Existing HDFS Directory (if exists)
		//And create a new directory
		String dirPath = "MR1178";
		//File dirName = new File(dirPath);
		//FileUtils.deleteDirectory(dirName);
		
		logger.info("Deleting existing Directory \"" + dirPath +
				"\" and creating new directory of same name");
		
		hdfs.mkdir(dirPath);
		

		//Create the HDFS Directory
		//FileUtils.forceMkdir(dirName);

		logger.info("STARTING MAPREDUCE:1178 (MultipleInputs fails" + 
				" with ClassCastException) TEST SUITE");

		dirPath += dirPath + "/MR1178_MultipleInputs";
		//dirName = new File(dirPath);
		//FileUtils.forceMkdir(dirName);
		hdfs.mkdir(dirPath);
		
		String randomWriterOutput = dirPath + "/randomWriterOutput";
		
		//Run RandomWriter
		int result = ToolRunner.run(TestSession.cluster.getConf(), new RandomWriter(),
				new String[] { randomWriterOutput });
		Assert.assertEquals(0, result);
		
		String wordcountOutput = dirPath + "/wordcountOutput";
		
		//Run WordCount
		result = ToolRunner.run(TestSession.cluster.getConf(),
				(Tool) new WordCount(),
				new String[] { randomWriterOutput, wordcountOutput});
		Assert.assertEquals(0, result);
		
	}
	
}
