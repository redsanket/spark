package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.workflow.hadoop.job.WordCountJob;

import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMR1178 extends TestSession {
	
	public static final String BYTES_PER_MAP =
			"mapreduce.randomwriter.bytespermap";

	private static DFS hdfs = new DFS();
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}
	
	@Test
	public void testJobWithMultipleInputFiles() throws Exception {
		
		//Delete the Existing HDFS Directory (if exists)
		//And create a new directory
		String dirPath = "/tmp/MR1178";
		
		logger.info("Deleting existing Directory \"" + dirPath +
				"\" (if exists) and creating new directory of same name");
		
		hdfs.mkdir(dirPath);

		logger.info("STARTING MAPREDUCE:1178");

		dirPath += "/MR1178_MultipleInputs";
		hdfs.mkdir(dirPath);
		
		HadoopConfiguration conf = cluster.getConf();
		conf.setLong(BYTES_PER_MAP, 256000);
		
		String randomWriterOutput = dirPath + "/randomWriterOutput";
		
		//Run RandomWriter
		
		/*
		 * ToolRunner will wait go into a blocking state when the job is running
		 * It will return 0 on successful job completion
		 * It will return a non-zero number on failure
		*/
		
		int result = ToolRunner.run(conf, new RandomWriter(),
				new String[] { randomWriterOutput });
		Assert.assertEquals(0, result);
		
		String wordcountOutput = dirPath + "/wordcountOutput";
		
		//Run WordCount
		
		/*
		 * WordCount can be run in the same way as RandomWriter Above
		 * Only difference will be, the 3rd argument will have two
		 * inputs - 1st for InputDir, 2nd for outDir
		 * 
		 * result = ToolRunner.run(conf, new WordCount(),
		 * 		new String[] { randomWriterOutput, wordcountOutput });
		 * Assert.assertEquals(0, result);
		 * 
		 * You will need to import org.apache.hadoop.mapred.WordCount;
		*/
		
		WordCountJob wcJob = new WordCountJob();
		
		wcJob.setInputFile(randomWriterOutput);
		wcJob.setOutputPath(wordcountOutput);

		wcJob.start();

		assertTrue("WordCount job was not assigned an ID within 10 seconds.", 
				wcJob.waitForID(10));
		assertTrue("WordCount job ID for WordCount job is invalid.", 
				wcJob.verifyID());

		int waitTime = 3;
		assertTrue("WordCount Job did not succeed.",
				wcJob.waitForSuccess(waitTime));
		
	}
}
