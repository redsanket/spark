package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.mapreduce.examples.RandomTextWriter;
import org.apache.tez.mapreduce.examples.MapredWordCount;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test class is there to check for backward compatibility, to ensure that
 * legacy MR jobs continue to run on Tez, with the framework set to yarn-tez
 * 
 */
public class TestMapredWordCountOnTez extends TestSession {
	public static String RANDOM_TEXT_WRITTEN_TO_HDFS_HERE = "/tmp/random/text/writted/to/hdfs/here";
	public static String OUT_DIR = "/tmp/mapredWordCount/tez/out/";

	@Before
	public void generateData() throws Exception {
		RandomTextWriter randomTextWriter = new RandomTextWriter();
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED, "n/a");
		conf.setInt("mapreduce.randomtextwriter.totalbytes", 10240);
		conf.setInt(MRJobConfig.NUM_MAPS, 2);

		randomTextWriter.setConf(conf);
		randomTextWriter.run(new String[] { RANDOM_TEXT_WRITTEN_TO_HDFS_HERE });

	}

	/**
	 * [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
	 */

	@Test
	public void testRandonmWriter() throws Exception {
		MapredWordCount mapredWordCount = new MapredWordCount();
		String[] args = new String[] { "-m", "2", "-r", "2",
				RANDOM_TEXT_WRITTEN_TO_HDFS_HERE, OUT_DIR };
		mapredWordCount.setConf(HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED, "n/a"));
		int returnCode = mapredWordCount.run(args);
		Assert.assertEquals(returnCode, 0);
	}

	@After
	public void deleteCreatedDir() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, RANDOM_TEXT_WRITTEN_TO_HDFS_HERE);
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR);

	}
}
