package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.mapreduce.examples.Join;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This test class is there to check for backward compatibility, to ensure that
 * legacy MR jobs continue to run on Tez, with the framework set to yarn-tez
 * 
 */
@Category(SerialTests.class)
public class TestJoinOnTez extends TestSession {
	private static String CANNED_DATA1 = "/home/y/share/htf-data/pig_methods_dataset1";
	private static String CANNED_DATA1_ON_HDFS = "/tmp/pig_methods_dataset1";
	private static String CANNED_DATA2 = "/home/y/share/htf-data/pig_methods_dataset2";
	private static String CANNED_DATA2_ON_HDFS = "/tmp/pig_methods_dataset2";
	private static String OUT_DIR_INNER_JOIN = "/tmp/join/tez/out/inner";
	private static String OUT_DIR_OUTER_JOIN = "/tmp/join/tez/out/outer";
	private static String OUT_DIR_OVERRIDE_JOIN = "/tmp/join/tez/out/override";
	private static String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
	private static String TEXT_OUTPUT_FORMAT = "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
	private static String LONG_WRITABLE = "org.apache.hadoop.io.LongWritable";
	private static String TUPLE_WRITABLE = "org.apache.hadoop.mapreduce.lib.join.TupleWritable";

	@Before
	public void copyDataToHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.put(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), CANNED_DATA1,
				CANNED_DATA1_ON_HDFS);
		genericCliResponse.process.waitFor();
		genericCliResponse = dfsCliCommands.put(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), CANNED_DATA2,
				CANNED_DATA2_ON_HDFS);
		genericCliResponse.process.waitFor();
		dfsCliCommands.chmod(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"),
				CANNED_DATA1_ON_HDFS, "777", Recursive.NO);
		dfsCliCommands.chmod(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"),
				CANNED_DATA2_ON_HDFS, "777", Recursive.NO);



	}

	@Test
	public void testInnerJoinWithSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_INNER_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED, "n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	@Test
	public void testInnerJoinWithoutSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_INNER_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED,"n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	@Test
	public void testOuterJoinWithSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_OUTER_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED, "n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	@Test
	public void testOuterJoinWithoutSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_OUTER_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED, "n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	@Test
	public void testOverrideJoinWithSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_OVERRIDE_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.YES, TimelineServer.ENABLED, "n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	@Test
	public void testOverrideJoinWithoutSession() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("-r" + " ");
		sb.append("1" + " ");
		sb.append("-inFormat" + " ");
		sb.append(TEXT_INPUT_FORMAT + " ");
		sb.append("-outFormat" + " ");
		sb.append(TEXT_OUTPUT_FORMAT + " ");
		sb.append("-outKey" + " ");
		sb.append(LONG_WRITABLE + " ");
		sb.append("-outValue" + " ");
		sb.append(TUPLE_WRITABLE + " ");
		sb.append("-joinOp" + " ");
		sb.append("inner" + " ");
		sb.append(CANNED_DATA1_ON_HDFS + " ");
		sb.append(CANNED_DATA2_ON_HDFS + " ");
		sb.append(OUT_DIR_OVERRIDE_JOIN + " ");
		String argsString = sb.toString();
		String[] args = argsString.split("\\s+");
		printArgs(args);
		Configuration conf = HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, TimelineServer.ENABLED, "n/a");
		int res = ToolRunner.run(conf, new Join(), args);
		Assert.assertEquals(res, 0);
	}

	private void printArgs(String[] args){
		TestSession.logger.info("Running with args:");
		for (String anArg:args){
			TestSession.logger.info(anArg + ",");
		}
	}
	@After
	public void deleteCreatedDir() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		
		genericCliResponse = dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR_INNER_JOIN);
		genericCliResponse.process.waitFor();
		genericCliResponse = dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR_OUTER_JOIN);
		genericCliResponse.process.waitFor();
		genericCliResponse = dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR_OVERRIDE_JOIN);
		genericCliResponse.process.waitFor();


	}
	

	public void logTaskReportSummary(){
		//Override
	}

}
