package hadooptest.tez.mapreduce.examples.cluster;


import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.tez.mapreduce.examples.RandomTextWriter;
import org.junit.After;
import org.junit.Test;

public class TestRandomTextWriterOnTez extends TestSession {
	/**
	 * This test class is there to check for backward compatibility, to ensure that 
	 * legacy MR jobs continue to run on Tez, with the framework set to yarn-tez
	 *
	 * This program uses map/reduce to just run a distributed job where there is
	 * no interaction between the tasks and each task writes a large unsorted
	 * random sequence of words.
	 * In order for this program to generate data for terasort with a 5-10 words
	 * per key and 20-100 words per value, have the following config:
	 * <xmp>
	 * <?xml version="1.0"?>
	 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	 * <configuration>
	 *   <property>
	 *     <name>mapreduce.randomtextwriter.minwordskey</name>
	 *     <value>5</value>
	 *   </property>
	 *   <property>
	 *     <name>mapreduce.randomtextwriter.maxwordskey</name>
	 *     <value>10</value>
	 *   </property>
	 *   <property>
	 *     <name>mapreduce.randomtextwriter.minwordsvalue</name>
	 *     <value>20</value>
	 *   </property>
	 *   <property>
	 *     <name>mapreduce.randomtextwriter.maxwordsvalue</name>
	 *     <value>100</value>
	 *   </property>
	 *   <property>
	 *     <name>mapreduce.randomtextwriter.totalbytes</name>
	 *     <value>1099511627776</value>
	 *   </property>
	 * </configuration></xmp>
	 * 
	 * Equivalently, {@link RandomTextWriter} also supports all the above options
	 * and ones supported by {@link Tool} via the command-line.
	 * 
	 * To run: bin/hadoop jar hadoop-${version}-examples.jar randomtextwriter
	 *            [-outFormat <i>output format class</i>] <i>output</i> 
	 */
	public static String OUT_DIR = "/tmp/randomTextWriter/tez/out/";	
	@Test
	public void testRandonmWriter() throws Exception{
		RandomTextWriter randomTextWriter = new RandomTextWriter();
		Configuration conf = HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO, "n/a");
		conf.setInt("mapreduce.randomtextwriter.totalbytes", 10240);
		conf.setInt(MRJobConfig.NUM_MAPS, 2);
		
		randomTextWriter.setConf(conf);
		randomTextWriter.run(new String[]{OUT_DIR});
		
	}
	
	@After
	public void deleteCreatedDir() throws Exception{
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR);

	}
}
