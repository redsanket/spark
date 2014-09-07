package hadooptest.tez.mapreduce.examples.cluster;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.mapreduce.examples.RandomWriter;
import org.junit.After;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.utils.HtfTezUtils;

public class TestRandomWriterOnTez extends TestSession {
	
	/**
	 *  * <configuration>
 *   <property>
 *     <name>mapreduce.randomwriter.minkey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.maxkey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.minvalue</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.maxvalue</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomwriter.totalbytes</name>
 *     <value>1099511627776</value>
 *   </property>
 * </configuration></xmp>
	 */
	public static String OUT_DIR = "/tmp/randomWriter/tez/out/";	
	@Test
	public void testRandonWriter() throws Exception{
		RandomWriter randomWriter = new RandomWriter();
		Configuration conf = HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), HadooptestConstants.Execution.TEZ_CLUSTER, false, "n/a");
		conf.setInt("mapreduce.randomwriter.totalbytes", 1024);
		conf.setInt(MRJobConfig.NUM_MAPS, 1);
		
		randomWriter.setConf(conf);
		randomWriter.run(new String[]{OUT_DIR});
		
	}
	
//	@After
	public void deleteCreatedDir() throws Exception{
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUT_DIR);

	}
}
