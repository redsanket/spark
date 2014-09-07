package hadooptest.tez.mapreduce.examples.localmode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.mapreduce.examples.RandomWriter;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import hadooptest.TestSession;
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
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.randomwriter.totalbytes", 1024);
		conf.setInt(MRJobConfig.NUM_MAPS, 1);
		
		randomWriter.setConf(conf);
		randomWriter.run(new String[]{OUT_DIR});
		
	}
	
	@After
	public static void deleteCreatedDir() throws IOException{
		HtfTezUtils.delete(new File(OUT_DIR));
	}
}
