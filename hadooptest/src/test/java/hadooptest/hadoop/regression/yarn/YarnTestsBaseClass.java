package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;

import java.util.HashMap;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;

public class YarnTestsBaseClass {
	
	/*
	 * Run a Sleep Job, from org.apache.hadoop.mapreduce
	 */
	public void runStdSleepJob(HashMap<String, String> jobParams, String[] args){
		TestSession.logger.info("running SleepJob.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key:jobParams.keySet()){
			conf.set(key,  jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new SleepJob(),
					args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			TestSession.logger.error("SleepJob barfed...");
			e.printStackTrace();
		}		
		
	}
	
	/*
	 * Run a RandomWriter Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopRandomWriter(HashMap<String, String> jobParams, String randomWriterOutputDirOnHdfs)
			throws Exception {
		TestSession.logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key:jobParams.keySet()){
			conf.set(key,  jobParams.get(key));
		}
		int res = ToolRunner.run(conf, new RandomWriter(),
				new String[] { randomWriterOutputDirOnHdfs });
		Assert.assertEquals(0, res);

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		TestSession.logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res = ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {
				sortInputDataLocation, sortOutputDataLocation });
		Assert.assertEquals(0, res);

	}

}
