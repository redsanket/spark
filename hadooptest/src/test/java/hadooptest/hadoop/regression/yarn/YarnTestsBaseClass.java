package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;

public class YarnTestsBaseClass extends TestSession {
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public final String KRB5CCNAME = "KRB5CCNAME";
	protected String localCluster = System.getProperty("CLUSTER_NAME");
	
	public static enum YarnAdminSubCommand {
		REFRESH_QUEUES, 
		REFRESH_NODES,
		REFRESH_SUPERUSER_GROUPS_CONFIGURATION,
		REFRESH_USER_TO_GROUPS_MAPPING,
		REFRESH_ADMIN_ACLS,
		REFRESH_SERVICE_ACL,
		GET_GROUPS
	};

	/*
	 * Run a Sleep Job, from org.apache.hadoop.mapreduce
	 */
	public void runStdSleepJob(HashMap<String, String> jobParams, String[] args)
			throws Exception {
		TestSession.logger.info("running SleepJob.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new SleepJob(), args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			TestSession.logger.error("SleepJob barfed...");
			throw e;
		}

	}

	/*
	 * Run a RandomWriter Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopRandomWriter(HashMap<String, String> jobParams,
			String randomWriterOutputDirOnHdfs) throws Exception {
		TestSession.logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new RandomWriter(),
					new String[] { randomWriterOutputDirOnHdfs });
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		TestSession.logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res ;
		
		try{
			res= ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {
		
				sortInputDataLocation, sortOutputDataLocation });
		Assert.assertEquals(0, res);
		} catch (Exception e){
			throw e;
		}

	}

}
