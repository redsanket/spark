package hadooptest.hadoop.regression.yarn.jvmParam478;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestJvmParam478 extends YarnTestsBaseClass {

//	 @Before
	public void before() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		// Bounce the RM
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.resetHadoopConfDir();
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		// Bounce the NM
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.NODE_MANAGER)
				.resetHadoopConfDir();
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NODE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NODE_MANAGER);
	}

	void updateRMConfigConfigAndBounceIt(HashMap<String, String> propertiesMap,
			String fileToModify) throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();

		String dirWhereRMConfHasBeenCopiedOnTheRemoteMachine = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.getHadoopConfDir();

		TestSession.logger.info("Backed up RM conf dir in :"
				+ dirWhereRMConfHasBeenCopiedOnTheRemoteMachine);

		for (String key : propertiesMap.keySet()) {
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.setHadoopConfFileProp(key, propertiesMap.get(key),
							fileToModify, null);
		}

		// Bounce RM
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		

		/**
		 * 
		 */
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.GATEWAY).backupConfDir();

		String dirWhereGWConfHasBeenCopiedOnTheLocalMachine = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.GATEWAY)
				.getHadoopConfDir();

		TestSession.logger.info("Backed up GW conf dir in :"
				+ dirWhereGWConfHasBeenCopiedOnTheLocalMachine);

		for (String key : propertiesMap.keySet()) {
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.GATEWAY)
					.setHadoopConfFileProp(key, propertiesMap.get(key),
							fileToModify, null);
		}

		/**
		 * 
		 */

		Thread.sleep(5000);

	}

	private String setupAndFireHadoopStreamingJob(String files, String mapCommand,
			String reduceCmd, String inputFile, HashMap<String, String> args)
			throws Exception {
		String fileOnHdfs = "input-testJvmParam" + System.currentTimeMillis();
		String outputPath = "output-testJvmParam" + System.currentTimeMillis();

		StringBuilder sb = new StringBuilder();

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericResponseBO = dfsCliCommands.put(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), inputFile, fileOnHdfs);
		Assert.assertTrue(genericResponseBO.process.exitValue() == 0);

		sb.append("-input " + fileOnHdfs);
		sb.append(" -output " + outputPath);
		sb.append(" -mapper " + mapCommand);
		if (reduceCmd != null)
			sb.append(" -reducer " + reduceCmd);
		else
			sb.append(" -reducer NONE");

		for (String key : args.keySet()) {
			sb.append(" -cmdenv \"" + key + "=" + args.get(key) + "\"");
		}
		sb.append(" -cmdenv \"mapreduce.job.acl-view-job=*\""); // replace cmdenv with jobconf
		sb.append(" -file " + files);

		TestSession.logger
				.info("Executing command with args-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outputPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);
		return genericCliResponseBO.response;

	}

	private String runEnvMapperAndReturnEnvSettings() throws Exception {
		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.ubertask.enable", "false");
		args.put("mapred.map.tasks", "1");
		args.put("mapred.reduce.tasks", "1");
		String mapCommand = "env.sh";
		String files = "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression/yarn/jvmParam478/env.sh\"";
		String inputFile = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression/yarn/jvmParam478/dummy_input";

		String envSettings = setupAndFireHadoopStreamingJob(files, mapCommand, null, inputFile, args);
		return envSettings;

	}

	@Test
	public void test() throws Exception {
		HashMap<String, String> rmSettingsMap = new HashMap<String, String>();
		rmSettingsMap.put("mapred.child.env", "childenv1=childenv");
		rmSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		rmSettingsMap.put("mapred.child.ulimit", "8000007");
		updateRMConfigConfigAndBounceIt(rmSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings();
		
	}

	void assertConfigSettings(HashMap<String, String> sentRMSettingsMap, String obtainedEnvSettings){
		for (String key:sentRMSettingsMap.keySet()){
			String keyWithDotsReplacedWithUnderscores = key.replace(".", "_");
			
		}
	}


}
