package hadooptest.hadoop.regression.yarn.jvmParam478;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestJvmParam478 extends YarnTestsBaseClass {

	// @Before
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

	void updateRMConfigAndBounceIt(HashMap<String, String> propertiesMap,
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

		Thread.sleep(5000);

	}

	void updateGWConfig(HashMap<String, String> propertiesMap,
			String fileToModify) throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.GATEWAY)
				.backupConfDir();

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

	}

	private String setupAndFireHadoopStreamingJob(String files,
			String mapCommand, String reduceCmd, String inputFile,
			HashMap<String, String> args) throws Exception {
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
		sb.append(" -cmdenv \"mapreduce.job.acl-view-job=*\""); // replace
																// cmdenv with
																// jobconf
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

		String envSettings = setupAndFireHadoopStreamingJob(files, mapCommand,
				null, inputFile, args);
		return envSettings;

	}

	private String runEnvReducerAndReturnEnvSettings() throws Exception {
		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.ubertask.enable", "false");
		args.put("mapred.map.tasks", "1");
		args.put("mapred.reduce.tasks", "1");
		String mapCommand = "cat";
		String reduceCommand = "env.sh";
		String files = "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression/yarn/jvmParam478/env.sh\"";
		String inputFile = TestSession.conf.getProperty("WORKSPACE")
				+ "/"
				+ "htf-common/resources/hadooptest/hadoop/regression/yarn/jvmParam478/dummy_input";

		String envSettings = setupAndFireHadoopStreamingJob(files, mapCommand,
				reduceCommand, inputFile, args);
		return envSettings;

	}

	@Test
	@Ignore("")
	public void test1() throws Exception {
		HashMap<String, String> rmSettingsMap = new HashMap<String, String>();
		rmSettingsMap.put("mapred.child.env", "childenv1=childenv");
		rmSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		rmSettingsMap.put("mapred.child.ulimit", "8000007");
		// updateRMConfigAndBounceIt(rmSettingsMap, "mapred-site.xml");
		updateGWConfig(rmSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings();
		Assert.assertTrue(
				"mapred_child_env=childenv1=childenv missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_env=childenv1=childenv"));
		Assert.assertTrue(
				"mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000007 missing from Mapper env setting",
				obtainedEnvSettings.contains("mapred_child_ulimit=8000007"));

	}

	@Test
	@Ignore("")
	public void test2() throws Exception {
		HashMap<String, String> rmSettingsMap = new HashMap<String, String>();
		rmSettingsMap.put("mapred.child.env", "childenv1=childenv");
		rmSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		rmSettingsMap.put("mapred.child.ulimit", "8000007");
		// updateRMConfigAndBounceIt(rmSettingsMap, "mapred-site.xml");
		updateGWConfig(rmSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvReducerAndReturnEnvSettings();
		Assert.assertTrue(
				"mapred_child_env=childenv1=childenv missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_env=childenv1=childenv"));
		Assert.assertTrue(
				"mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000007 missing from Mapper env setting",
				obtainedEnvSettings.contains("mapred_child_ulimit=8000007"));

	}

	@Test
	@Ignore("")
	public void test3() throws Exception {
		HashMap<String, String> rmSettingsMap = new HashMap<String, String>();
		rmSettingsMap.put("mapred.child.ulimit", "invalid");
		rmSettingsMap.put("mapreduce.map.ulimit", "invalid");
		// updateRMConfigAndBounceIt(rmSettingsMap, "mapred-site.xml");
		updateGWConfig(rmSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings();
		Assert.assertTrue(
				"mapred_child_env=childenv1=childenv [should not be present] in "
						+ "Mapper env setting", !obtainedEnvSettings
						.contains("mapred_child_env=childenv1=childenv"));
		Assert.assertTrue(
				"mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true "
						+ "[should not be present] in Mapper env setting",
				!obtainedEnvSettings
						.contains("mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000007 [should not be present] in Mapper env setting",
				!obtainedEnvSettings.contains("mapred_child_ulimit=8000007"));

	}

	@Test
	public void test4() throws Exception {
		HashMap<String, String> rmSettingsMap = new HashMap<String, String>();
		rmSettingsMap.put("mapred.child.ulimit", "invalid");
		rmSettingsMap.put("mapreduce.map.ulimit", "invalid");
		// updateRMConfigAndBounceIt(rmSettingsMap, "mapred-site.xml");
		updateGWConfig(rmSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvReducerAndReturnEnvSettings();
		Assert.assertTrue(
				"mapred_child_env=childenv1=childenv [should not be present] in "
						+ "Mapper env setting", !obtainedEnvSettings
						.contains("mapred_child_env=childenv1=childenv"));
		Assert.assertTrue(
				"mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true "
						+ "[should not be present] in Mapper env setting",
				!obtainedEnvSettings
						.contains("mapred_child_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000007 [should not be present] in Mapper env setting",
				!obtainedEnvSettings.contains("mapred_child_ulimit=8000007"));

	}

	void assertConfigSettings(HashMap<String, String> sentRMSettingsMap,
			String obtainedEnvSettings) {
		for (String key : sentRMSettingsMap.keySet()) {
			String keyWithDotsReplacedWithUnderscores = key.replace(".", "_");

		}
	}

}
