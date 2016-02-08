package hadooptest.hadoop.regression.yarn.jvmParam478;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * Porting JvmParams478 suite into HTF
 * @author tiwari
 *
 */
@Category(SerialTests.class)
public class TestJvmParam478 extends YarnTestsBaseClass {

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
			sb.append(" -jobconf \"" + key + "=" + args.get(key) + "\"");
		}
		// Alt: replace cmdenv with jobconf
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\""); 
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

	private String runEnvMapperAndReturnEnvSettings(
			Map<String, String> commandLineOverrides) throws Exception {
		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.ubertask.enable", "false");
		args.put("mapred.map.tasks", "1");
		args.put("mapred.reduce.tasks", "1");
		for (String key : commandLineOverrides.keySet()) {
			args.put(key, commandLineOverrides.get(key));
		}
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

	private String runEnvReducerAndReturnEnvSettings(
			Map<String, String> commandLineOverrides) throws Exception {
		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.ubertask.enable", "false");
		args.put("mapred.map.tasks", "1");
		args.put("mapred.reduce.tasks", "1");
		for (String key : commandLineOverrides.keySet()) {
			args.put(key, commandLineOverrides.get(key));
		}

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

	/**
	 * First, verify that the xml values are picked up in the mapper for the deprecated settings.
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.env", "childenv1=childenv");
		gwSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapred.child.ulimit", "8000007");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
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
	/**
	 * First, verify that the xml values are picked up in thereducerfor the deprecated settings.
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.env", "childenv1=childenv");
		gwSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapred.child.ulimit", "8000007");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
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

	/**
	 * Verify that invalid settings actually cause the jobs to fail:
	 * Set an invalid java.opts and verify that the job fails (meaning it picked up that setting)
	 * @throws Exception
	 */
	@Test
	@Ignore("Existing shell test fails.")
	public void test3() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.GATEWAY).resetHadoopConfDir();

		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.ulimit", "invalid");
		gwSettingsMap.put("mapreduce.map.ulimit", "invalid");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings();
		Assert.assertTrue(
				"mapred_child_env=invalid [should not be present] in "
						+ "Mapper env setting", !obtainedEnvSettings
						.contains("mapred_child_env=invalid"));
		Assert.assertTrue(
				"mapreduce_map_ulimit=invalid [should not be present] in Mapper env setting",
				!obtainedEnvSettings.contains("mapreduce_map_ulimit=invalid"));

	}

	/**
	 * now run for recuder
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
	    FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession.getCluster();
	    fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.GATEWAY).resetHadoopConfDir();
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.ulimit", "invalid");
		gwSettingsMap.put("mapreduce.map.ulimit", "invalid");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
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

	/**
	 * At this point, the java.opts and ulimit values are still invalid and will 
	cause job failure. Now, pass in these settings on the command-line and verify
	that they are picked up and not the xml settings. To do this, set invalid 
	settings for the java.opts and ulimit, and then pass in valid ones on the
	command-line. The job will fail unless the command-line settings are picked up.
	The $settings_hash{'old.env'} settings can be checked through the environment.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.env", "childenv1=childenv");
		gwSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapred.child.ulimit", "8000007");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
		Map<String, String> commandLineOverrides = new HashMap<String, String>();
		commandLineOverrides.put("mapred.child.env",
				"childenv1=commandlinechildenv");
		commandLineOverrides.put("mapred.child.ulimit", "8000008");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings(commandLineOverrides);
		Assert.assertTrue(
				"mapred_child_env=childenv1=commandlinechildenv missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_env=childenv1=commandlinechildenv"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000008 missing from Mapper env setting",
				obtainedEnvSettings.contains("mapred_child_ulimit=8000008"));

	}

	/**
	 * now, for recuder
	 * 
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		gwSettingsMap.put("mapred.child.env", "childenv1=childenv");
		gwSettingsMap.put("mapred.child.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapred.child.ulimit", "8000007");
		updateGWConfig(gwSettingsMap, "mapred-site.xml");
		Map<String, String> commandLineOverrides = new HashMap<String, String>();
		commandLineOverrides.put("mapred.child.env",
				"childenv1=commandlinechildenv");
		commandLineOverrides.put("mapred.child.ulimit", "8000008");
		String obtainedEnvSettings = runEnvReducerAndReturnEnvSettings(commandLineOverrides);
		Assert.assertTrue(
				"mapred_child_env=childenv1=commandlinechildenv missing from Mapper env setting",
				obtainedEnvSettings
						.contains("mapred_child_env=childenv1=commandlinechildenv"));
		Assert.assertTrue(
				"mapred_child_ulimit=8000008 missing from Mapper env setting",
				obtainedEnvSettings.contains("mapred_child_ulimit=8000008"));

	}

	@Test
	public void test7() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();

		gwSettingsMap.put("mapreduce.map.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapreduce.map.env", "childenv3=mapchildvalue");
		gwSettingsMap.put("mapreduce.map.ulimit", "8000800");
		// reducer
		gwSettingsMap.put("mapreduce.reduce.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapreduce.reduce.env", "childenv3=redchildvalue");
		gwSettingsMap.put("mapreduce.reduce.ulimit", "8000801");

		updateGWConfig(gwSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings();
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_map_env=childenv3=mapchildvalue"));
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_map_ulimit=8000800"));

		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_map_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
	}

	/**
	 * reducer
	 * @throws Exception
	 */
	@Test
	public void test8() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();

		gwSettingsMap.put("mapreduce.map.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapreduce.map.env", "childenv3=mapchildvalue");
		gwSettingsMap.put("mapreduce.map.ulimit", "8000800");
		// reducer
		gwSettingsMap.put("mapreduce.reduce.java.opts",
				"-server -Xmx1156m -Djava.net.preferIPv4Stack=true");//
		gwSettingsMap.put("mapreduce.reduce.env", "childenv3=redchildvalue");
		gwSettingsMap.put("mapreduce.reduce.ulimit", "8000801");

		updateGWConfig(gwSettingsMap, "mapred-site.xml");
		String obtainedEnvSettings = runEnvReducerAndReturnEnvSettings();
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_reduce_env=childenv3=redchildvalue"));
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_reduce_ulimit=8000801"));

		Assert.assertTrue(obtainedEnvSettings
				.contains("mapreduce_reduce_java_opts=-server -Xmx1156m -Djava.net.preferIPv4Stack=true"));
	}

	/**
	 * mapper
	 * @throws Exception
	 */
	@Test
	public void test9() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		Map<String, String> commandLineOverrides = new HashMap<String, String>();
		commandLineOverrides.put("mapred.child.env", "childenv3=childenvvalue");
		commandLineOverrides.put("mapred.child.ulimit", "8000888");
		String obtainedEnvSettings = runEnvMapperAndReturnEnvSettings(commandLineOverrides);
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapred_child_env=childenv3=childenvvalue"));
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapred_child_ulimit=8000888"));

	}
	/**
	 * reducer
	 * @throws Exception
	 */
	@Test
	public void test10() throws Exception {
		HashMap<String, String> gwSettingsMap = new HashMap<String, String>();
		Map<String, String> commandLineOverrides = new HashMap<String, String>();
		commandLineOverrides.put("mapred.child.env", "childenv3=childenvvalue");
		commandLineOverrides.put("mapred.child.ulimit", "8000888");
		String obtainedEnvSettings = runEnvReducerAndReturnEnvSettings(commandLineOverrides);
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapred_child_env=childenv3=childenvvalue"));
		Assert.assertTrue(obtainedEnvSettings
				.contains("mapred_child_ulimit=8000888"));


	}


	void assertConfigSettings(HashMap<String, String> sentgwSettingsMap,
			String obtainedEnvSettings) {
		for (String key : sentgwSettingsMap.keySet()) {
			String keyWithDotsReplacedWithUnderscores = key.replace(".", "_");

		}
	}

}
