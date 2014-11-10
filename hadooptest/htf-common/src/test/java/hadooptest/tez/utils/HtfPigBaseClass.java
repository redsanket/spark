package hadooptest.tez.utils;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;

/**
 * Tez introduces a new concept of aligning the sequence of operations as a DAG.
 * Since in reality folks do not create DAG code and execute them, instead DAGs
 * are generated as a part of running Pig/Hive scripts. This section of the code
 * provides Util methods for Pig code that implicitly generates DAGs.
 * 
 * This class contains common methods that are required to be run by most of the
 * Pig scripts (testing Tez). Common operations such as ensuring data is present
 * in the cluster before starting a test go here. Here is how the Pig scripts
 * have been organized. Since the mandate is to test Tez in 'local mode' and
 * 'cluster mode' data/scripts that Pig tests use come from two places [a]
 * 'htf_pig_data' yinst package (predominantly meant to be run on local mode and
 * [b] from within HTF, from htf-common/resources/hadooptest/pig/scripts/*
 * predominantly meant to be run in cluster mode. The latter set of scripts run
 * on ABF feed data. I meant to run those locally as well, but they error out
 * when run locally. Hence I had to replace them with a different set of local
 * scripts. The cluster bound scripts are parameterized and take in things such
 * as $protocol (hdfs/webhdfs) and $namenode as parameters from the command
 * line. This is where those parameters are converted and script invoked via
 * ProcessBuilder.
 * 
 * The ABF data, is quite large hence it sits in /grid/0/HTF/testdata location.
 * These are scripts/data that Pat has traditionally been running on Axonite Red
 * cluster. Also, since the deploy jobs have an option of 'delete existing data
 * from HDFS', I had to store the data/scripts on /grid/0/.... I could not
 * include this huge data as a part of yinst package, because there is a size
 * limit on how large a yinst package could be. ABF data/scripts work off of two
 * date stamps 20130309 and 20130310, thats what you would find referenced in
 * the code below.
 * 
 * Note: The htf_pig_data yinst package (used for local Pig runs) stores its
 * caned data/scripts under /home/y/share/htf-data/ and some class files (that
 * are distributed as a part of Pig distribution, living unser ~/pig/tutorial
 * section). I've taken that JAR and packaged it as a part of htf_pig_data
 * package. The way you would build that JAR is go to ~/git/pig/tutorial and run
 * 'ant'. That would generate pigtutorial.tar.gz On Unzipping/untar'ing this you
 * would find a tutorial.jar and [script1-local.pig, script2-local.pig,
 * excite-small.log ] This is what the htf_pig_data yinst package contains.
 */
public class HtfPigBaseClass extends TestSession {

	public static String WORKSPACE_SCRIPT_LOCATION = "/htf-common/resources/hadooptest/pig/scripts/";
	public static String PIG_DATA_DIR_IN_HDFS = "/HTF/testdata/pig/";
	public static final String GRID_0 = "/grid/0/";
	public static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/pig/ABF_Daily";
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public static HashMap<String, Boolean> pathsChmodedSoFar = new HashMap<String, Boolean>();
	private static boolean dataVerifiedOnce = false;

	protected static List<String> fileNames = new ArrayList<String>();

	static {
		fileNames.add("20130309/");
		fileNames.add("20130310/");
	}

	/**
	 * Before a test runs, ensure that it has canned data to work off of.
	 * 
	 * @throws Exception
	 */
	@Before
	public void ensurePigDataPresenceinClusterBeforeTest() throws Exception {
		if (!dataVerifiedOnce) {
			printVersion();
			DfsCliCommands dfsCommonCli = new DfsCliCommands();
			logger.info("running ensurePigDataPresenceinClusterBeforeTest");
			String aCluster = System.getProperty("CLUSTER_NAME");

			for (String aFileName : fileNames) {
				GenericCliResponseBO doesFileExistResponseBO;
				if (aFileName.contains("/")) {
					// This is a directory
					doesFileExistResponseBO = dfsCommonCli.test(
							EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HDFSQA,
							HadooptestConstants.Schema.NONE, aCluster,
							PIG_DATA_DIR_IN_HDFS + aFileName,
							DfsCliCommands.FILE_SYSTEM_ENTITY_DIRECTORY);

				} else {
					doesFileExistResponseBO = dfsCommonCli.test(
							EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HDFSQA,
							HadooptestConstants.Schema.NONE, aCluster,
							PIG_DATA_DIR_IN_HDFS + aFileName,
							DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);

				}

				if (doesFileExistResponseBO.process.exitValue() != 0) {
					dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HDFSQA,
							HadooptestConstants.Schema.NONE, aCluster,
							PIG_DATA_DIR_IN_HDFS);
					dfsCommonCli.put(EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HDFSQA,
							HadooptestConstants.Schema.NONE, aCluster,
							DATA_DIR_IN_LOCAL_FS + aFileName,
							PIG_DATA_DIR_IN_HDFS);

				}
			}
			/**
			 * Since hdfsqa 'put's the files over and 'hadoopqa' is generally
			 * the user, chmod 777 the dirs recursively for subsequent access.
			 */
			dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE,
					System.getProperty("CLUSTER_NAME"), "/HTF", "777",
					Recursive.YES);

			dataVerifiedOnce = true;
		}

	}
	public int printVersion() throws Exception {
		TestSession.logger.info("Retrieveing PIG version..now:");
		StringBuilder sb = new StringBuilder();
		sb.append("/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/pig/bin/pig");
		sb.append(" ");

		sb.append("-version");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>();
		environmentVariablesWrappingTheCommand.put("PIG_HOME",
				"/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
						+ "/share/pig");
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				environmentVariablesWrappingTheCommand);
		printResponseAndReturnItAsString(process);
		return process.exitValue();

	}

	/**
	 * This is where the arguments are resolved into real arguments on the
	 * command line and invoked via ProcessBuilder.
	 * 
	 * @param scriptName
	 * @param protocol
	 * @param namenode
	 * @return
	 * @throws Exception
	 */
	public int runPigScriptOnCluster(List<String> params,
			String scriptWithLocation) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/pig/bin/pig");
		sb.append(" ");

		sb.append("-x tez");
		sb.append(" ");

		for (String aParam:params){
			sb.append(" -param " + aParam);
		}
		sb.append(" -f " + scriptWithLocation);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>();
		environmentVariablesWrappingTheCommand.put("PIG_HOME",
				"/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
						+ "/share/pig");
		environmentVariablesWrappingTheCommand.put("TEZ_HOME", "/home/gs/tez/");
		environmentVariablesWrappingTheCommand.put("TEZ_CONF_DIR",
				"/home/gs/conf/tez/");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				environmentVariablesWrappingTheCommand);
		printResponseAndReturnItAsString(process);
		return process.exitValue();

	}

	/**
	 * As mentioned in the class JavaDoc the local scripts live off of data
	 * provided by the htf_pig_data yinst package. When the package is
	 * installed, it installs its canned data and pig scripts in
	 * /home/y/share/htf-data and the Java classes in /home/y/lib/jar location.
	 * 
	 * @param scriptName
	 * @param outdir
	 * @return
	 * @throws Exception
	 */
	public int runPigScriptLocally(String scriptName, String outdir)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/pig/bin/pig");
		sb.append(" ");

		sb.append("-x tez_local");
		sb.append(" ");
		sb.append("-param outdir=" + outdir);
		sb.append(" ");

		sb.append("-f " + "/home/y/share/htf-data/" + scriptName);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>();
		environmentVariablesWrappingTheCommand.put("PIG_HOME",
				"/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
						+ "/share/pig");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				environmentVariablesWrappingTheCommand);
		printResponseAndReturnItAsString(process);
		return process.exitValue();

	}

	private String printResponseAndReturnItAsString(Process process)
			throws InterruptedException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line;
		try {
			line = reader.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				TestSession.logger.debug(line);
				line = reader.readLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		process.waitFor();
		return sb.toString();
	}

}
