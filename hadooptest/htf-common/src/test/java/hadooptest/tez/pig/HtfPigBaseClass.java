package hadooptest.tez.pig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

public class HtfPigBaseClass extends TestSession {
	public enum ON_TEZ {
		YES, NO
	};

	public static String WORKSPACE_SCRIPT_LOCATION = "/htf-common/resources/hadooptest/pig/scripts/";
	public static String PIG_DATA_DIR_IN_HDFS = "/HTF/testdata/pig/";
	public static final String GRID_0 = "/grid/0/";
	public static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/pig/";
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public static HashMap<String, Boolean> pathsChmodedSoFar = new HashMap<String, Boolean>();
	private static boolean dataVerifiedOnce = false;

	protected static List<String> fileNames = new ArrayList<String>();

	static {
		fileNames.add("20130309/");
		fileNames.add("20130310/");
		fileNames.add("15n15mBy10.data");
		fileNames.add("cube_rollup_5.data");
	}

	@Before
	public void ensurePigDataPresenceinClusterBeforeTest() throws Exception {
		if (!dataVerifiedOnce) {
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
					doChmodRecursively(PIG_DATA_DIR_IN_HDFS);
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
					System.getProperty("CLUSTER_NAME"), PIG_DATA_DIR_IN_HDFS, "777");

			dataVerifiedOnce = true;
		}

	}

	private void doChmodRecursively(String dirHierarchy) throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty())
				continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir + "/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			if (!pathsChmodedSoFar.containsKey(pathsChmodedSoFar)) {
				dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE,
						System.getProperty("CLUSTER_NAME"), pathSoFar, "777");
				pathsChmodedSoFar.put(pathSoFar, true);
			}
		}
	}

	int runPigScript(String scriptName, String nnHostname, ON_TEZ onTez)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/pig/bin/pig");
		sb.append(" ");
		if (onTez == ON_TEZ.YES) {
			sb.append("-x tez");
			sb.append(" ");
		}
		if (!nnHostname.isEmpty()) {
			sb.append("-param namenode=" + nnHostname);
			sb.append(" ");
		}
		sb.append("-f " + TestSession.conf.getProperty("WORKSPACE")
				+ WORKSPACE_SCRIPT_LOCATION + scriptName);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>();
		environmentVariablesWrappingTheCommand.put("PIG_HOME",
				"/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
						+ "/share/pig");
		if (onTez == ON_TEZ.YES) {
			environmentVariablesWrappingTheCommand.put("TEZ_HOME",
					"/home/gs/tez/");
			environmentVariablesWrappingTheCommand.put("TEZ_CONF_DIR",
					"/home/gs/conf/tez/");
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HADOOPQA,
				environmentVariablesWrappingTheCommand);
		printResponseAndReturnItAsString(process);
		return process.exitValue();

	}

	String printResponseAndReturnItAsString(Process process)
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
