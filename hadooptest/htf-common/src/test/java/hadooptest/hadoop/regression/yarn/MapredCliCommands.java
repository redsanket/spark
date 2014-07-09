package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MapredCliCommands {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	public static String KRB5CCNAME = "KRB5CCNAME";

	public MapredCliCommands() {
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties
					.load(new FileInputStream(
							HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

	}

	/*
	 * A CLI Response Business Object. All CLI responses can conform to this.
	 */
	public class GenericMapredCliResponseBO {
		public Process process;
		public String response;

		public GenericMapredCliResponseBO(Process process, String response) {
			this.process = process;
			this.response = response;
		}
	}

	public GenericMapredCliResponseBO listActiveTrackers(
			HashMap<String, String> envMapSentByTest, String user)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("job");
		sb.append(" ");
		sb.append("-list-active-trackers");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO getJobStatus(
			HashMap<String, String> envMapSentByTest, String user, String jobId)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("job");
		sb.append(" ");
		sb.append("-status");
		sb.append(" ");
		sb.append(jobId);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO jobList(
			HashMap<String, String> envMapSentByTest, String user, String jobId)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("job");
		sb.append(" ");
		sb.append("-list");
		if (!jobId.isEmpty()) {
			sb.append(" ");
			sb.append(jobId);
		}

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO killJob(
			HashMap<String, String> envMapSentByTest, String user, String jobId)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("job");
		sb.append(" ");
		sb.append("-kill");
		sb.append(" ");
		sb.append(jobId);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO queueList(
			HashMap<String, String> envMapSentByTest, String user)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("queue");
		sb.append(" ");
		sb.append("-list");
		sb.append(" ");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO queueShowAcls(
			HashMap<String, String> envMapSentByTest, String user)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("queue");
		sb.append(" ");
		sb.append("-showacls");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	public GenericMapredCliResponseBO queueInfo(
			HashMap<String, String> envMapSentByTest, String user,
			String queueName, boolean showJobs) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.MAPRED);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("queue");
		sb.append(" ");
		sb.append("-info");
		sb.append(" ");
		sb.append(queueName);
		if (showJobs) {
			sb.append(" ");
			sb.append("-showJobs");
		}

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericMapredCliResponseBO responseBO = new GenericMapredCliResponseBO(
				process, response);
		return responseBO;
	}

	String printResponseAndReturnItAsString(Process process)
			throws InterruptedException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		boolean fileExists = false;
		String line;
		try {
			line = reader.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				TestSession.logger.info(line);
				line = reader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();
		return sb.toString();
	}

}
