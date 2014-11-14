package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass.YarnAdminSubCommand;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass.YarnApplicationSubCommand;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class YarnCliCommands {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	public static String KRB5CCNAME = "KRB5CCNAME";

	public YarnCliCommands() {
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
	public class GenericYarnCliResponseBO {
		public Process process;
		public String response;

		public GenericYarnCliResponseBO(Process process, String response) {
			this.process = process;
			this.response = response;
		}
	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param directoryHierarchy
	 * @return
	 * @throws Exception
	 */
	public GenericYarnCliResponseBO rmadmin(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			YarnAdminSubCommand yarnSubCommand, String var) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.YARN);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("rmadmin");
		sb.append(" ");
		
		if (yarnSubCommand == YarnAdminSubCommand.GET_GROUPS){
			sb.append("-getGroups"); sb.append(" ");sb.append(var);
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_ADMIN_ACLS){
			sb.append("-refreshAdminAcls"); sb.append(" ");
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_NODES){
			sb.append("-refreshNodes"); sb.append(" ");
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_QUEUES){
			sb.append("-refreshQueues"); sb.append(" ");
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_SERVICE_ACL){
			sb.append("-refreshServiceAcl"); sb.append(" ");			
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_SUPERUSER_GROUPS_CONFIGURATION){
			sb.append("-refreshSuperUserGroupsConfiguration"); sb.append(" ");
		}else if(yarnSubCommand == YarnAdminSubCommand.REFRESH_USER_TO_GROUPS_MAPPING){
			sb.append("-refreshUserToGroupsMappings"); sb.append(" ");
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
		GenericYarnCliResponseBO responseBO = new GenericYarnCliResponseBO(process,
				response);
		return responseBO;
	}

	public GenericYarnCliResponseBO application(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			YarnApplicationSubCommand yarnSubCommand, String argList) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.YARN);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("application");
		sb.append(" ");
		
		if (yarnSubCommand == YarnApplicationSubCommand.APPSTATES){
			sb.append("-appStates").append(" ").append(argList);
		}else if(yarnSubCommand == YarnApplicationSubCommand.APPTYPES){
			sb.append("-appTypes").append(" ").append(argList);
		}else if(yarnSubCommand == YarnApplicationSubCommand.KILL){
			sb.append("-kill").append(" ").append(argList);
		}else if(yarnSubCommand == YarnApplicationSubCommand.LIST){
			sb.append("-list").append(" ").append(argList);
		}else if(yarnSubCommand == YarnApplicationSubCommand.MOVE_TO_QUEUE){
			sb.append("-movetoqueue").append(" ").append(argList);
		}else if(yarnSubCommand == YarnApplicationSubCommand.QUEUE){
			sb.append("-queue").append(" ").append(argList);
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
		GenericYarnCliResponseBO responseBO = new GenericYarnCliResponseBO(process,
				response);
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
				TestSession.logger.debug(line);
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
