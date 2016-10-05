package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DfsCliCommands {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	public static String KRB5CCNAME = "KRB5CCNAME";

	public DfsCliCommands() {
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties.load(new FileInputStream(
			        HadooptestConstants.Location.TestProperties.CrossClusterProperties));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

	}

	/*
	 * A CLI Response Business Object. All CLI responses can conform to this.
	 */
	public class GenericCliResponseBO {
		public Process process;
		public String response;

		public GenericCliResponseBO(Process process, String response) {
			this.process = process;
			this.response = response;
		}
	}

	/*
	 * 'dfsload' is our superuser who can create directories in DFS. For some
	 * reason it did not work with 'hadoopqa'.
	 */
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
	public GenericCliResponseBO mkdir(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			String directoryHierarchy) throws Exception {
		String nameNodePrependedWithProtocol = "";
		HashMap<String, String> tempEnv = new HashMap<String, String>();
		if (envMapSentByTest.containsKey(KRB5CCNAME)) {
			tempEnv.put(KRB5CCNAME, envMapSentByTest.get(KRB5CCNAME));
		}
		GenericCliResponseBO quickCheck = test(tempEnv,
				HadooptestConstants.UserNames.HDFSQA, protocol, cluster,
				directoryHierarchy, FILE_SYSTEM_ENTITY_DIRECTORY);

		if (quickCheck.process.exitValue() == 0) {
			// Do not need to re-create the directories
			return quickCheck;
		}
		if (directoryHierarchy.charAt(directoryHierarchy.length() - 1) != '/') {
			directoryHierarchy = directoryHierarchy + "/";
		}

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-mkdir -p");
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(directoryHierarchy);

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
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;
	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePath
	 * @param permissions
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO chmod(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, String completePath,
			String permissions, Recursive recursively) throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-chmod ");
		sb.append(" ");
		if (recursively == Recursive.YES) {
			sb.append("-R");
			sb.append(" ");
		}
		sb.append(permissions);
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		/*
		 * If the command uses bash wildcards (*, {..}), we will need to specify
		 * the path to the shell. Otherwise, they will not get interpreted by
		 * the process builder.
		 */
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				new String[] { "/bin/bash", "-c", commandString }, user,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePath
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO touchz(
			HashMap<String, String> envMapSentByTest, String user,
			String protocol, String cluster, String completePath)
			throws Exception {
		String nameNodePrependedWithProtocol = "";

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-touchz");
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		/*
		 * If the command uses bash wildcards (*, {..}), we will need to specify
		 * the path to the shell. Otherwise, they will not get interpreted by
		 * the process builder.
		 */
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				new String[] { "/bin/bash", "-c", commandString }, user,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePath
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO cat(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, String completePath)
			throws Exception {
		String nameNodePrependedWithProtocol = "";

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-cat");
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

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

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePath
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO test(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, String completePath,
			String entityType) throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-test");
		sb.append(" ");
		if (entityType.equalsIgnoreCase(FILE_SYSTEM_ENTITY_FILE)) {
			sb.append("-f");
			sb.append(" ");
		} else {
			sb.append("-d");
			sb.append(" ");

		}
		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

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
		if (process.exitValue() == 0) {
			TestSession.logger.info("Entity:" + completePath
					+ " exists on cluster:" + cluster);
		} else {
			TestSession.logger.info("Entity:" + completePath
					+ " does not exist on cluster:" + cluster);
		}
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * Delete a dirctory on HDFS
	 */
	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param recursive
	 * @param force
	 * @param skipTrash
	 * @param completePath
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO rm(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, Recursive recursive,
			Force force, SkipTrash skipTrash, String completePath)
			throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-rm");
		sb.append(" ");
		if (recursive == Recursive.YES) {
			sb.append("-r");
			sb.append(" ");
		}
		if (force == Force.YES) {
			sb.append("-f");
			sb.append(" ");
		}
		if (skipTrash == SkipTrash.YES) {
			sb.append("-skipTrash");
			sb.append(" ");
		}

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

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
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * Delete a dirctory on HDFS
	 */
	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePath
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO rmdir(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, String completePath)
			throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-rmdir");
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePath);

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
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * FSCK is run on the local cluster, hence does not need a protocol
	 * parameter.
	 */
	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param completePathToFile
	 * @param includeFilesArg
	 * @param includeBlocksArg
	 * @param includeRacksArg
	 * @return
	 * @throws Exception
	 */
	public FsckResponseBO fsck(HashMap<String, String> envMapSentByTest,
			String user, String completePathToFile, boolean includeFilesArg,
			boolean includeBlocksArg, boolean includeRacksArg) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("fsck");
		sb.append(" ");
		sb.append(completePathToFile);
		sb.append(" ");
		if (includeFilesArg) {
			sb.append("-files");
			sb.append(" ");
		}
		if (includeBlocksArg) {
			sb.append("-blocks");
			sb.append(" ");
		}
		if (includeRacksArg) {
			sb.append("-racks");
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
		FsckResponseBO fsckResponseBO = new FsckResponseBO(process,
				includeFilesArg, includeBlocksArg, includeRacksArg);
		TestSession.logger.info(fsckResponseBO);
		printResponseAndReturnItAsString(process);
		if (process.exitValue() == 0) {
			return fsckResponseBO;
		} else {
			// Problem in executing the command
			return null;
		}

	}

	/*
	 * DFSADMIN is always run on the local cluster, hence does not need a
	 * protocol argument
	 */
	/**
	 * 
	 * @param envMapSentByTest
	 * @param runReport
	 * @param safemodeArg
	 * @param clearQuota
	 * @param setQuota
	 * @param quota
	 * @param clearSpaceQuota
	 * @param setSpaceQuota
	 * @param spaceQuota
	 * @param printTopology
	 * @param fsEntity
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO dfsadmin(
			HashMap<String, String> envMapSentByTest, Report runReport,
			String safemodeArg, ClearQuota clearQuota, SetQuota setQuota,
			long quota, ClearSpaceQuota clearSpaceQuota,
			SetSpaceQuota setSpaceQuota, long spaceQuota,
			PrintTopology printTopology, String fsEntity) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfsadmin");
		sb.append(" ");

		// Report
		if (runReport == Report.YES) {
			sb.append("-report");
			sb.append(" ");
		}

		// Print Topology
		if (printTopology == PrintTopology.YES) {
			sb.append(" ");
			sb.append("-printTopology");
			sb.append(" ");
		}

		// Safemode
		if ((safemodeArg != null)) {
			sb.append("-safemode");
			sb.append(" ");
			sb.append(safemodeArg);
			sb.append(" ");

		}
		// Quota
		if (clearQuota == ClearQuota.YES) {
			sb.append("-clrQuota");
			sb.append(" ");
			sb.append(fsEntity);
		} else if (setQuota == SetQuota.YES) {
			if (quota > 0) {
				sb.append("-setQuota");
				sb.append(" ");
				sb.append(quota);
				sb.append(" ");
				sb.append(fsEntity);
			}
		}
		// Space Quota
		if (clearSpaceQuota == ClearSpaceQuota.YES) {
			sb.append("-clrSpaceQuota");
			sb.append(" ");
			sb.append(fsEntity);
		} else if (setSpaceQuota == SetSpaceQuota.YES) {
			if (spaceQuota > 0) {
				sb.append("-setSpaceQuota");
				sb.append(" ");
				sb.append(spaceQuota);
				sb.append(" ");
				sb.append(fsEntity);
			}
		}

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param policyValue
	 * @param thresholdValue
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO balancer(
			HashMap<String, String> envMapSentByTest, String user,
			String policyValue, String thresholdValue) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("balancer");
		if (policyValue != null) {
			sb.append(" ");
			sb.append("-policy");
			sb.append(" ");
			sb.append(policyValue);

		}
		if (thresholdValue != null) {
			sb.append(" ");
			sb.append("-threshold");
			sb.append(" ");
			sb.append(thresholdValue);

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

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePathToFile
	 * @param recursive
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO ls(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			String completePathToFile, Recursive recursive) throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HADOOP);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("fs");
		sb.append(" ");
		sb.append("-ls");
		sb.append(" ");
		if (recursive == Recursive.YES) {
			sb.append("-R");
			sb.append(" ");

		}
		if (protocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePathToFile);

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
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * We use web-hdfs to move files across clusters, because there could be a
	 * version incompatibility. Webhdfs is agnostic to that, unlike Hdfs.
	 */
	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePathOfSource
	 * @param completePathOfDest
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO copyFromLocal(
			HashMap<String, String> envMapSentByTest, String user,
			String protocol, String cluster, String completePathOfSource,
			String completePathOfDest) throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-copyFromLocal");
		sb.append(" ");
		sb.append(completePathOfSource);
		sb.append(" ");

		if (protocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePathOfDest);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		/*
		 * If the command uses bash wildcards (*, {..}), we will need to specify
		 * the path to the shell. Otherwise, they will not get interpreted by
		 * the process builder.
		 */
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				new String[] { "/bin/bash", "-c", commandString }, user,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePathOfSource
	 * @param completePathOfDest
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO put(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			String completePathOfSource, String completePathOfDest)
			throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-put");
		sb.append(" ");
		sb.append(completePathOfSource);
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePathOfDest);

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		/*
		 * If the command uses bash wildcards (*, {..}), we will need to specify
		 * the path to the shell. Otherwise, they will not get interpreted by
		 * the process builder.
		 */
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				new String[] { "/bin/bash", "-c", commandString }, user,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param completePathOfSource
	 * @param completePathOfDest
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO cp(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster,
			String completePathOfSource, String completePathOfDest)
			throws Exception {
		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-cp");
		sb.append(" ");
		sb.append(completePathOfSource);
		sb.append(" ");

		if ((protocol.trim()).isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(completePathOfDest);

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
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param cluster
	 * @param completePathOfSource
	 * @param completePathOfDest
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO mv(HashMap<String, String> envMapSentByTest,
			String user, String cluster, String completePathOfSource,
			String completePathOfDest) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-mv");
		sb.append(" ");
		sb.append(completePathOfSource);
		sb.append(" ");

		sb.append(completePathOfDest);
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

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param archiveName
	 * @param parentPath
	 * @param source
	 * @param destinationPath
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO archive(
			HashMap<String, String> envMapSentByTest, String user,
			String protocol, String cluster, String archiveName,
			String parentPath, String source, String destinationPath)
			throws Exception {
		// USAGE: /home/gs/gridre/yroot.merry/share/hadoop/bin/hadoop
		// --config /home/gs/gridre/yroot.merry/conf/hadoop/
		// archive -archiveName
		// eventual.har -p /user/hadoopqa/parent
		// stuffThatUWantArchived
		// /user/hadoopqa/dst

		String nameNodePrependedWithProtocol = "";
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HADOOP);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("archive");
		sb.append(" ");
		sb.append("-archiveName");
		sb.append(" ");
		sb.append(archiveName);
		sb.append(" ");
		sb.append("-p");
		sb.append(" ");

		if (protocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(parentPath);

		sb.append(" ");
		sb.append(source);
		sb.append(" ");

		if (protocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);
		} else if (protocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);
		} else if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);
		sb.append(destinationPath);

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

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	static String getNamenodeForLocalCluster() {
		// This is the same cluster, no need to lookup the config file
		String namenodeHostName = null;
		HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
				HadooptestConstants.NodeTypes.NAMENODE);

		Hashtable<String, HadoopNode> nodesHash = hadoopComp.getNodes();
		for (String key : nodesHash.keySet()) {
			TestSession.logger.info("Key:" + key);
			TestSession.logger.info("The associated namenode host is:"
					+ nodesHash.get(key).getHostname());
			namenodeHostName = nodesHash.get(key).getHostname();
			break;
		}
		return namenodeHostName;

	}

	/**
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case webhdfs://
	 * 
	 * @param cluster
	 * @return
	 */
	public static String getNNUrlForWebhdfs(String cluster) {
		String namenodeHost;
		String nameNodeWithNoPortButSchemaSetAsWebhdfs = null;
		if (cluster.equalsIgnoreCase(System.getProperty("CLUSTER_NAME"))) {

			namenodeHost = getNamenodeForLocalCluster();
			nameNodeWithNoPortButSchemaSetAsWebhdfs = HadooptestConstants.Schema.WEBHDFS
					+ namenodeHost;

		} else {
			// Remote cluster
	        namenodeHost = TestSession.getNamenodeURL(cluster);
			nameNodeWithNoPortButSchemaSetAsWebhdfs = namenodeHost.replace(
					":50070", "");
			nameNodeWithNoPortButSchemaSetAsWebhdfs = nameNodeWithNoPortButSchemaSetAsWebhdfs
					.replace(HadooptestConstants.Schema.HTTP,
							HadooptestConstants.Schema.WEBHDFS);

		}

		return nameNodeWithNoPortButSchemaSetAsWebhdfs;

	}

	/**
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hdfs://
	 * 
	 * @param cluster
	 * @return
	 */
	static public String getNNUrlForHdfs(String cluster) {
		String namenodeHost;
		String nameNodeWithNoPortButSchemaSetAsHdfs = null;
		if (cluster.equalsIgnoreCase(System.getProperty("CLUSTER_NAME"))) {
			namenodeHost = getNamenodeForLocalCluster();
			nameNodeWithNoPortButSchemaSetAsHdfs = HadooptestConstants.Schema.HDFS
					+ namenodeHost;

		} else {
			// Remote cluster
			namenodeHost = TestSession.getNamenodeURL(cluster);
			nameNodeWithNoPortButSchemaSetAsHdfs = namenodeHost.replace(
					":50070", "");
			nameNodeWithNoPortButSchemaSetAsHdfs = nameNodeWithNoPortButSchemaSetAsHdfs
					.replace(HadooptestConstants.Schema.HTTP,
							HadooptestConstants.Schema.HDFS);
		}

		return nameNodeWithNoPortButSchemaSetAsHdfs;
	}

	/**
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hftp://
	 * 
	 * @param cluster
	 * @return
	 */
	private String getNNUrlForHftp(String cluster) {
        String nnReadFromPropFile = TestSession.getNamenodeURL(cluster);
		String nameNodeWithPortAndSchemaForHftp = nnReadFromPropFile.replace(
				HadooptestConstants.Schema.HTTP,
				HadooptestConstants.Schema.HFTP);
		return nameNodeWithPortAndSchemaForHftp;

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

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param srcCluster
	 * @param dstCluster
	 * @param srcFile
	 * @param dstFile
	 * @param srcProtocol
	 * @param dstProtocol
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO distcp(
			HashMap<String, String> envMapSentByTest, String user,
			String srcCluster, String dstCluster, String srcFile,
			String dstFile, String srcProtocol, String dstProtocol)
			throws Exception {
		return this.distcp(envMapSentByTest, user, srcCluster, dstCluster,
				srcFile, dstFile, srcProtocol, dstProtocol, null);
	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param srcCluster
	 * @param dstCluster
	 * @param srcFile
	 * @param dstFile
	 * @param srcProtocol
	 * @param dstProtocol
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO distcp(
			HashMap<String, String> envMapSentByTest, String user,
			String srcCluster, String dstCluster, String srcFile,
			String dstFile, String srcProtocol, String dstProtocol,
			String optionArgs) throws Exception {
		String nameNodePrependedWithProtocol = null;

        StringBuilder sb = new StringBuilder();

        if (DfsTestsBaseClass.crossclusterPerf) {
            String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
            if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
                sb.append("export HADOOP_OPTS=");
                sb.append("'-Dhttp.proxyHost=" + httpProxyHost);
                sb.append(" -Dhttp.proxyPort=4080'");
                sb.append("; ");
            }
        } else {
            sb.append("unset HADOOP_OPTS;");
        }

		// HADOOP_OPTS="-Dhttp.proxyHost=fsbl824n00.blue.ygrid.yahoo.com -Dhttp.proxyPort=4080"
		sb.append(HadooptestConstants.Location.Binary.HADOOP);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");

		sb.append("distcp");
		sb.append(" ");

		// Use the file name for the job name
		String fileName = new File(srcFile).getName();
		sb.append("-Dmapreduce.job.name=" + fileName + "_" + srcCluster + "_"
				+ dstCluster);
		sb.append(" ");

		// E.g. Http proxy override:
		// -Dhttp.proxyHost=ats307.tan.ygrid.yahoo.com -Dhttp.proxyPort=4080'
		if (optionArgs != null && !optionArgs.isEmpty()) {
			sb.append(optionArgs);
			sb.append(" ");
		}

		sb.append("-pbugp");
		sb.append(" ");

		if (srcProtocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (srcProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(srcCluster);
		} else if (srcProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(srcCluster);
		} else if (srcProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(srcCluster);
		}
		sb.append(nameNodePrependedWithProtocol + srcFile);
		sb.append(" ");

		if (dstProtocol.isEmpty()) {
			nameNodePrependedWithProtocol = "";
		} else if (dstProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForHdfs(dstCluster);
		} else if (dstProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(dstCluster);
		} else if (dstProtocol
				.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
			nameNodePrependedWithProtocol = getNNUrlForHftp(dstCluster);
		}

		sb.append(nameNodePrependedWithProtocol + dstFile);
		String commandString = sb.toString();

		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		/*
		 * If the command uses bash wildcards (*, {..}), we will need to specify
		 * the path to the shell. Otherwise, they will not get interpreted by
		 * the process builder.
		 */
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				new String[] { "/bin/bash", "-c", commandString }, user,
				environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;
	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param protocol
	 * @param cluster
	 * @param webservice
	 * @param renewer
	 * @param cancel
	 * @param renew
	 * @param print
	 * @param tokenFile
	 * @param conf
	 * @param d
	 * @param jt
	 * @param files
	 * @param libjars
	 * @param archives
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO fetchdt(
			HashMap<String, String> envMapSentByTest, String user,
			String protocol, String cluster, String webservice, String renewer,
			String cancel, String renew, String print, String tokenFile,
			String conf, String d, String jt, String files, String libjars,
			String archives) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("fetchdt");
		sb.append(" ");

		if (webservice != null) {
			sb.append("--webservice");
			sb.append(" ");
			sb.append(webservice);
			sb.append(" ");
		}
		if (renewer != null) {
			sb.append("--renewer");
			sb.append(" ");
			sb.append(renewer);
			sb.append(" ");
		}
		if (cancel != null) {
			sb.append("--cancel");
			sb.append(" ");
		}
		if (renew != null) {
			sb.append("--renew");
			sb.append(" ");
		}
		if (print != null) {
			sb.append("--print");
			sb.append(" ");
		}
		if (conf != null) {
			sb.append("-conf");
			sb.append(" ");
			sb.append(conf);
			sb.append(" ");
		}
		if (d != null) {
			sb.append("-D");
			sb.append(" ");
			sb.append(d);
			sb.append(" ");
		}
		if (protocol != null) {
			if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
				// Commentedout because for HDFS, you cannot specify the fs
				// sb.append(getNNUrlForHdfs(cluster));sb.append(" ");
			} else if (protocol
					.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
				sb.append("-fs");
				sb.append(" ");
				sb.append(getNNUrlForWebhdfs(cluster));
				sb.append(" ");
			} else if (protocol
					.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
				sb.append("-fs");
				sb.append(" ");
				sb.append(getNNUrlForHftp(cluster));
				sb.append(" ");
			} else if ((protocol.trim()).equals("")) {
				// Nothing to append
			}

		}
		if (jt != null) {
			sb.append("-jt");
			sb.append(" ");
			sb.append(jt);
			sb.append(" ");
		}
		if (files != null) {
			sb.append("-files");
			sb.append(" ");
			sb.append(files);
			sb.append(" ");
		}
		if (libjars != null) {
			sb.append("-libjars");
			sb.append(" ");
			sb.append(libjars);
			sb.append(" ");
		}
		if (archives != null) {
			sb.append("-archives");
			sb.append(" ");
			sb.append(archives);
			sb.append(" ");
		}
		if (tokenFile != null) {
			sb.append(tokenFile);
			sb.append(" ");
		}

		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param envMapSentByTest
	 * @param user
	 * @param cluster
	 * @param fsEntity
	 * @return
	 * @throws Exception
	 */
	public GenericCliResponseBO count(HashMap<String, String> envMapSentByTest,
			String user, String protocol, String cluster, String fsEntity)
			throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs -count -q");
		sb.append(" ");
		if (protocol != null) {
			if (protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)) {
				sb.append(getNNUrlForHdfs(cluster));
				sb.append(fsEntity);
				sb.append(" ");
			} else if (protocol
					.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)) {
				sb.append(getNNUrlForWebhdfs(cluster));
				sb.append(fsEntity);
				sb.append(" ");
			} else if (protocol
					.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)) {
				sb.append(getNNUrlForHftp(cluster));
				sb.append(fsEntity);
				sb.append(" ");
			} else if ((protocol.trim()).equals("")) {
				sb.append(fsEntity);
			}
		}

		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
				envMapSentByTest);
		environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, environmentVariablesWrappingTheCommand);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/**
	 * 
	 * @param user
	 * @param destination
	 * @param lifetime
	 * @throws Exception
	 */
	public void createCustomizedKerberosTicket(String user, String destination,
			String lifetime) throws Exception {
		String keytabFileSuffix = user + ".dev.headless.keytab";
		String translatedUserName;
		String keytabFileDir;
		TestSession.logger.info("createKerberosTicketWithLocation '" + user
				+ "':" + "'" + destination + "':" + "'" + lifetime + "':");

		File file = new File(destination);
		file.getParentFile().mkdirs();

		// Point to the keytab
		if (user.equals(HadooptestConstants.UserNames.HADOOPQA)) {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.HADOOPQA;
		} else if (user.equals(HadooptestConstants.UserNames.DFSLOAD)) {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.DFSLOAD;
		} else {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.HDFSQA
					+ "/etc/keytabs/";
		}
		// Translate the user
		if (user.equals(HadooptestConstants.UserNames.HDFS)) {
			translatedUserName = user
					+ "/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM";
		} else if (user.equals(HadooptestConstants.UserNames.DFSLOAD)) {
			translatedUserName = user + "@DEV.YGRID.YAHOO.COM";
		} else {
			translatedUserName = user;
		}

		Map<String, String> newEnv = new HashMap<String, String>();
		newEnv.put("PATH", System.getenv("PATH")
				+ ":/usr/kerberos/bin/:/usr/local/bin:/usr/bin");
		StringBuilder sb = new StringBuilder();
		sb.append("kinit");
		sb.append(" ");
		if (lifetime != null) {
			if (!lifetime.isEmpty()) {
				sb.append("-l");
				sb.append(" ");
				sb.append(lifetime);
				sb.append(" ");
			}
		}
		sb.append("-c");
		sb.append(" ");
		sb.append(destination);
		sb.append(" ");
		sb.append("-k");
		sb.append(" ");
		sb.append("-t");
		sb.append(" ");
		sb.append(" ");
		sb.append(keytabFileDir + "/" + keytabFileSuffix);
		sb.append(" ");
		sb.append(translatedUserName);

		TestSession.exec.runProcBuilder(sb.toString().split("\\s+"));
	}

	public void kdestroy(String cache) throws Exception {

		Map<String, String> newEnv = new HashMap<String, String>();
		newEnv.put("PATH", System.getenv("PATH")
				+ ":/usr/kerberos/bin/:/usr/local/bin:/usr/bin");
		StringBuilder sb = new StringBuilder();
		sb.append("kdestroy");
		sb.append(" ");
		sb.append("-c");
		sb.append(" ");
		sb.append(cache);

		TestSession.exec.runProcBuilder(sb.toString().split("\\s+"));
	}

	public String getKerberosCacheLocation() throws Exception {
		String cacheLocation = "";
		StringBuilder sb = new StringBuilder();
		sb.append("/usr/bin/klist");

		Process proc = TestSession.exec.runProcBuilderGetProc(sb.toString()
				.split("\\s+"));
		proc.waitFor();
		if (proc.exitValue() != 0)
			return cacheLocation;

		InputStream is = proc.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		String SEARCH_TOKEN = "Ticket cache: FILE:";
		while ((line = br.readLine()) != null) {
			if (line.contains(SEARCH_TOKEN)) {
				cacheLocation = line.replace(SEARCH_TOKEN, "").trim();
				break;
			}
		}

		return cacheLocation;
	}

	public void kinit(String user) throws Exception {
		String keytabFileSuffix = user + ".dev.headless.keytab";
		String translatedUserName;
		String keytabFileDir;

		if (user.equals(HadooptestConstants.UserNames.HADOOPQA)) {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.HADOOPQA;
		} else if (user.equals(HadooptestConstants.UserNames.DFSLOAD)) {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.DFSLOAD;
		} else {
			keytabFileDir = "/homes/" + HadooptestConstants.UserNames.HDFSQA
					+ "/etc/keytabs/";
		}
		// Translate the user
		if (user.equals(HadooptestConstants.UserNames.HDFS)) {
			translatedUserName = user
					+ "/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM";
		} else if (user.equals(HadooptestConstants.UserNames.DFSLOAD)) {
			translatedUserName = user + "@DEV.YGRID.YAHOO.COM";
		} else {
			translatedUserName = user;
		}

		Map<String, String> newEnv = new HashMap<String, String>();
		newEnv.put("PATH", System.getenv("PATH")
				+ ":/usr/kerberos/bin/:/usr/local/bin:/usr/bin");
		StringBuilder sb = new StringBuilder();
		sb.append("kinit");
		sb.append(" -k ");
		sb.append(" -t ");
		sb.append(keytabFileDir + "/" + keytabFileSuffix);
		sb.append(" ");
		sb.append(translatedUserName);

		TestSession.exec.runProcBuilder(sb.toString().split("\\s+"));
	}
}
