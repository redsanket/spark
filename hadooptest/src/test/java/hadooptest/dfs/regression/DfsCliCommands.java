package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DfsCliCommands {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";

	public DfsCliCommands() {
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
	public class GenericCliResponseBO {
		Process process;
		String response;

		public GenericCliResponseBO(Process process, String response) {
			this.process = process;
			this.response = response;
		}
	}

	/*
	 * 'dfsload' is our superuser who can create directories in DFS. For some
	 * reason it did not work with 'hadoopqa'.
	 */
	GenericCliResponseBO mkdir(String KRB5CCNAME, String user, String protocol,
			String cluster, String directoryHierarchy) throws Exception {
		String nameNodePrependedWithProtocol = "";
		GenericCliResponseBO quickCheck = test(KRB5CCNAME,
				HadooptestConstants.UserNames.HDFSQA, protocol, cluster,
				directoryHierarchy, FILE_SYSTEM_ENTITY_DIRECTORY);
		if (quickCheck.process.exitValue() == 0) {
			// Do not need to re-create the directories
			return null;
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;
	}

	public GenericCliResponseBO chmod(String KRB5CCNAME, String user,
			String protocol, String cluster, String completePath,
			String permissions) throws Exception {
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
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	public GenericCliResponseBO touchz(String KRB5CCNAME, String user,
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
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	public GenericCliResponseBO test(String KRB5CCNAME, String user,
			String protocol, String cluster, String completePath,
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
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
	GenericCliResponseBO rm(String KRB5CCNAME, String user, String protocol,
			String cluster, boolean recursive, boolean force,
			boolean skipTrash, String completePath) throws Exception {
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
		if (recursive) {
			sb.append("-r");
			sb.append(" ");
		}
		if (force) {
			sb.append("-f");
			sb.append(" ");
		}
		if (skipTrash) {
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * FSCK is run on the local cluster, hence does not need a protocol
	 * parameter.
	 */
	FsckResponseBO fsck(String KRB5CCNAME, String user,
			String completePathToFile, boolean includeFilesArg,
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
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
	GenericCliResponseBO dfsadmin(String KRB5CCNAME, String safemodeArg,
			boolean clearQuota, boolean setQuota, long quota,
			boolean clearSpaceQuota, boolean setSpaceQuota, long spaceQuota,
			String fsEntity) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfsadmin");
		sb.append(" ");
		// Safemode
		if ((safemodeArg != null)) {
			sb.append("-safemode");
			sb.append(" ");
			sb.append(safemodeArg);
			sb.append(" ");

		}
		// Quota
		if (clearQuota) {
			sb.append("-clrQuota");
			sb.append(" ");
			sb.append(fsEntity);
		} else if (setQuota) {
			if (quota > 0) {
				sb.append("-setQuota");
				sb.append(" ");
				sb.append(quota);
				sb.append(" ");
				sb.append(fsEntity);
			}
		}
		// Space Quota
		if (clearSpaceQuota) {
			sb.append("-clrSpaceQuota");
			sb.append(" ");
			sb.append(fsEntity);
		} else if (setSpaceQuota) {
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	GenericCliResponseBO balancer(String KRB5CCNAME, String user)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("balancer");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	GenericCliResponseBO ls(String KRB5CCNAME, String user, String protocol,
			String cluster, String completePathToFile, boolean recursive)
			throws Exception {
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
		if (recursive) {
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * We use web-hdfs to move files across clusters, because there could be a
	 * version incompatibility. Webhdfs is agnostic to that, unlike Hdfs.
	 */
	GenericCliResponseBO copyFromLocal(String KRB5CCNAME, String user,
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
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	GenericCliResponseBO put(String KRB5CCNAME, String user, String protocol,
			String cluster, String completePathOfSource,
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
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	GenericCliResponseBO cp(String KRB5CCNAME, String user, String protocol,
			String cluster, String completePathOfSource,
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	GenericCliResponseBO mv(String KRB5CCNAME, String user, String cluster,
			String completePathOfSource, String completePathOfDest)
			throws Exception {
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);

		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case webhdfs://
	 */
	private String getNNUrlForWebhdfs(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.toLowerCase() + "." + HadooptestConstants.NodeTypes.NAMENODE);

		String nameNodeWithNoPortForWebhdfs = nnReadFromPropFile.replace(
				":50070", "");
		String nameNodeWithNoPortAndSchemaForWebhdfs = nameNodeWithNoPortForWebhdfs
				.replace(HadooptestConstants.Schema.HTTP,
						HadooptestConstants.Schema.WEBHDFS);
		return nameNodeWithNoPortAndSchemaForWebhdfs;

	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hdfs://
	 */
	private String getNNUrlForHdfs(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.trim().toLowerCase()
				+ "."
				+ HadooptestConstants.NodeTypes.NAMENODE);

		String nameNodeWithPortForHdfs = nnReadFromPropFile.replace("50070",
				"8020");
		String nameNodeWithPortAndSchemaForHdfs = nameNodeWithPortForHdfs
				.replace(HadooptestConstants.Schema.HTTP,
						HadooptestConstants.Schema.HDFS);
		return nameNodeWithPortAndSchemaForHdfs;

	}

	/*
	 * Given a cluster, create the NameNode decorated with the proper schema. In
	 * this case hftp://
	 */
	private String getNNUrlForHftp(String cluster) {
		String nnReadFromPropFile = crossClusterProperties.getProperty(cluster
				.trim().toLowerCase()
				+ "."
				+ HadooptestConstants.NodeTypes.NAMENODE);
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
				sb.append("/n");
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

	public GenericCliResponseBO distcpFileUsingProtocol(String KRB5CCNAME,
			String user, String srcCluster, String dstCluster, String srcFile,
			String dstFile, String srcProtocol, String dstProtocol)
			throws Exception {
		String nameNodePrependedWithProtocol = null;
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HADOOP);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("distcp -pbugp");
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

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	public GenericCliResponseBO fetchdt(String KRB5CCNAME, String user,
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
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	public GenericCliResponseBO count(String KRB5CCNAME, String user,
			String cluster, String fsEntity) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs -count -q");
		sb.append(" ");
		sb.append(fsEntity);

		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		if (KRB5CCNAME != null) {
			envToUnsetHadoopPrefix.put("KRB5CCNAME", KRB5CCNAME);
		}

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user, envToUnsetHadoopPrefix);
		String response = printResponseAndReturnItAsString(process);
		GenericCliResponseBO responseBO = new GenericCliResponseBO(process,
				response);
		return responseBO;

	}

	public void createCustomizedKerberosTicket(String user, String destination,
			String lifetime) throws Exception {
		String keytabFileSuffix = user + ".dev.headless.keytab";
		String translatedUserName;
		String keytabFileDir;
		TestSession.logger.info("createKerberosTicketWithLocation '" + user
				+ "':" + "'" + destination + "':" + "'" + lifetime + "':");

		File file = new File(destination);
		file.mkdirs();
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

}