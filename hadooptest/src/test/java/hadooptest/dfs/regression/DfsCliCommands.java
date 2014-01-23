package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;

public class DfsCliCommands {
	private static Properties crossClusterProperties;
	public static String FILE_SYSTEM_ENTITY_FILE = "FILE";
	public static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	
	public DfsCliCommands(){
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
	 * 'dfsload' is our superuser who can create directories in DFS. For some
	 * reason it did not work with 'hadoopqa'.
	 */
	void createDirectoriesOnHdfs(String cluster, String directoryHierarchy)
			throws Exception {

		if (doesFsEntityAlreadyExistOnDfs(cluster, directoryHierarchy, FILE_SYSTEM_ENTITY_DIRECTORY)==0){
			//Do not need to re-create the directories
			return;
		}
		if (directoryHierarchy.charAt(directoryHierarchy.length()-1) != '/'){
			directoryHierarchy = directoryHierarchy +"/";
		}
		int lastIndexOfSlash = directoryHierarchy.lastIndexOf('/');

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
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(directoryHierarchy);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());

	}

	public void doChmod(String aCluster, String completePath, String permissions) throws Exception {

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

		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(aCluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePath);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}
	
	public int doesFsEntityAlreadyExistOnDfs(String aCluster, String completePath, String entityType) throws Exception {

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
		if (entityType.equalsIgnoreCase(FILE_SYSTEM_ENTITY_FILE)){
			sb.append("-f");
			sb.append(" ");
		}else{
			sb.append("-d");
			sb.append(" ");

		}
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(aCluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePath);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		if (process.exitValue() ==0){
			TestSession.logger.info("Entity:" + completePath + " exists on cluster:" + aCluster);
		}else{
			TestSession.logger.info("Entity:" + completePath + " does not exist on cluster:" + aCluster);
		}
		return process.exitValue();
	}

	
	/*
	 * Delete a dirctory on HDFS
	 */
	void deleteDirectoriesFromThisPointOnwardsOnHdfs(String cluster,
			String completePathToLeafDir) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs");
		sb.append(" ");
		sb.append("-rm -r -f");
		sb.append(" ");
		sb.append("-skipTrash");
		sb.append(" ");
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePathToLeafDir);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());

	}
	FsckResponseBO executeFsckCommand(String cluster,
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

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		FsckResponseBO fsckResponseBO = new FsckResponseBO(process,
				includeFilesArg, includeBlocksArg, includeRacksArg);
		TestSession.logger.info(fsckResponseBO);
		printAndScanResponse(process);
		if (process.exitValue() == 0) {
			return fsckResponseBO;
		} else {
			// Problem in executing the command
			return null;
		}

	}

	boolean executeSafemodeCommand(String safemodeArg) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfsadmin");
		sb.append(" ");
		sb.append("-safemode");
		sb.append(" ");
		sb.append(safemodeArg);
		sb.append(" ");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA, // This guy
																	// is the
																	// super-user
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	boolean executeBalancerCommand() throws Exception {
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

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA, // This guy
																	// is the
																	// super-user
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	boolean doLsOnFile(String cluster, String completePathToFile)
			throws Exception {
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
		String nameNodeWithWebHdfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebHdfsSchema);
		sb.append(completePathToFile);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	/*
	 * We use web-hdfs to move files across clusters, because there could be a
	 * version incompatibility. Webhdfs is agnostic to that, unlike Hdfs.
	 */
	void copyLocalFileIntoClusterUsingWebhdfs(String cluster,
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
		sb.append("-copyFromLocal");
		sb.append(" ");
		sb.append(completePathOfSource);
		sb.append(" ");
		String nameNodeWithWebddfsSchema = getNNUrlForWebhdfs(cluster);
		sb.append(nameNodeWithWebddfsSchema);
		sb.append(completePathOfDest);
		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		boolean fileExists = printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
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


	/*
	 * Remove the copied over file, at the end of a successful test.
	 */
	public void removeFile(String cluster, String pathToFile) throws Exception {

		TestSession.logger.info("Initiating removal of " + pathToFile);
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("dfs -rm -f");
		sb.append(" ");
		String nnForWebhdfs = getNNUrlForWebhdfs(cluster);
		sb.append(nnForWebhdfs + pathToFile);

		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}

	boolean printAndScanResponse(Process process) throws InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		boolean fileExists = false;
		String line;
		try {
			line = reader.readLine();
			while (line != null) {
				if (line.contains("File exists")){
					fileExists = true;
				}
				TestSession.logger.debug(line);
				line = reader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();
		return fileExists;
	}
	
	public void distcpFileUsingProtocol(String srcCluster, String dstCluster,
			String srcFile, String dstFile, String srcProtocol, String dstProtocol) throws Exception {
		String nameNodePrependedWithProtocol=null;
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HADOOP);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("distcp -pbugp");
		sb.append(" ");
		
		if (srcProtocol.isEmpty()){
			nameNodePrependedWithProtocol="";
		}else if(srcProtocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)){
			nameNodePrependedWithProtocol = getNNUrlForHdfs(srcCluster);			
		}else if(srcProtocol.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)){
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(srcCluster);		
		}else if(srcProtocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)){
			nameNodePrependedWithProtocol = getNNUrlForHftp(srcCluster);
		}
		sb.append(nameNodePrependedWithProtocol + srcFile);
		sb.append(" ");
		
		if (dstProtocol.isEmpty()){
			nameNodePrependedWithProtocol="";
		}else if(dstProtocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)){
			nameNodePrependedWithProtocol = getNNUrlForHdfs(dstCluster);			
		}else if(dstProtocol.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)){
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(dstCluster);		
		}else if(dstProtocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)){
			nameNodePrependedWithProtocol = getNNUrlForHftp(dstCluster);
		}

		sb.append(nameNodePrependedWithProtocol + dstFile);
		
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}
	
	public void fetchDelegationTokenViaHdfsCli(String cluster,String user, String storeItInThisFile, String protocol) throws Exception {
		String nameNodePrependedWithProtocol=null;
		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.HDFS);
		sb.append(" ");
		sb.append("--config");
		sb.append(" ");
		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
		sb.append(" ");
		sb.append("fetchdt -fs");
		sb.append(" ");
		
		if (protocol.isEmpty()){
			nameNodePrependedWithProtocol="";
		}else if(protocol.equalsIgnoreCase(HadooptestConstants.Schema.HDFS)){
			nameNodePrependedWithProtocol = getNNUrlForHdfs(cluster);			
		}else if(protocol.equalsIgnoreCase(HadooptestConstants.Schema.WEBHDFS)){
			nameNodePrependedWithProtocol = getNNUrlForWebhdfs(cluster);		
		}else if(protocol.equalsIgnoreCase(HadooptestConstants.Schema.HFTP)){
			nameNodePrependedWithProtocol = getNNUrlForHftp(cluster);
		}
		sb.append(nameNodePrependedWithProtocol);		
		
		sb.append(" ");
		sb.append(storeItInThisFile);
		
		String commandString = sb.toString();

		String[] commandFrags = commandString.split("\\s+");
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");

		Process process = null;
		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, user,
				envToUnsetHadoopPrefix);
		printAndScanResponse(process);
		Assert.assertEquals(commandString, 0, process.exitValue());
	}
	
	
//	public void distcpWebhdfsToWebhdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		
//		String nnForWebhdfsSrc = getNNUrlForWebhdfs(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)+ dstFile;
//		sb.append(schemaDecoratedDestination);
//		
//		String commandString = sb.toString();
//
//		String[] commandFrags = commandString.split("\\s+");
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//	}
//	
//	void distcpWebhdfsToHdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		String nnForWebhdfsSrc = getNNUrlForWebhdfs(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
//				+ dstFile;
//		sb.append(schemaDecoratedDestination);
//		
//		String commandString = sb.toString();
//
//		String[] commandFrags = commandString.split("\\s+");
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//
//	}
//
//	void distcpHftpToWebhdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		String nnForWebhdfsSrc = getNNUrlForHftp(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)+ dstFile;
//		sb.append(schemaDecoratedDestination);
//		String commandString = sb.toString();
//
//		String[] commandFrags = commandString.split("\\s+");
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//	}
//	
//	void distcpHftpToHdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		String nnForWebhdfsSrc = getNNUrlForHftp(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
//				+ dstFile;
//		sb.append(schemaDecoratedDestination);
//		String commandString = sb.toString();
//
//		String[] commandFrags = commandString.split("\\s+");
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//
//	}
//	void distcpHdfsToWebhdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		String nnForWebhdfsSrc = getNNUrlForHdfs(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForWebhdfs(dstCluster)
//				+ dstFile ;
//		sb.append(schemaDecoratedDestination);
//		String commandString = sb.toString();
//
//		String[] commandFrags = commandString.split("\\s+");
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//
//	}
//	
//	void distcpHdfsToHdfs(String srcCluster, String dstCluster,
//			String srcFile, String dstFile) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append(HadooptestConstants.Location.Binary.HADOOP);
//		sb.append(" ");
//		sb.append("--config");
//		sb.append(" ");
//		sb.append(HadooptestConstants.Location.Conf.DIRECTORY);
//		sb.append(" ");
//		sb.append("distcp -pbugp");
//		sb.append(" ");
//		String nnForWebhdfsSrc = getNNUrlForHdfs(srcCluster);
//		sb.append(nnForWebhdfsSrc + srcFile);
//		sb.append(" ");
//		String schemaDecoratedDestination = getNNUrlForHdfs(dstCluster)
//				+ dstFile;
//		sb.append(schemaDecoratedDestination);
//		String commandString = sb.toString();
//		String[] commandFrags = commandString.split("\\s+");
//
//		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();
//		envToUnsetHadoopPrefix.put("HADOOP_PREFIX", "");
//
//		Process process = null;
//		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
//				commandFrags, HadooptestConstants.UserNames.HDFSQA,
//				envToUnsetHadoopPrefix);
//		printAndScanResponse(process);
//		Assert.assertEquals(commandString, 0, process.exitValue());
//
//	}



}
