package hadooptest.tez.utils;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.TestHdfsApi;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;

/**
 * This class houses all the common util functions that are used used by Tez.
 * 
 * TODO: Modify this class for parallel runs
 */
public class HtfTezUtils {

	public static String TEZ_SITE_XML = "/home/gs/conf/tez/current/tez-site.xml";

	public static enum Session {
		YES, NO
	};

	public static enum TimelineServer {
		ENABLED, DISABLED
	};

	/**
	 * Tez supports two modes 'local' and 'cluster'. This flip is controlled via
	 * a config parameter/file. This convinience method provides that flip.
	 * 
	 * @param conf
	 * @param mode
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static Configuration setupConfForTez(Configuration conf,
			String mode, Session useSession, TimelineServer timelineServer,
			String testName) throws IOException, InterruptedException {
		// Tez version
		String tezVersion = getTezVersion();
		TestSession.logger.info("Read back tez version as:" + tezVersion);

		// Hadoop version
		String hadoopVersion = "hadoop-" + getHadoopVersion();
		TestSession.logger.info("Read back Hadoop version as:" + hadoopVersion);

		// Dump the env settings.
		Map<String, String> env = System.getenv();
		for (String envName : env.keySet()) {
//			System.out.format("%s=%s%n", envName, env.get(envName));
		}

		// Local mode settings
		if (mode.equals(HadooptestConstants.Execution.TEZ_LOCAL)) {
			TestSession.logger.info("So it is :"
					+ System.getenv(ApplicationConstants.Environment.NM_HOST
							.toString()));
			conf.set("fs.defaultFS", "file:///");
			conf.set(
					"tez.lib.uris",
					"${fs.defaultFS}/sharelib/v1/tez/ytez-"
							+ tezVersion
							+ "/libexec/tez,${fs.defaultFS}/sharelib/v1/tez/ytez-"
							+ tezVersion + "/libexec/tez/lib,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/common,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/common/lib,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/hdfs/,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/hdfs/lib,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/yarn,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/yarn/lib,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/mapreduce,"
							+ "file:///home/gs/gridre/yroot."
							+ System.getProperty("CLUSTER_NAME") + "/share/"
							+ hadoopVersion + "/share/hadoop/mapreduce/lib");

			conf.setBoolean("tez.local.mode", true);
			conf.set("hadoop.security.authentication", "simple");
			conf.setBoolean("tez.runtime.optimize.local.fetch", true);

		} else {
			// Cluster mode
			conf.setBoolean("tez.local.mode", false);
			conf.setBoolean("tez.use.cluster.hadoop-libs", true);
			try {
				File file = new File(TEZ_SITE_XML);
				FileInputStream fileInput = new FileInputStream(file);
				Properties properties = new Properties();
				properties.loadFromXML(fileInput);
				fileInput.close();

				Enumeration enuKeys = properties.keys();
				while (enuKeys.hasMoreElements()) {
					String key = (String) enuKeys.nextElement();
					String value = properties.getProperty(key);
					conf.set(key, value);
//					System.out.println(key + ": " + value);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Consider using a session
		if (useSession == Session.YES) {
			conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
			conf.setBoolean("USE_TEZ_SESSION", true);
		} else {
			conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
			conf.setBoolean("USE_TEZ_SESSION", false);
		}

		// If using Timeline Server
		if (timelineServer == TimelineServer.ENABLED) {
			TestSession.logger.info("Timeline server is enabled for test "
					+ testName);
			conf.set("tez.history.logging.service.class",
					"org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService");
			conf.setBoolean("yarn.timeline-service.enabled", true);
		}

		// Set the staging dir
		String user = UserGroupInformation.getCurrentUser().getShortUserName();
		FileSystem fs = FileSystem.get(conf);

		Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID()
				.toString());
		conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());

		conf.set("mapreduce.framework.name", "yarn-tez");

		/**
		 * Int value. Time (in seconds) for which the Tez AM should wait for a
		 * DAG to be submitted before shutting down. Only relevant in session
		 * mode.
		 */

		conf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, 30);
		conf.set("tez.lib.uris", "${fs.defaultFS}/sharelib/v1/tez/ytez-"
				+ tezVersion
				+ "/libexec/tez,${fs.defaultFS}/sharelib/v1/tez/ytez-"
				+ tezVersion + "/libexec/tez/lib");

		return new TezConfiguration(conf);
	}


	/**
	 * If you run this command readlink /home/gs/conf/tez you would get
	 * "/grid/0/Releases/tez_conf-0.5.0.1408211546/tez/conf/tez" Notice that
	 * /home/gs/conf (and also /home/gs/tez) are symbolic links pointing to the
	 * place where the actual HTF has been stored. One can install multiple
	 * versions of Tez and use any one of them by changing the symbolic link.
	 * Hence I've provided a function to read the current version of Tez that is
	 * currently in play.
	 * 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static String getTezVersion() throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("/bin/readlink");
		sb.append(" ");
		sb.append("/home/gs/tez/current");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");

		Process process = TestSession.exec.runProcBuilderGetProc(commandFrags,
				null);
		process.waitFor();
		Assert.assertEquals(process.exitValue(), 0);
		String response = printResponseAndReturnItAsString(process);
		// Response is of the format
		// '/home/gs/gridre/yroot.omegak/tez-0.5.1.0.1410031854/libexec/tez'
		String prefix = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME") + "/tez-";
		response = response.replace(prefix, "");
		String suffix = "/libexec/tez";
		response = response.replace(suffix, "");

		return response.trim();
	}

	public static String getHadoopVersion() throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/bin/hadoop");
		sb.append(" ");
		sb.append("version");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");

		Process process = TestSession.exec.runProcBuilderGetProc(commandFrags,
				null);
		process.waitFor();
		Assert.assertEquals(process.exitValue(), 0);
		String response = printResponseAndReturnItAsString(process);
		// Response is of the format
		// Hadoop 2.5.0.3.1408251445
		// Subversion git@git.corp.yahoo.com:hadoop/Hadoop.git -r
		// 3afaa2c152b18a45f4314808c9d0dd7af75a4f84
		// Compiled by hadoopqa on 2014-08-25T21:55Z
		// Compiled with protoc 2.5.0
		// From source with checksum 408a7f5aafaa8d578542c88dbc39dbd6
		// This command was run using
		// /home/gs/gridre/yroot.tiwaripig/share/hadoop-2.5.0.3.1408251445/share/hadoop/common/hadoop-common-2.5.0.3.1408251445.jar
		// hadoopqa@oxy-oxygen-0a577732 hadooptest>$

		return response.split("\n")[0].split("\\s+")[1].trim();
	}

	private static String printResponseAndReturnItAsString(Process process)
			throws InterruptedException, IOException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line;

		line = reader.readLine();
		while (line != null) {
			sb.append(line);
			sb.append("\n");
			TestSession.logger.debug(line);
			line = reader.readLine();
		}

		process.waitFor();
		return sb.toString();
	}

	/**
	 * This method, generates a shell script on the fly and then runs it to
	 * generate data used by the Test. TODO: Move this into a htf_pig_data
	 * package. Alternately consider renaming that package htf_test_data
	 * package.
	 * 
	 * @param shellScriptGoesHere
	 * @param dataGenByShellScriptGoesHere
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void generateTestData(String shellScriptGoesHere,
			String dataGenByShellScriptGoesHere) throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("#/bin/bash" + "\n");
		sb.append("i=1000" + "\n");
		sb.append("j=1000" + "\n");
		sb.append("id=0" + "\n");
		sb.append("while [[ \"$id\" -ne \"$i\" ]]" + "\n");
		sb.append("do" + "\n");
		sb.append("id=`expr $id + 1`" + "\n");
		sb.append("deptId=`expr $RANDOM % $j + 1`" + "\n");
		sb.append("deptName=`echo \"ibase=10;obase=16;$deptId\" | bc`" + "\n");
		sb.append("echo \"$id O$deptName\"" + "\n");
		sb.append("done" + "\n");

		File file = new File(shellScriptGoesHere);

		if (file.exists()) {
			if (!file.canExecute()) {
				file.setExecutable(true);
			}
		} else {
			file.createNewFile();
			Writer writer = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(writer);
			bw.write(sb.toString());
			bw.close();
			writer.close();
			file.setExecutable(true);
		}

		ProcessBuilder builder = new ProcessBuilder("sh",
				file.getAbsolutePath());
		builder.redirectOutput(new File(dataGenByShellScriptGoesHere));
		builder.redirectErrorStream();
		Process proc = builder.start();
		Assert.assertTrue(proc.waitFor() == 0);
		Assert.assertEquals(proc.exitValue(), 0);

	}

}