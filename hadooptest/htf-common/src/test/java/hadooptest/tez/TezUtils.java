package hadooptest.tez;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;

public class TezUtils {

	public static String TEZ_SITE_XML = "/home/gs/conf/tez/tez-site.xml";

	public static Configuration setupConfForTez(Configuration conf,
			String mode) throws IOException, InterruptedException {
		String tezVersion = getTezVersion();
		tezVersion = tezVersion.trim();
		TestSession.logger.info("Read back tez version as:" + tezVersion);
		String fsProtocol = conf.get("fs.defaultFS");
		conf.set("tez.lib.uris", "${fs.defaultFS}/sharelib/v1/tez/ytez-"
				+ tezVersion
				+ "/libexec/tez,${fs.defaultFS}/sharelib/v1/tez/ytez-"
				+ tezVersion + "/libexec/tez/lib");

		conf.set("mapreduce.framework.name", "yarn-tez");

		if (mode.equals(HadooptestConstants.Mode.LOCAL)) {
			conf.set("tez.local.mode", "true");
		} else {
			try {
				 applyTezSettingsToAllHosts();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return conf;
	}

	/**
	 * Refer this: http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.0.2/
	 * bk_installing_manually_book/content/rpm-chap-tez-2.html
	 * 
	 * @throws Exception
	 */
	public static void applyTezSettingsToAllHosts() throws Exception {
		String[] allTezComponentTypes = new String[] {
				HadooptestConstants.NodeTypes.HISTORY_SERVER,
				HadooptestConstants.NodeTypes.NODE_MANAGER,
				HadooptestConstants.NodeTypes.DATANODE,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER };
		ExecutorService bounceThreadPool = Executors
				.newFixedThreadPool(allTezComponentTypes.length);
		ArrayList<Thread> spawnedThreads = new ArrayList<Thread>();
		for (final String aTezComponentType : allTezComponentTypes) {
			Runnable aRunnableBouncer = new Runnable() {

				public void run() {
					FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
							.getCluster();

					try {
						fullyDistributedCluster.getConf(aTezComponentType)
								.backupConfDir();

						String dirWhereConfHasBeenCopiedOnTheRemoteMachine = fullyDistributedCluster
								.getConf(aTezComponentType).getHadoopConfDir();

						copyTezSiteXmlOverToHadoopHost(aTezComponentType,
								dirWhereConfHasBeenCopiedOnTheRemoteMachine);

						TestSession.logger.info("Backed up "
								+ aTezComponentType + " conf dir in :"
								+ dirWhereConfHasBeenCopiedOnTheRemoteMachine);

						// Since setting the HadoopConf file also backs up the
						// config dir,
						// wait for a few seconds.
						Thread.sleep(10000);
						/**
						 * Commenting out the statement below, 'cos got a clarification
						 * from Jon on Aug 10th This isn't needed for our setup unless
						 * we want all map reduce jobs to use tez. All tez jobs (pig on
						 * tez and tez map reduce examples) do this automatically.
						 */
//						fullyDistributedCluster.getConf(aTezComponentType)
//								.setHadoopConfFileProp(
//										"mapreduce.framework.name", "yarn",
//										"mapred-site.xml", null);

						fullyDistributedCluster.getConf(aTezComponentType)						
						.setHadoopConfFileProp(
								"mapreduce.job.acl-view-job", "*",
								"yarn-site.xml", null);

						// Bounce the node
						fullyDistributedCluster.hadoopDaemon(Action.STOP,
								aTezComponentType);
						fullyDistributedCluster.hadoopDaemon(Action.START,
								aTezComponentType);

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			};
			Thread t = new Thread(aRunnableBouncer);
			t.start();
			spawnedThreads.add(t);
		}
		for (Thread t : spawnedThreads) {
			t.join();
		}
		Thread.sleep(10000);
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		dfsCommonCli.dfsadmin(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, Report.NO,
				"leave", ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO,
				DfsTestsBaseClass.EMPTY_FS_ENTITY);
		dfsCommonCli.dfsadmin(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, Report.NO,
				"get", ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO,
				DfsTestsBaseClass.EMPTY_FS_ENTITY);

	}

	private static void copyTezSiteXmlOverToHadoopHost(String nodeType,
			String remoteCongfigLocation) throws Exception {
		String[] hostnames = TestSession.cluster.getNodeNames(nodeType);
		FullyDistributedExecutor fde = new FullyDistributedExecutor();

		StringBuilder copySb = new StringBuilder();
		for (String aHostName : hostnames) {
			// mkdir
			String mkdirString = "/bin/mkdir " + remoteCongfigLocation + "/tez";
			try {
				fde.runProcBuilder(mkdirString.split("\\s+"));
			} catch (Exception e) {
				e.printStackTrace();
			}

			// copy
			copySb.append("scp " + TEZ_SITE_XML + " hadoopqa@" + aHostName
					+ ":" + remoteCongfigLocation + "tez/tez-site.xml	");
			String command = copySb.toString();
			fde.runProcBuilder(command.split("\\s+"));
		}

	}

	public static String getTezVersion() throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("/bin/readlink");
		sb.append(" ");
		sb.append("/home/gs/tez");

		String commandString = sb.toString();
		TestSession.logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");

		Process process = TestSession.exec.runProcBuilderGetProc(commandFrags,
				null);
		process.waitFor();
		Assert.assertEquals(process.exitValue(), 0);
		String response = printResponseAndReturnItAsString(process);
		// Response is of the format
		// '/grid/0/Releases/tez-0.5.0.1408070056/libexec/tez'
		response = response.replace("/grid/0/Releases/tez-", "");
		response = response.replace("/libexec/tez", "");

		return response;
	}

	private static String printResponseAndReturnItAsString(Process process)
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();
		return sb.toString();
	}

	private static void generateTestData() throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("#/bin/bash" + "\n");
		sb.append("i=1000000" + "\n");
		sb.append("j=1000" + "\n");
		sb.append("id=0" + "\n");
		sb.append("while [[ \"$id\" -ne \"$i\" ]]" + "\n");
		sb.append("do" + "\n");
		sb.append("id=`expr $id + 1`" + "\n");
		sb.append("deptId=`expr $RANDOM % $j + 1`" + "\n");
		sb.append("deptName=`echo \"ibase=10;obase=16;$deptId\" | bc`" + "\n");
		sb.append("echo \"$id O$deptName\"" + "\n");
		sb.append("done" + "\n");

		File file = new File("~/dataCreationScriptForTestGroupByOrderByMRR.sh");

		if (file.exists()) {
			if (!file.canExecute()) {
				file.setExecutable(true);
			}
		} else {
			Writer writer = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(writer);
			bw.write(sb.toString());
			bw.close();
			file.setExecutable(true);
			sb = new StringBuilder();
			sb.append("/bin/bash" + " ");
			sb.append(file.getAbsolutePath());
			String commandString = sb.toString();
			TestSession.logger.info(commandString);
			String[] commandFrags = commandString.split("\\s+");
			Process process = TestSession.exec.runProcBuilderGetProc(
					commandFrags, null);
			process.waitFor();
			Assert.assertEquals(process.exitValue(), 0);

		}

	}

}
