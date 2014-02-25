package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class DfsBaseClass extends TestSession {
	/**
	 * Identity files needed for SSH
	 */
	public static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";
	public static final String HADOOPQA_BLUE_DSA = "/homes/hadoopqa/.ssh/blue_dsa";
	public final String HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";

	/**
	 * Data structures for creating initial files
	 */
	static HashMap<String, Double> fileMetadata = new HashMap<String, Double>();
	protected Set<String> setOfTestDataFilesInHdfs;
	protected Set<String> setOfTestDataFilesInLocalFs;

	public static final String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
	public static final String GRID_0 = "/grid/0/";
	public static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/dfs/";
	public static final String THREE_GB_FILE_NAME = "3GbFile.txt";
	public static final String ONE_BYTE_FILE = "file_1B";

	public final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public static final String EMPTY_FS_ENTITY = "";
	public final ArrayList<String> DFSADMIN_VAR_ARG_ARRAY = new ArrayList<String>();
	public final String KRB5CCNAME = "KRB5CCNAME";

	HashMap<String, Boolean> pathsChmodedSoFar;
	protected String localCluster = System.getProperty("CLUSTER_NAME");

	/**
	 * Hadoop job defines
	 */
	

	/**
	 * Options passed to CLI commands
	 * 
	 */
	public enum Recursive {
		YES, NO
	};

	public enum Force {
		YES, NO
	};

	public enum SkipTrash {
		YES, NO
	};

	public enum ClearQuota {
		YES, NO
	};

	public enum SetQuota {
		YES, NO
	};

	public enum ClearSpaceQuota {
		YES, NO
	};

	public enum SetSpaceQuota {
		YES, NO
	};

	public enum Report {
		YES, NO
	};

	public enum PrintTopology {
		YES, NO
	};

	// ----------------------------------------------------------//
	@Before
	public void ensureDataBeforeTestRun() {

		fileMetadata.put("file_empty", new Double((double) 0));
		/*
		 * The file below actually ends up putting 2 bytes, because it is a
		 * double
		 */
		fileMetadata.put(ONE_BYTE_FILE, new Double((double) 1));
		// 64 MB file size variations
		fileMetadata.put("file_1_byte_short_of_64MB", new Double(
				(double) 64 * 1024 * 1024) - 1);
		fileMetadata.put("file_64MB", new Double((double) 64 * 1024 * 1024));
		fileMetadata.put("file_1_byte_more_than_64MB", new Double(
				(double) 64 * 1024 * 1024) + 1);

		// 128 MB file size variations
		fileMetadata.put("file_1_byte_short_of_128MB", new Double(
				(double) 128 * 1024 * 1024) - 1);
		fileMetadata.put("file_128MB", new Double((double) 128 * 1024 * 1024));
		fileMetadata.put("file_1_byte_more_than_128MB", new Double(
				(double) 128 * 1024 * 1024) + 1);

		fileMetadata.put("file_255MB", new Double((double) 255 * 1024 * 1024));
		fileMetadata.put("file_256MB", new Double((double) 256 * 1024 * 1024));
		fileMetadata.put("file_257MB", new Double((double) 257 * 1024 * 1024));

		fileMetadata.put("file_767MB", new Double((double) 767 * 1024 * 1024));
		fileMetadata.put("file_768MB", new Double((double) 768 * 1024 * 1024));
		fileMetadata.put("file_769MB", new Double((double) 769 * 1024 * 1024));
		// Huge file
		fileMetadata.put("file_11GB",
				new Double(((double) ((double) (double) 10 * (double) 1024
						* 1024 * 1024) + (double) (700 * 1024 * 1024))));

		setOfTestDataFilesInHdfs = new HashSet<String>();
		setOfTestDataFilesInLocalFs = new HashSet<String>();

		for (String aFileName : fileMetadata.keySet()) {
			// Working set of Files on HDFS
			setOfTestDataFilesInHdfs.add(DATA_DIR_IN_HDFS + aFileName);
			// Working set of Files on Local FS
			setOfTestDataFilesInLocalFs.add(DATA_DIR_IN_LOCAL_FS + aFileName);
		}

		createLocalPreparatoryFiles();

	}

	public void doChmodRecursively(String cluster, String dirHierarchy)
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty())
				continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir + "/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			if (!pathsChmodedSoFar.containsKey(pathSoFar)) {
				dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.WEBHDFS, cluster, pathSoFar,
						"777");
				pathsChmodedSoFar.put(pathSoFar, true);
			}
		}
	}

	/*
	 * called by @Before
	 */
	void createLocalPreparatoryFiles() {
		for (String aFileName : fileMetadata.keySet()) {
			TestSession.logger.info("!!!!!!! Checking local file:"
					+ DATA_DIR_IN_LOCAL_FS + aFileName);
			File attemptedFile = new File(DATA_DIR_IN_LOCAL_FS + aFileName);
			if (attemptedFile.exists()) {
				TestSession.logger.info(attemptedFile
						+ " already exists, not recreating it");
				continue;
			}
			TestSession.logger.info("!!!!!!! Creating local file:"
					+ DATA_DIR_IN_LOCAL_FS + aFileName);
			// create a file on the local fs
			if (!attemptedFile.getParentFile().exists()) {
				attemptedFile.getParentFile().mkdirs();
			}
			FileOutputStream fout;
			try {
				fout = new FileOutputStream(attemptedFile);
				int macroStepSize = 1;
				int macroLoopCount = 1;
				int microLoopCount = 0;
				if ((int) (fileMetadata.get(aFileName) / (1024 * 1024 * 1024)) > 0) {
					macroStepSize = 1024 * 1024 * 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					TestSession.logger
							.info("Processing: "
									+ aFileName
									+ " size:"
									+ fileMetadata.get(aFileName)
									+ " stepSize: "
									+ macroStepSize
									+ " because: "
									+ (int) (fileMetadata.get(aFileName) / (1024 * 1024 * 1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else if ((int) (fileMetadata.get(aFileName) / (1024 * 1024)) > 0) {
					macroStepSize = 1024 * 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					TestSession.logger
							.info("Processing: "
									+ aFileName
									+ " size:"
									+ fileMetadata.get(aFileName)
									+ " stepSize: "
									+ macroStepSize
									+ " because: "
									+ (int) (fileMetadata.get(aFileName) / (1024 * 1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else if ((int) (fileMetadata.get(aFileName) / (1024)) > 0) {
					macroStepSize = 1024;
					macroLoopCount = (int) (fileMetadata.get(aFileName) / macroStepSize);
					TestSession.logger.info("Processing: " + aFileName
							+ " size:" + fileMetadata.get(aFileName)
							+ " stepSize: " + macroStepSize + " because: "
							+ (int) (fileMetadata.get(aFileName) / (1024)));
					microLoopCount = (int) (fileMetadata.get(aFileName) % (macroStepSize * macroLoopCount));
				} else {
					macroLoopCount = 0;
					macroStepSize = 0;
					TestSession.logger.info("Processing: " + aFileName
							+ " size:" + fileMetadata.get(aFileName)
							+ " stepSize: " + macroStepSize);
					microLoopCount = (int) (fileMetadata.get(aFileName) % (1024));
				}
				for (double i = 0; i < macroLoopCount; i++) {
					fout.write(new byte[(int) macroStepSize]);
				}

				for (int i = 0; i < microLoopCount; i++) {
					fout.write(new byte[1]);
				}

				fout.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

	}

	public boolean create3GbFile(TemporaryFolder tempFolder) throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		tempFolder.newFile(THREE_GB_FILE_NAME);
		StringBuilder sb = new StringBuilder();
		sb.append("/bin/dd");
		sb.append(" ");
		sb.append("if=/dev/zero");
		sb.append(" ");
		sb.append("of=" + tempFolder.getRoot() + "/" + THREE_GB_FILE_NAME);
		sb.append(" ");
		sb.append("bs=10240");
		sb.append(" ");
		sb.append("count=300000");
		sb.append(" ");
		String commandString = sb.toString();
		logger.info(commandString);
		String[] commandFrags = commandString.split("\\s+");

		Process process = null;
		Map<String, String> envToUnsetHadoopPrefix = new HashMap<String, String>();

		process = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
				commandFrags, HadooptestConstants.UserNames.HDFSQA,
				envToUnsetHadoopPrefix);
		dfsCommonCli.printResponseAndReturnItAsString(process);
		if (process.exitValue() == 0) {
			return true;
		} else {
			return false;
		}

	}

	public static class MyLogger implements com.jcraft.jsch.Logger {
		static java.util.Hashtable name = new java.util.Hashtable();
		static {
			name.put(new Integer(DEBUG), "DEBUG: ");
			name.put(new Integer(INFO), "INFO: ");
			name.put(new Integer(WARN), "WARN: ");
			name.put(new Integer(ERROR), "ERROR: ");
			name.put(new Integer(FATAL), "FATAL: ");
		}

		public boolean isEnabled(int level) {
			return true;
		}

		public void log(int level, String message) {
			System.err.print(name.get(new Integer(level)));
			System.err.println(message);
		}
	}

	public String doJavaSSHClientExec(String user, String host, String command,
			String identityFile) {
		JSch jsch = new JSch();

		// JSch.setLogger(new MyLogger());

		TestSession.logger.info("SSH Client is about to run command:" + command
				+ " on host:" + host + "as user:" + user
				+ " using identity file:" + identityFile);
		Session session;
		StringBuilder sb = new StringBuilder();
		try {
			session = jsch.getSession(user, host, 22);
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();

			channel.connect();

			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
					TestSession.logger.info(outputFragment);
					sb.append(outputFragment);
				}
				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: "
							+ channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {
				}
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	public class MyUserInfo implements UserInfo {

		public String getPassphrase() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptPassword(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptYesNo(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public void showMessage(String arg0) {
			// TODO Auto-generated method stub

		}

	}

	String getHostNameFromIp(String ip) throws Exception {

		InetAddress iaddr = InetAddress.getByName(ip);
		System.out.println("And the Host name of the g/w is:"
				+ iaddr.getHostName());
		return iaddr.getHostName();

	}

	/*
	 * called by @Before
	 */
	public void runStdHadoopRandomWriter(HashMap<String, String> jobParams, String randomWriterOutputDirOnHdfs)
			throws Exception {
		logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key:jobParams.keySet()){
			conf.set(key,  jobParams.get(key));
//			conf.setLong(BYTES_PER_MAP, 256000);
		}
		int res = ToolRunner.run(conf, new RandomWriter(),
				new String[] { randomWriterOutputDirOnHdfs });
		Assert.assertEquals(0, res);

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res = ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {
				sortInputDataLocation, sortOutputDataLocation });
		Assert.assertEquals(0, res);

	}

}
