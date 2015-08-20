package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class DfsTestsBaseClass extends TestSession {
	/**
	 * Identity files needed for SSH
	 */
	public static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/home/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";
	public static final String HADOOPQA_AS_MAPREDQA_IDENTITY_FILE = "/home/hadoopqa/.ssh/flubber_hadoopqa_as_mapredqa";
	public static final String HADOOPQA_BLUE_DSA = "/home/hadoopqa/.ssh/blue_dsa";
	public final String HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";

	/**
	 * Data structures for creating initial files
	 */
	protected static HashMap<String, String> fileMetadata = new HashMap<String, String>();
	protected static HashMap<String,Integer> fileMetadataPerf = new HashMap<String,Integer>();
	protected static Set<String> setOfTestDataFilesInHdfs;
	protected static Set<String> setOfTestDataFilesInLocalFs;
	public static final String INPUT_TO_WORD_COUNT = "input_to_word_count.txt";

        public static final String DATA_DIR_IN_HDFS =
            System.getProperty("HDFS_TEST_DATA_DIR", "/HTF/testdata/dfs/");

	public static final String GRID_0 = "/grid/0/";
	public static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "tmp/HTF/testdata/dfs/";
	public static final String THREE_GB_FILE_NAME = "3GbFile.txt";
	public static final String ONE_BYTE_FILE = "file_1B";

    // TODO: rename to webhdfs_to_hdfs
	// For ycrypt proxy performance testing
	public static boolean crosscoloPerf =
	        Boolean.parseBoolean(System.getProperty("CROSS_COLO_PERF", "false"));
    // TODO: rename to hdfs_to_hdfs
	// For http proxy performance testing
	public static boolean crossclusterPerf =
            Boolean.parseBoolean(System.getProperty("CROSS_CLUSTER_PERF", "false"));
	
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public static final String EMPTY_FS_ENTITY = "";
	public final ArrayList<String> DFSADMIN_VAR_ARG_ARRAY = new ArrayList<String>();
	public final String KRB5CCNAME = "KRB5CCNAME";

	public static HashMap<String, Boolean> pathsChmodedSoFar = new HashMap<String, Boolean>();
	protected String localCluster = System.getProperty("CLUSTER_NAME");
	
	public static int payload = 
	        Integer.parseInt(System.getProperty("CROSS_COLO_PERF_PAYLOAD", "3072"));
    public static String perfFileSizes = 
            System.getProperty("CROSS_COLO_PERF_SIZES", "16M,512M,1G,3G");
	/**
	 * Hadoop job defines
	 */

	/**
	 * Options passed to CLI commands
	 * 
	 */
	public static enum Recursive {
		YES, NO
	};

	public static enum Force {
		YES, NO
	};

	public static enum SkipTrash {
		YES, NO
	};

	public static enum ClearQuota {
		YES, NO
	};

	public static enum SetQuota {
		YES, NO
	};

	public static enum ClearSpaceQuota {
		YES, NO
	};

	public static enum SetSpaceQuota {
		YES, NO
	};

	public static enum Report {
		YES, NO
	};

	public static enum PrintTopology {
		YES, NO
	};
	
	@BeforeClass
	public static void ensureDataPresenceInCluster() throws Exception{
		TestSession.start();
		ensureLocalFilesPresentBeforeTestRun();
		if ((DfsTestsBaseClass.crosscoloPerf == true) ||
		        (DfsTestsBaseClass.crossclusterPerf == true)) {
		    copyFilesIntoClusterForPerf(System.getProperty("CLUSTER_NAME"));
		} else {
		    copyFilesIntoCluster(System.getProperty("CLUSTER_NAME"));
		}
	}

    public static void setFileMetadata() {
    	DecimalFormat df = new DecimalFormat("#");
    	Double dbl = new Double((double) 0);
    	fileMetadata.put("file_empty", df.format(dbl));
        
        /*
         * The file below actually ends up putting 2 bytes, because it is a
         * double
         */
        dbl = new Double((double) 1);
        fileMetadata.put(ONE_BYTE_FILE,df.format(dbl));
        
        // 64 MB file size variations
        dbl = new Double((double) 64 * 1024 * 1024) - 1;
        fileMetadata.put("file_1_byte_short_of_64MB", df.format(dbl));

        dbl = new Double((double) 64 * 1024 * 1024);
        fileMetadata.put("file_64MB", df.format(dbl));

        dbl = new Double((double) 64 * 1024 * 1024) + 1;
        fileMetadata.put("file_1_byte_more_than_64MB", df.format(dbl));


        // 128 MB file size variations
        dbl = new Double((double) 128 * 1024 * 1024) - 1;
        fileMetadata.put("file_1_byte_short_of_128MB", df.format(dbl));

        dbl = new Double((double) 128 * 1024 * 1024);
        fileMetadata.put("file_128MB", df.format(dbl));

        dbl = new Double((double) 128 * 1024 * 1024) + 1;
        fileMetadata.put("file_1_byte_more_than_128MB", df.format(dbl));

        dbl = new Double((double) 255 * 1024 * 1024);
        fileMetadata.put("file_255MB", df.format(dbl));

        dbl = new Double((double) 256 * 1024 * 1024);
        fileMetadata.put("file_256MB", df.format(dbl));

        dbl = new Double((double) 257 * 1024 * 1024);
        fileMetadata.put("file_257MB", df.format(dbl));

        dbl = new Double((double) 767 * 1024 * 1024);
        fileMetadata.put("file_767MB", df.format(dbl));

        dbl = new Double((double) 768 * 1024 * 1024);
        fileMetadata.put("file_768MB", df.format(dbl));

        dbl = new Double((double) 769 * 1024 * 1024);
        fileMetadata.put("file_769MB", df.format(dbl));

        // Huge file
      dbl = new Double(((double) ((double) (double) 10 * (double) 1024
			     * 1024 * 1024) + (double) (700 * 1024 * 1024)));

      fileMetadata.put("file_11GB", df.format(dbl));

      dbl = new Double(((double) ((double) (double) 1 * (double) 1024 * 1024 * 1024)));
      fileMetadata.put(INPUT_TO_WORD_COUNT, df.format(dbl));

    }

    public static void setFileMetadataForPerf(String fileSize, int numFiles) {
        fileMetadataPerf.put("file_"+fileSize, numFiles);
        for(int i = 1; i <= numFiles; i++) {
            fileMetadata.put("file_"+fileSize+"_"+i, fileSize);
        }
    }
    
    private static int parseFileSize(String size) {
        String unit = size.substring(size.length() - 1); 
        int value = Integer.parseInt(size.substring(0, size.length()-1));
        if (unit.equals("G")) {
            value = value*1024;
        }
        return value;
    }
    
    public static void setFileMetadataForPerf() {
        TestSession.logger.info("Payload: " + payload);
        int sizeMB;
        for (String size : perfFileSizes.split(",")) {
            sizeMB = parseFileSize(size);
            TestSession.logger.info("File size: " + size + ": " + sizeMB);
            setFileMetadataForPerf(size, payload/sizeMB);
        }        
        
        /*
        setFileMetadataForPerf("16M", payload/16);
        setFileMetadataForPerf("512M", payload/512);
        setFileMetadataForPerf("1G", payload/1024);
        setFileMetadataForPerf("3G", payload/(1024*3));
        */
    }

	public static void ensureLocalFilesPresentBeforeTestRun() {

        if ((DfsTestsBaseClass.crosscoloPerf == true) ||
                (DfsTestsBaseClass.crossclusterPerf == true)) {
	        setFileMetadataForPerf();
	    } else {
	        setFileMetadata();	        
	    }

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

	static void ensureDirsArePresentInHdfs(String cluster) throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, "", cluster, DfsTestsBaseClass.DATA_DIR_IN_HDFS);

	}
	
	public static void copyFilesIntoCluster(String cluster) throws Exception{
		ensureDirsArePresentInHdfs(cluster);
		doChmodRecursively(cluster,DfsTestsBaseClass.DATA_DIR_IN_HDFS);
		
		DfsTestsBaseClass.ensureLocalFilesPresentBeforeTestRun();
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		for (String aFile: DfsTestsBaseClass.fileMetadata.keySet()){
			genericCliResponse = dfsCliCommands.test(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, "",
					cluster,
					DfsTestsBaseClass.DATA_DIR_IN_HDFS + aFile,
					DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
			if (genericCliResponse.process.exitValue() != 0) {
				genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "",
						System.getProperty("CLUSTER_NAME"),
						DfsTestsBaseClass.DATA_DIR_IN_LOCAL_FS + aFile,
						DfsTestsBaseClass.DATA_DIR_IN_HDFS + aFile);
				dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP, 
				        HadooptestConstants.UserNames.HADOOPQA, "", 
				        cluster, 
				        DfsTestsBaseClass.DATA_DIR_IN_HDFS + aFile, "777", Recursive.NO);
			}
		}
	}

	public static void copyFilesIntoClusterForPerf(String cluster) throws Exception{
		ensureDirsArePresentInHdfs(cluster);
		doChmodRecursively(cluster,DfsTestsBaseClass.DATA_DIR_IN_HDFS);
		
		DfsTestsBaseClass.ensureLocalFilesPresentBeforeTestRun();
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		String filePattern;
		for (String aFile: DfsTestsBaseClass.fileMetadataPerf.keySet()){
		    filePattern = aFile + "_{1.." + fileMetadataPerf.get(aFile) + "}";
			genericCliResponse = dfsCliCommands.test(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, "",
					cluster,
					DfsTestsBaseClass.DATA_DIR_IN_HDFS + filePattern,
					DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
			if (genericCliResponse.process.exitValue() != 0) {
				genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
				        HadooptestConstants.UserNames.HADOOPQA, "",
						System.getProperty("CLUSTER_NAME"),
						DfsTestsBaseClass.DATA_DIR_IN_LOCAL_FS + filePattern,
						DfsTestsBaseClass.DATA_DIR_IN_HDFS);
				dfsCliCommands.chmod(EMPTY_ENV_HASH_MAP, 
				        HadooptestConstants.UserNames.HADOOPQA, "", 
				        cluster, 
				        DfsTestsBaseClass.DATA_DIR_IN_HDFS + filePattern, "777", Recursive.NO);
			}
		}
	}
	      		
	public static void doChmodRecursively(String cluster, String dirHierarchy)
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
						"777", Recursive.NO);
				pathsChmodedSoFar.put(pathSoFar, true);
			}
		}
	}

	/*
	 * called by @Before
	 */
	public static void createLocalPreparatoryFiles() {
		for (String aFileName : fileMetadata.keySet()) {
			TestSession.logger.info("!!!!!!! Checking local file:"
					+ DATA_DIR_IN_LOCAL_FS + aFileName);
			File attemptedFile = new File(DATA_DIR_IN_LOCAL_FS + aFileName);
			if (attemptedFile.exists()) {
				TestSession.logger.info(attemptedFile
						+ " already exists, not recreating it");
				continue;
			}
			TestSession.logger.info("!!!!!!! CREATING local file:"
					+ DATA_DIR_IN_LOCAL_FS + aFileName);
			
			// create a file on the local fs
			if (!attemptedFile.getParentFile().exists()) {
				attemptedFile.getParentFile().mkdirs();
			}

			actualFileCreation(DATA_DIR_IN_LOCAL_FS + aFileName,
					fileMetadata.get(aFileName));


		}

	}


    /**
     * Overloaded method
     * @param completeLocalName
     * @param size
     */
    private static void actualFileCreation(String completeLocalName, String size)  {
        try {
            if (size.equals("0")) {
                actualFileCreation(completeLocalName, new Double((double) 0));
            } else {
                String[] command = 
                    {"/usr/bin/fallocate", "-l", size, completeLocalName};
        		Process process = TestSession.exec.runProcBuilderSecurityGetProc(
        				command,
        		        HadooptestConstants.UserNames.HADOOPQA);
        		DfsCliCommands dfsCommonCli = new DfsCliCommands();
        		dfsCommonCli.printResponseAndReturnItAsString(process);
        		Assert.assertTrue("fallocate command errored!", process.exitValue() == 0);

            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

	/**
	 * Overloaded method
	 * @param completeLocalName
	 * @param size
	 */
	private static void actualFileCreation(String completeLocalName, Double size)  {
		PrintWriter out;
		try {
			out = new PrintWriter(completeLocalName);
			if (size == 0){
				out.close();
				return;
			}
			do {				
				if (size%20 == 0)
					out.println("");
				else
					out.print("a");
			} while (--size >0);
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		TestSession.logger.info(commandString);
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
	
	
}
