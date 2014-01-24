package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.rules.TemporaryFolder;

public class DfsBaseClass extends TestSession {
	static HashMap<String, Double> fileMetadata = new HashMap<String, Double>();
	private Set<String> setOfTestDataFilesInHdfs;
	private Set<String> setOfTestDataFilesInLocalFs;

	public static final String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
	public static final String GRID_0 = "/grid/0/";
	public static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/dfs/";
	public static final String THREE_GB_FILE_NAME = "3GbFile.txt";
	public static final String ONE_BYTE_FILE = "file_1B";

	HashMap<String, Boolean> pathsChmodedSoFar;
	protected String localCluster = System.getProperty("CLUSTER_NAME");

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
		// // Huge file
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
				dfsCommonCli.chmod(null, HadooptestConstants.UserNames.HDFSQA,
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

}