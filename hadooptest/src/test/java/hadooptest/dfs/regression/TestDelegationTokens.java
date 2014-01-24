package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class TestDelegationTokens extends TestSession{
	static Logger logger = Logger.getLogger(TestDelegationTokens.class);

	private static final String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
	private static final String GRID_0 = "/grid/0/";
	private static final String DATA_DIR_IN_LOCAL_FS = GRID_0
			+ "HTF/testdata/dfs/";
	private static final String ONE_BYTE_FILE = "file_1B";

	// Supporting Data
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
	File fileStoringDelagationTokenReceivedViaDfsCommand;
	File fileStoringDelegationTokenReceivedViaKinit;
	/*
	 * Creates a temporary directory before each test run to create a 3gb file.
	 */
	@Rule
	public TemporaryFolder tempDelegationTokenFolder = new TemporaryFolder();

	@Before
	public void beforeEachTest() throws IOException {
		 fileStoringDelagationTokenReceivedViaDfsCommand = tempDelegationTokenFolder.newFile("delegation.token.received.via.dfs.command");
		 fileStoringDelegationTokenReceivedViaKinit = tempDelegationTokenFolder.newFile("delegation.token.received.via.kinit");
		
	}
	@Test
	public void test() throws Exception{

		logger.info("Absolute file:" + fileStoringDelagationTokenReceivedViaDfsCommand.getAbsoluteFile());
		logger.info("Absolute path:" + fileStoringDelagationTokenReceivedViaDfsCommand.getAbsolutePath());
		logger.info("Canonical path:" + fileStoringDelagationTokenReceivedViaDfsCommand.getCanonicalPath());
		logger.info("Name:" + fileStoringDelagationTokenReceivedViaDfsCommand.getName());
		logger.info("Path:" + fileStoringDelagationTokenReceivedViaDfsCommand.getPath());
		
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		
		dfsCliCommands.fetchDelegationTokenViaHdfsCli(System.getProperty("CLUSTER_NAME"), 
				HadooptestConstants.UserNames.HADOOPQA, 
				fileStoringDelagationTokenReceivedViaDfsCommand.getPath(), 
				HadooptestConstants.Schema.HDFS);
		
		
		BufferedReader buffReader = new BufferedReader(new FileReader(fileStoringDelagationTokenReceivedViaDfsCommand.getPath()));
		String line;
		while ((line = buffReader.readLine()) != null){
			logger.info(line);
		}
		buffReader.close();
		
		logger.info("Absolute file:" + fileStoringDelegationTokenReceivedViaKinit.getAbsoluteFile());
		logger.info("Absolute path:" + fileStoringDelegationTokenReceivedViaKinit.getAbsolutePath());
		logger.info("Canonical path:" + fileStoringDelegationTokenReceivedViaKinit.getCanonicalPath());
		logger.info("Name:" + fileStoringDelegationTokenReceivedViaKinit.getName());
		logger.info("Path:" + fileStoringDelegationTokenReceivedViaKinit.getPath());

	}
}
