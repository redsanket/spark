package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Bug: 6801150 kinit, set KRB5CCNAME="FILE:<path to ticket cache>", run a
 * fsshell command. the key is the inclusion of the "FILE:" prefix. command will
 * fail if wrong ugi is used. Verified the KRBCCNAME handles paths preceded with
 * FILE:
 * 
 * @author tiwari
 * 
 */

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestKRB5CCNAMEPrependedWithFILE extends DelegationTokenBaseClass {
	String protocol;
	static String TEMP_LOCATION_OF_CACHE_FILE = "/tmp/TestKRB5CCNAMEPrependedWithFILE";

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "webhdfs" }, { "hdfs" }, });
	}

	public TestKRB5CCNAMEPrependedWithFILE(String protocol) {
		this.protocol = protocol;
	}

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Test
	public void runTestPrependKrb5ccnameWithFileColon() throws Exception {
		// Start the background thread to do 'kdestroy'
		startKinitCacheDestroyer();

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				TEMP_LOCATION_OF_CACHE_FILE, null);

		HashMap<String, String> envHashMap = new HashMap<String, String>();
		envHashMap.put(KRB5CCNAME, TEMP_LOCATION_OF_CACHE_FILE);
		genericCliResponseBO = dfsCliCommands.ls(envHashMap,
				HadooptestConstants.UserNames.HADOOPQA, protocol,
				System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS,
				Recursive.YES);
		Assert.assertTrue(
				"'ls' command bombed for some reason, gave a non zero code",
				genericCliResponseBO.process.exitValue() == 0);

		// Run the same command, pass an empty KRB5CCNAME value. It should
		// fail
		envHashMap.clear();
		envHashMap.put(KRB5CCNAME, "junk");
		genericCliResponseBO = dfsCliCommands.ls(envHashMap,
				HadooptestConstants.UserNames.HADOOPQA, protocol,
				System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS,
				Recursive.YES);
		Assert.assertTrue(
				"'ls' command shouldn't have worked, as I've not passed "
						+ "any KRB5CCNAME and the background thread would have "
						+ "cleaned up the cache regularly",
				genericCliResponseBO.process.exitValue() != 0);

		// Re-run the command, this time precede the value of KRB5CCNAME with
		// FILE:
		envHashMap.clear();
		envHashMap.put(KRB5CCNAME, "FILE:" + TEMP_LOCATION_OF_CACHE_FILE);
		genericCliResponseBO = dfsCliCommands.ls(envHashMap,
				HadooptestConstants.UserNames.HADOOPQA, protocol,
				System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS,
				Recursive.YES);
		Assert.assertTrue(
				"'ls' command failed when KRB5CCNAME value prepended with FILE:",
				genericCliResponseBO.process.exitValue() == 0);

		stopKinitCacheDestroyer();

	}
}