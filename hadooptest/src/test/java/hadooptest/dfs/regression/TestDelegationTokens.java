package hadooptest.dfs.regression;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.dfs.regression.DfsCliCommands.GenericCliResponseBO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import hadooptest.SerialTests;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestDelegationTokens extends DfsBaseClass {
	String protocol;

	public TestDelegationTokens(String protocol) {
		this.protocol = protocol;
		logger.info("Test invoked for protocol/schema:" + protocol);
	}

	static Logger logger = Logger.getLogger(TestDelegationTokens.class);
	private static final String DELEGATION_TOKEN_TESTS_DIR = "/user/"
			+ HadooptestConstants.UserNames.HADOOPQA
			+ "/delegation_token_tests/";

	File fileStoringDelagationTokenReceivedViaFetchdt;
	File fileStoringDelegationTokenReceivedViaKinit;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
		// Schemas
		// { HadooptestConstants.Schema.WEBHDFS },
		// { "" },
		{ HadooptestConstants.Schema.HDFS }, });
	}

	@Rule
	public TemporaryFolder tempDelegationTokenFolder = new TemporaryFolder();

	@Before
	public void beforeEachTest() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		dfsCommonCliCommands.mkdir(null,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DELEGATION_TOKEN_TESTS_DIR);
		fileStoringDelagationTokenReceivedViaFetchdt = tempDelegationTokenFolder
				.newFile("delegation.token.received.via.dfs.command");
		fileStoringDelegationTokenReceivedViaKinit = tempDelegationTokenFolder
				.newFile("delegation.token.received.via.kinit");
		dfsCommonCliCommands.fetchdt(null,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				null, HadooptestConstants.UserNames.HADOOPQA, null, null, null,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(), null,
				null, null, null, null, null);

	}

	/*
	 * test_SF110_1
	 */
	@Test
	public void test_SF110_1() throws Exception {
		GenericCliResponseBO genericCliResponseBO;
		logger.info("Absolute file:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getName());
		logger.info("Path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), null);
		// BufferedReader buffReader = new BufferedReader(new FileReader(
		// fileStoringDelegationTokenReceivedViaKinit.getPath()));
		// String line;
		// while ((line = buffReader.readLine()) != null) {
		// logger.info(line);
		// }
		// buffReader.close();

		dfsCliCommands.kdestroy(fileStoringDelegationTokenReceivedViaKinit
				.getPath());
		genericCliResponseBO = dfsCliCommands.ls(
				fileStoringDelegationTokenReceivedViaKinit.getPath(),
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);
		// Now, use the token received via fetchdt, it should work
		genericCliResponseBO = dfsCliCommands.ls(
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(),
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		logger.info("Absolute file:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelegationTokenReceivedViaKinit.getName());
		logger.info("Path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getPath());

	}

	/*
	 * test_SF110_2
	 */
	@Test
	public void test_SF110_2() throws Exception {
		GenericCliResponseBO genericCliResponseBO;
		logger.info("Absolute file:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getName());
		logger.info("Path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), "60s");

		// Wait for longer than 60s, let the ticket expire
		Thread.sleep(90000);

		genericCliResponseBO = dfsCliCommands.ls(
				fileStoringDelegationTokenReceivedViaKinit.getPath(),
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		// Now, use the token received via fetchdt, it should work
		genericCliResponseBO = dfsCliCommands.ls(
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(),
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		logger.info("Absolute file:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelegationTokenReceivedViaKinit.getName());
		logger.info("Path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getPath());

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		genericCliResponse = dfsCliCommands.rm(null,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				true, true, true, DELEGATION_TOKEN_TESTS_DIR);

	}

	@After
	public void logTaskResportSummary() {
		// Override to hide the Test Session logs
	}

}
