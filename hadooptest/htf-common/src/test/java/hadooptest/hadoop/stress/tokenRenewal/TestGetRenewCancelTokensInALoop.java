package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Bug: 6804438 Get a filesystem, do an operation like fileStatus("/"), sleep
 * for a little over 30s to let token naturally expire, issue another
 * fileStatus("/") to confirm webhdfs recovers from expired token
 * 
 * @author patw, tiwari
 * 
 */

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestGetRenewCancelTokensInALoop extends DelegationTokenBaseClass {
	static String RENEW_HDFS_DELEGATION_TOKEN_IN_A_LOOP = "RENEW_TOKEN_IN_A_LOOP";
	static String ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP = "ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP";
	static String ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP = "ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP";
	String protocol;

	@Rule
	public ErrorCollector collector = new ErrorCollector();

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "webhdfs" }, { "hdfs", } });
	}

	public TestGetRenewCancelTokensInALoop(String protocol) {
		this.protocol = protocol;
	}

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	/**
	 * Overide the base class method, that bounces the NN and RM nodes. What we
	 * want in this test is to set such token max life durations that the job
	 * complains and fails.
	 */

	@Test
	public void renewTokensTest() throws Exception {
		startKinitCacheDestroyer();
		UserGroupInformation ugiOrig = loginUserFromKeytabAndReturnUGI(
				"hadoopqa@DEV.YGRID.YAHOO.COM",
				HadooptestConstants.Location.Keytab.HADOOPQA);
		UserGroupInformation.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");

		// list out our config prop change
		MultiPurposeDoasBlock doAsForRenewingTokensInLoop = new MultiPurposeDoasBlock(
				ugiOrig, RENEW_HDFS_DELEGATION_TOKEN_IN_A_LOOP);
		doAsForRenewingTokensInLoop.go();

		MultiPurposeDoasBlock doAsForAcquiringTokensInLoop = new MultiPurposeDoasBlock(
				ugiOrig, ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP);
		doAsForAcquiringTokensInLoop.go();

		MultiPurposeDoasBlock doAsForAcquiringAndCancellingTokensInLoop = new MultiPurposeDoasBlock(
				ugiOrig, ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP);
		doAsForAcquiringAndCancellingTokensInLoop.go();

		/*
		 * Added the commented methods below, just to check if running the test
		 * within or without the doAs block makes any difference. It does not,
		 * but what I've learnt is that if you are kinit'ed from the shell then
		 * that influences the API calls within the code. So if you 'kdestroy'
		 * and run just the following methods, then the API call (the 1st one)
		 * would bomb and stop the test.
		 */
		// multiPurposeFunction(RENEW_HDFS_DELEGATION_TOKEN_IN_A_LOOP);
		// multiPurposeFunction(ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP);
		// multiPurposeFunction(ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP);
		stopKinitCacheDestroyer();

	}

	void multiPurposeFunction(String testType) throws IOException,
			URISyntaxException {
		Configuration doasConf = new Configuration();
		int MAX_LOOP = 10000;
		// FileSystem aRemoteFS = FileSystem.get(doasConf);

		FileSystem aRemoteFS = FileSystem.get(
				new URI(getPrefixForProtocol(protocol,
						System.getProperty("CLUSTER_NAME"))
						+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST), doasConf);
		Token<? extends TokenIdentifier> fsForRenewToken = aRemoteFS
				.getDelegationToken("hadoopqa");

		for (int counter = 0; counter < MAX_LOOP; counter++) {
			if (testType == RENEW_HDFS_DELEGATION_TOKEN_IN_A_LOOP) {
				if ((counter % 100) == 0) {
					TestSession.logger.info("Renewed[" + protocol
							+ "]delegation token " + counter + " times");
				}
				try {
					fsForRenewToken.renew(doasConf);
				} catch (Exception e) {
					collector.addError(e);
					// Assert.fail("Got an exception, while renewing token at count:"
					// + counter);
					e.printStackTrace();
					break;
				}
			}
			/**
			 * Test to acquire a token
			 */
			if (testType == ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP) {
				try {
					Token<? extends TokenIdentifier> fsForAcquireToken = aRemoteFS
							.getDelegationToken("hadoopqa");
				} catch (Exception e) {
					collector.addError(e);
					// Assert.fail("Got an exception, while acquiring token at count:"
					// + counter);
					e.printStackTrace();
					break;
				}
				if ((counter % 100) == 0) {
					TestSession.logger.info("Acquired[" + protocol
							+ "] delegation token " + counter + " times");

				}
			}
			/**
			 * Test to acquire and cancel a token
			 */

			if (testType == ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP) {
				try {
					Token<? extends TokenIdentifier> fsForAcquireAndCancelToken = aRemoteFS
							.getDelegationToken("hadoopqa");
					fsForAcquireAndCancelToken.cancel(doasConf);

				} catch (Exception e) {
					collector.addError(e);
					// Assert.fail("Got an exception, while acquiring token at count:"
					// + counter);
					e.printStackTrace();
					break;
				}
				if ((counter % 100) == 0) {
					TestSession.logger.info("Acquired and cancelled ["
							+ protocol + "] delegation token " + counter
							+ " times");
				}
			}

		}

	}

	public class MultiPurposeDoasBlock {
		UserGroupInformation ugi;
		String testType;

		MultiPurposeDoasBlock(UserGroupInformation ugi, String testType)
				throws Exception {
			this.ugi = ugi;
			this.testType = testType;
		}

		public void go() throws Exception {

			// run as the original user
			String retVal = ugi.doAs(new PrivilegedExceptionAction<String>() {

				public String run() throws Exception {
					Configuration doasConf = new Configuration();
					int MAX_LOOP = 10000;
					// FileSystem aRemoteFS = FileSystem.get(doasConf);

					FileSystem aRemoteFS = FileSystem.get(
							new URI(getPrefixForProtocol(protocol,
									System.getProperty("CLUSTER_NAME"))
									+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST),
							doasConf);
					Token<? extends TokenIdentifier> aFSDelegationTokenThatWillBeRenewedRepeatedlyWithHadoopqaAsRenewer = aRemoteFS
							.getDelegationToken("hadoopqa");

					for (int counter = 0; counter < MAX_LOOP; counter++) {
						if (testType == RENEW_HDFS_DELEGATION_TOKEN_IN_A_LOOP) {
							if ((counter % 100) == 0) {
								TestSession.logger.info("Renewed[" + protocol
										+ "]delegation token " + counter
										+ " times");
							}
							try {
								aFSDelegationTokenThatWillBeRenewedRepeatedlyWithHadoopqaAsRenewer.renew(doasConf);
							} catch (Exception e) {
								collector.addError(e);
								e.printStackTrace();
								break;
							}
						}
						/**
						 * Test to acquire a token
						 */
						if (testType == ACQUIRE_HDFS_DELEGATION_TOKENS_IN_A_LOOP) {
							try {
								Token<? extends TokenIdentifier> aDelegationTokenAcquiredRepeatedly = aRemoteFS
										.getDelegationToken("hadoopqa");
							} catch (Exception e) {
								collector.addError(e);
								e.printStackTrace();
								break;
							}
							if ((counter % 100) == 0) {
								TestSession.logger.info("Acquired[" + protocol
										+ "] delegation token " + counter
										+ " times");

							}
						}
						/**
						 * Test to acquire and cancel a token
						 */

						if (testType == ACQUIRE_AND_CANCEL_HDFS_DELEGATION_TOKENS_IN_A_LOOP) {
							try {
								Token<? extends TokenIdentifier> aFSDelegationTokenAcquiredJustToBeCancelled = aRemoteFS
										.getDelegationToken("hadoopqa");
								aFSDelegationTokenAcquiredJustToBeCancelled.cancel(doasConf);

							} catch (Exception e) {
								collector.addError(e);
								e.printStackTrace();
								break;
							}
							if ((counter % 100) == 0) {
								TestSession.logger
										.info("Acquired and cancelled ["
												+ protocol
												+ "] delegation token "
												+ counter + " times");
							}
						}

					}

					return "This is the doAs block";
				}
			});
			TestSession.logger.info(retVal);
		}

	}
}