package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * TokenRenewalTest_doasBlock_cleanUgi_proxyUser - acquire and attempt renew of
 * HDFS and RM DT tokens from a login user context but using a new remoteUser
 * UGI to clear the security context. The token renewal should fail since
 * renewers don't match, the tokens should be valid and can be passed to a doAs
 * block where these tokens are the only credentials available to submit jobs.
 * However, in the proxy user case, we can't use these tokens, they don't match
 * the UGI security context. So, in the doAs context, obtain the UGI for a
 * different (proxy) user than the original user, and we will call the doAs
 * block without any credentials, which should force the Rm and NN to have to
 * get new tokens, and this should succeed becuase of the fallback to the login
 * user's TGT. 20130109 phw
 * 
 * Config needs: on RM, property
 * 'yarn.resourcemanager.delegation.token.renew-interval' was reduced to 60
 * (seconds) to force token renewal between jobs, actual setting is in mSec so
 * it's '60000'. on RM and NN, add to 'local-superuser-conf.xml';</p> <property>
 * </p>
 * <p>
 * <name>hadoop.proxyuser.YOUR_USER.groups</name>
 * </p>
 * <p>
 * <value>users</value>
 * </p>
 * </property>
 * <p>
 * <property>
 * <p>
 * </p>
 * <name>hadoop.proxyuser.YOUR_USER.hosts</name>
 * <p>
 * <p>
 * <value>YOUR_HOST.blue.ygrid.yahoo.com</value> </property>
 * 
 * Input needs: the wordcount needs some sizeable text input in hdfs at
 * '/data/in' with path perms 777 (or at least 644)
 * 
 * Output Expectations: token acquisition success with renewal failure, these
 * creds are added to the new ugi, this ugi is passed to a soad block where we
 * try to run two wordcount jobs in succession. We should see teh same token
 * behavior as we do in the normal (non doas) case, which that Rm and HDFS DTs
 * are acquired, after job submission we automatically get an MR (job history)
 * DT token, in addition to having the existing acquired tokens. After a
 * predefiend timeout, <10 minutes for RM and <1 minute for HDFS, the tokens are
 * automatically cancelled.
 * 
 * @author patw, tiwari
 * 
 */
@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestTokenRenewalDoasBlockCleanUgiProxyUser extends
		DelegationTokenBaseClass {
	String protocol;
	static boolean haventBouncedServers = true;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "hdfs" },
		// { "webhdfs" },
				});
	}

	public TestTokenRenewalDoasBlockCleanUgiProxyUser(String protocol) {
		this.protocol = protocol;
	}

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();

	}

	public void ensureDataPresenceinCluster() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		logger.info("running ensureDataPresenceAcrossClusters");
		String aCluster = System.getProperty("CLUSTER_NAME");

		for (String justTheFileName : fileMetadata.keySet()) {
			GenericCliResponseBO doesFileExistResponseBO = dfsCommonCli.test(
					EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE, aCluster, DATA_DIR_IN_HDFS
							+ justTheFileName,
					DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
			if (doesFileExistResponseBO.process.exitValue() != 0) {
				dfsCommonCli.mkdir(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, aCluster,
						DATA_DIR_IN_HDFS);
				doChmodRecursively(aCluster, DATA_DIR_IN_HDFS
						+ FILE_USED_IN_THIS_TEST);
				dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.NONE, aCluster,
						DATA_DIR_IN_LOCAL_FS + justTheFileName,
						DATA_DIR_IN_HDFS + justTheFileName);

			}
			dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.NONE, aCluster, DATA_DIR_IN_HDFS
							+ justTheFileName, "777", Recursive.NO);
		}

	}

	@Test
	public void runTestTokenRenewalCleanUgiProxyUser() throws Exception {
		long renewTimeHdfs = -1;
		long renewTimeRm = -1;
		ensureDataPresenceinCluster();
		testPrepSetTokenRenewAndMaxLifeInterval();
		SecurityUtil.login(TestSession.cluster.getConf(), "keytab-hdfsqa",
				"user-hdfsqa");

		Iterator<Entry<String, String>> iterator = TestSession.cluster
				.getConf().iterator();
		while (iterator.hasNext()) {
			Entry<String, String> anEntry = iterator.next();
			TestSession.logger.info("Key:" + anEntry.getKey() + " Value:"
					+ anEntry.getValue());

		}
		UserGroupInformation ugiOrigLoggedInAsHdfsqa = loginUserFromKeytabAndReturnUGI(
				"hdfsqa@DEV.YGRID.YAHOO.COM",
				HadooptestConstants.Location.Keytab.HDFSQA);
		UserGroupInformation.createRemoteUser("hdfsqa@DEV.YGRID.YAHOO.COM");

		Cluster cluster = new Cluster(conf);
		FileSystem fs = FileSystem.get(conf);

		// list out our config prop change, should be 60 (seconds)
		TestSession.logger
				.info("Check the renew property setting, "
						+ "yarn.resourcemanager.delegation.token.renew-interval: "
						+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
		TestSession.logger
				.info("Check the renew property setting, "
						+ "yarn.resourcemanager.delegation.token.max-lifetime: "
						+ conf.get("yarn.resourcemanager.delegation.token.max-lifetime"));

		TestSession.logger.info("Check the renew property setting,"
				+ "dfs.namenode.delegation.token.renew-interval: "
				+ conf.get("dfs.namenode.delegation.token.renew-interval"));

		Token<? extends TokenIdentifier> myTokenRm = cluster
				.getDelegationToken(new Text("GARBAGE1_mapredqa"));
		// Note well, need to use fs.addDelegationTokens() to get an HDFS DT
		// that is recognized by the fs,
		// trying to use fs.getDelegationToken() appears to work but doesn't, fs
		// still auto fetches a token
		// so said token is not being recognized

		Credentials creds = new Credentials();
		creds.addToken(new Text("MyTokenAliasRM"), myTokenRm);

		// TODO: should capture this list and iterate over it, not grab first
		// element...
		Token<?> myTokenHdfsFs = fs.addDelegationTokens("mapredqa", creds)[0];

		// let's see what we got...
		TestSession.logger.info("myTokenRm: " + myTokenRm.getIdentifier());
		TestSession.logger.info("myTokenRm kind: " + myTokenRm.getKind());
		TestSession.logger.info("myTokenRm decodeIdentifier: "
				+ myTokenRm.getIdentifier() + "\n");
		TestSession.logger.info("myTokenHdfsFs: "
				+ myTokenHdfsFs.getIdentifier());
		TestSession.logger.info("myTokenHdfsFs: "
				+ myTokenHdfsFs.getIdentifier());
		TestSession.logger.info("myTokenHdfsFs kind: "
				+ myTokenHdfsFs.getKind() + "\n");

		// add creds to UGI, this adds the two RM tokens, the HDFS token was
		// added already as part
		// of the addDelegationTokens()
		ugiOrigLoggedInAsHdfsqa.addCredentials(creds);

		TestSession.logger.info("From OriginalUser... my Creds say i'm: "
				+ UserGroupInformation.getCurrentUser() + " and I have "
				+ creds.numberOfTokens() + " tokens");

		// write our tokenfile
		// we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1
		// RM_DELEGATION_TOKEN, let's verify
		// we can't renew them, since renewers don't match

		TestSession.logger
				.info("\nLet's try to renew our tokens, should fail since renewers don't match...");
		TestSession.logger.info("First our HDFS_DELEGATION_TOKEN: ");
		try {
			renewTimeHdfs = myTokenHdfsFs.renew(conf);
		} catch (Exception e) {
			TestSession.logger
					.info("Success, renew failed as expected since (hdfsqa) is not the renewer (mapredqa) is");
		}
		TestSession.logger
				.info("Got renew time 1st time HDFS:" + renewTimeHdfs);

		TestSession.logger.info("\nOur first RM_DELEGATION_TOKEN: ");
		try {
			renewTimeRm = myTokenRm.renew(conf);
		} catch (Exception e) {
			TestSession.logger
					.info("Success, renew failed as expected since we're not the priv user");
		}

		TestSession.logger.info("Got renew time 1st time RM:" + renewTimeRm);
		int numTokens = ugiOrigLoggedInAsHdfsqa.getCredentials()
				.numberOfTokens();
		TestSession.logger.info("We have a total of " + numTokens + " tokens");
		TestSession.logger
				.info("Dump all tokens currently in our Credentials:");
		TestSession.logger.info(ugiOrigLoggedInAsHdfsqa.getCredentials()
				.getAllTokens() + "\n");

		// instantiate a seperate object to use for submitting jobs, using
		// our shiney new tokens
		DoasUser du = new DoasUser();
		du.go();

		// back to our original context, our two doAs jobs should have ran as
		// the specified
		// proxy user, dump our existing credentials
		TestSession.logger
				.info("Back from the doAs block to original context... my Creds say i'm: "
						+ UserGroupInformation.getCurrentUser()
						+ " and I now have "
						+ creds.numberOfTokens()
						+ " tokens");
		TestSession.logger
				.info("\nDump all tokens currently in our Credentials:");
		TestSession.logger.info(ugiOrigLoggedInAsHdfsqa.getCredentials()
				.getAllTokens() + "\n");

	}

	// class DoasUser
	// this is used create a new UGI (new security context) for running
	// wordcount jobs
	// using the credentials passed into said ugi
	public class DoasUser {

		private Credentials doasCreds;
		private Cluster doasCluster;
		private Configuration doasConf;
		private FileSystem doasFs;
		UserGroupInformation ugi;

		DoasUser() throws Exception {
			// get a proxy UGI for hadoopqa, since no creds are passed in we
			// have to get tokens
			// using the TGT fallback for the login user
			try {

				ugi = UserGroupInformation.createProxyUser(
						"hadoopqa@DEV.YGRID.YAHOO.COM",
						UserGroupInformation.getLoginUser());
				TestSession.logger
						.info("Created proxy user (hadoopqa@DEV.YGRID.YAHOO.COM) f	or main guy(hdfsqa)");
			} catch (Exception e) {
				System.out
						.println("Failed, couldn't get UGI object for proxy user: "
								+ e);
			}
		}

		public void go() throws Exception {

			// run as the original user
			String retVal = ugi.doAs(new PrivilegedExceptionAction<String>() {
				HashMap<Text, byte[]> previouslyObtainedTokensInDoas = new HashMap<Text, byte[]>();
				HashMap<Text, byte[]> tokensReadBackToConfirmTheyHaveNotChanged = new HashMap<Text, byte[]>();
				Iterator<Token<? extends TokenIdentifier>> iterator;

				public String run() throws Exception {
					// MUST instantiate objs here in the doas go() run method to
					// get the proxy context
					// might not have to be in the run method but just be in the
					// go() method, need
					// to try that...
					doasConf = new Configuration();
					doasCluster = new Cluster(doasConf);
					TestSession.logger
							.info(" in doAs block, before doing Fs Get");
					doasFs = FileSystem.get(doasConf);
					TestSession.logger
							.info(" in doAs block, after doing Fs Get");
					doasCreds = new Credentials();

					// get RM and HDFS token within our proxy ugi context, these
					// we should be able to use
					// renewer is not valid, shouldn't matter now after 23.6
					// design change for renewer
					TestSession.logger
							.info(" in doAs block, before getting RM delegation token");
					Token<?> doasRmToken = null;
					try {
						doasRmToken = doasCluster.getDelegationToken(new Text(
								"DOAS_GARBAGE1_mapredqa"));
					} catch (Exception e) {
						e.printStackTrace();
					}
					TestSession.logger
							.info(" in doAs block, got RM delegation token");
					doasCreds.addToken(new Text("MyDoasTokenAliasRM"),
							doasRmToken);

					// TODO: should capture this list and iterate over it, not
					// grab first element...
					TestSession.logger
							.info(" in doAs block, before adding hdfs delegation tokens");
					Token<?> doasHdfsToken = doasFs.addDelegationTokens(
							"mapredqa", doasCreds)[0];
					TestSession.logger
							.info(" in doAs block, got FS delegation token");
					iterator = doasCreds.getAllTokens().iterator();
					while (iterator.hasNext()) {
						Token<? extends TokenIdentifier> aContainedToken = iterator
								.next();
						previouslyObtainedTokensInDoas.put(
								aContainedToken.getKind(),
								aContainedToken.getIdentifier());

					}
					// let's see what we got...
					printTokens(previouslyObtainedTokensInDoas);

					ugi.addCredentials(doasCreds);
					System.out
							.println("From DoasProxyUser... my Creds say i'm: "
									+ UserGroupInformation.getCurrentUser()
									+ " and I have "
									+ doasCreds.numberOfTokens() + " tokens");

					JobConf jobDoasJobConf = new JobConf(conf);
					TestSession.logger.info("Readback renew-interval as:"
							+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
					try {
						jobDoasJobConf
								.setJarByClass(TestTokenRenewalDoasBlockCleanUgiProxyUser.class);
						jobDoasJobConf
								.setJobName("TokenRenewalTest_doasBlock_cleanUgi_proxyUser_wordcountOrigUser_job1");

						jobDoasJobConf.setOutputKeyClass(Text.class);
						jobDoasJobConf.setOutputValueClass(IntWritable.class);

						jobDoasJobConf.setMapperClass(Map.class);
						jobDoasJobConf.setCombinerClass(Reduce.class);
						jobDoasJobConf.setReducerClass(Reduce.class);
						jobDoasJobConf.set("mapreduce.job.acl-view-job", "*");
						jobDoasJobConf.set("mapreduce.job.acl-modify-job", "*");
						// jobDoasJobConf.set("mapred.child.java.opts",
						// "-Xmx2048m");
						// jobDoasJobConf.set("mapreduce.map.memory.mb",
						// "4096");
						// jobDoasJobConf.set("mapreduce.reduce.memory.mb",
						// "8192");
						jobDoasJobConf.set("mapred.child.java.opts",
								"-Xmx2048m");
						jobDoasJobConf.set("mapreduce.map.memory.mb", "2560");
						jobDoasJobConf
								.set("mapreduce.reduce.memory.mb", "2560");

						jobDoasJobConf.setInputFormat(TextInputFormat.class);
						jobDoasJobConf.setOutputFormat(TextOutputFormat.class);

						Path outpath = new Path("/tmp/outfoo");
						if (outpath.getFileSystem(conf).isDirectory(outpath)) {
							outpath.getFileSystem(conf).delete(outpath, true);
							TestSession.logger
									.info("Info: deleted output path: "
											+ outpath);
						}
						// Path inputPath = new Path("/tmp/data/in");
						Path inputPath = new Path(getPrefixForProtocol(
								protocol, System.getProperty("CLUSTER_NAME"))
								+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST);

						if (!inputPath.getFileSystem(conf).exists(inputPath)) {
							inputPath.getFileSystem(conf).mkdirs(inputPath);
						}
						FileInputFormat
								.setInputPaths(jobDoasJobConf, inputPath);
						FileOutputFormat.setOutputPath(jobDoasJobConf, outpath);

						long timeStampBeforeRunningJobs = System
								.currentTimeMillis();
						// submit the job, this should automatically get us a
						// jobhistory token,
						// but does not seem to do so...
						// jobDoasJobConf.submit();
						RunningJob runningJob = JobClient
								.runJob(jobDoasJobConf);

						System.out.print("...wait while the doAs job runs.");
						while (!runningJob.isComplete()) {
							System.out.print(".");
							Thread.sleep(2000);
						}
						if (runningJob.isSuccessful()) {
							TestSession.logger
									.info("Job completion successful");
							// open perms on the output
							outpath.getFileSystem(conf).setPermission(outpath,
									new FsPermission("777"));
							outpath.getFileSystem(conf).setPermission(
									outpath.suffix("/part-00000"),
									new FsPermission("777"));
						} else {
							TestSession.logger.info("Job 1 failed");
							Assert.fail();
						}
						System.out
								.println("After doasUser first job... my Creds say i'm: "
										+ UserGroupInformation.getCurrentUser()
										+ " and I now have "
										+ doasCreds.numberOfTokens()
										+ " tokens");

						// setup and run another wordcount job, this should
						// exceed
						// the token renewal time of 60 seconds
						// and cause all of our passed-in tokens to be renewed,
						// job
						// should also succeed
						JobConf jobDoasJobConf2 = new JobConf(conf);
						jobDoasJobConf2
								.setJarByClass(TestTokenRenewalDoasBlockCleanUgiProxyUser.class);
						jobDoasJobConf2
								.setJobName("TokenRenewalTest_doasBlock_cleanUgi_proxyUser_wordcountOrigUser_job2");

						jobDoasJobConf2.setOutputKeyClass(Text.class);
						jobDoasJobConf2.setOutputValueClass(IntWritable.class);

						jobDoasJobConf2.setMapperClass(Map.class);
						jobDoasJobConf2.setCombinerClass(Reduce.class);
						jobDoasJobConf2.setReducerClass(Reduce.class);
						jobDoasJobConf.set("mapreduce.job.acl-view-job", "*");
						jobDoasJobConf.set("mapreduce.job.acl-modify-job", "*");
						// jobDoasJobConf.set("mapred.child.java.opts",
						// "-Xmx2048m");
						// jobDoasJobConf.set("mapreduce.map.memory.mb",
						// "4096");
						// jobDoasJobConf.set("mapreduce.reduce.memory.mb",
						// "8192");
						jobDoasJobConf.set("mapred.child.java.opts",
								"-Xmx2048m");
						jobDoasJobConf.set("mapreduce.map.memory.mb", "2560");
						jobDoasJobConf
								.set("mapreduce.reduce.memory.mb", "2560");

						jobDoasJobConf2.setInputFormat(TextInputFormat.class);
						jobDoasJobConf2.setOutputFormat(TextOutputFormat.class);

						Path outpath2 = new Path("/tmp/outfoo2");
						if (outpath2.getFileSystem(conf).isDirectory(outpath2)) {
							outpath2.getFileSystem(conf).delete(outpath2, true);
							TestSession.logger
									.info("Info: deleted output path2: "
											+ outpath2);
						}
						inputPath = new Path(getPrefixForProtocol(protocol,
								System.getProperty("CLUSTER_NAME"))
								+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST);
						FileInputFormat.setInputPaths(jobDoasJobConf2,
								inputPath);

						FileOutputFormat.setOutputPath(jobDoasJobConf2,
								outpath2);

						// submit the second job, this should also automatically
						// get
						// us a
						// jobhistory token, but doesn't...
						RunningJob runningJob2 = JobClient
								.runJob(jobDoasJobConf2);

						System.out
								.print("...wait while the second doAs job runs.");
						while (!runningJob2.isComplete()) {
							System.out.print(".");
							Thread.sleep(2000);
						}

						// Get the details on the tokens again (should be the
						// same
						// guys, just reused)
						iterator = doasCreds.getAllTokens().iterator();
						while (iterator.hasNext()) {
							Token<? extends TokenIdentifier> aContainedToken = iterator
									.next();
							tokensReadBackToConfirmTheyHaveNotChanged.put(
									aContainedToken.getKind(),
									aContainedToken.getIdentifier());

						}

						if (runningJob2.isSuccessful()) {
							TestSession.logger
									.info("Job 2 completion successful");
							// open perms on the output
							outpath2.getFileSystem(conf).setPermission(
									outpath2, new FsPermission("777"));
							outpath2.getFileSystem(conf).setPermission(
									outpath2.suffix("/part-00000"),
									new FsPermission("777"));
						} else {
							TestSession.logger.info("Job 2 failed");
							Assert.fail();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

					// let's see what we got...
					printTokens(tokensReadBackToConfirmTheyHaveNotChanged);
					assertTokenRenewals(previouslyObtainedTokensInDoas,
							tokensReadBackToConfirmTheyHaveNotChanged);

					return "This is the doAs block";
				}
			});
			TestSession.logger.info(retVal);
		}

	}

	@After
	public void logTaskReportSummary() {

	}
}