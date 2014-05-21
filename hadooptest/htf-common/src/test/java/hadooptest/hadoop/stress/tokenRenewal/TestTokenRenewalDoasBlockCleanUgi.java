package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This test is ignored in 2.x (while it is valid in 0.23), here is why. In this
 * test we take a clean user and pass 2 delegation tokens to him in a DoAs
 * Block. Those tokens being a RM token and a HDFS token. There is a 3rd token
 * History Server token that needs to be passed (there is no way to get it from
 * the client), in the absence of that the job cannot be submitted. In 0.23, it
 * ignores the History server token (a.k.a a Bug) hence it works there.
 * 
 * @author patw, tiwari
 * 
 */

@Ignore("Client needs access to History Server token. This is not possible in a clean DoAs block, "
		+ "since it does not have any TGT credentials, just the RM token and HDFS token."
		+ "This works in 0.23 (a bug) but not in 2.x, hence skipping")
@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestTokenRenewalDoasBlockCleanUgi extends DelegationTokenBaseClass {
	String protocol;
	static boolean haventBouncedServers = true;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "hdfs" }, { "webhdfs" }, });
	}

	public TestTokenRenewalDoasBlockCleanUgi(String protocol) {
		this.protocol = protocol;
	}

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	/*
	 * A test for running a TestTokenRenewal job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN
	 * regression suite.
	 */

	@Test
	public void runTestTokenRenewalCleanUgi() throws Exception {
		long renewTimeHdfs = -1;
		long renewTimeRm = -1;
		DfsCliCommands dfsCliCommands = new DfsCliCommands();

		dfsCliCommands.kinit(HadooptestConstants.UserNames.HADOOPQA);
		if (haventBouncedServers) {
			testPrepSetTokenRenewAndMaxLifeInterval();
			haventBouncedServers = false;
		}

		// Explicitly login as a user
		SecurityUtil.login(TestSession.cluster.getConf(), "keytab-hadoopqa",
				"user-hadoopqa");

		Cluster cluster = new Cluster(conf);
		FileSystem fs = FileSystem.get(conf);

		// list out our config prop change, should be 60 (seconds)
		TestSession.logger
				.info("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: "
						+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
		// don't cancel our tokens so we can use them in second
		conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens",
				false);
		/**
		 * Create a user from a login name. It is intended to be used for remote
		 * users in RPC, since it won't have any credentials.
		 * 
		 * @param user
		 *            the full user principal name, must not be empty or null
		 * @return the UserGroupInformation for the remote user.
		 */
		UserGroupInformation createdRemoteUsersUgi = UserGroupInformation
				.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");

		// renewer is not valid, shouldn't matter now after 23.6 design change
		// for renewer
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
		createdRemoteUsersUgi.addCredentials(creds);

		TestSession.logger.info("From OriginalUser... my Creds say i'm: "
				+ UserGroupInformation.getCurrentUser() + " and I have "
				+ creds.numberOfTokens() + " tokens");

		// write our tokenfile
		// we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1
		// RM_DELEGATION_TOKEN, let's verify
		// we can't renew them, since renewers don't match

		TestSession.logger.info("\nLet's try to renew our tokens"
				+ ", should fail since renewers don't match...");
		TestSession.logger.info("First our HDFS_DELEGATION_TOKEN:");
		try {
			renewTimeHdfs = myTokenHdfsFs.renew(conf);
			// Should never come here, as we are not the previliged user
			Assert.fail();

		} catch (Exception e) {
			TestSession.logger.info("Success, renew failed "
					+ "as expected since we're not the priv user");
		}
		TestSession.logger
				.info("Got renew time 1st time HDFS:" + renewTimeHdfs);

		TestSession.logger.info("\nOur first RM_DELEGATION_TOKEN: ");
		try {
			renewTimeRm = myTokenRm.renew(conf);
		} catch (Exception e) {
			TestSession.logger.info("Success, renew failed as expected"
					+ " since we're not the priv user");
		}

		TestSession.logger.info("Got renew time 1st time RM:" + renewTimeRm);
		int numTokens = createdRemoteUsersUgi.getCredentials().numberOfTokens();
		TestSession.logger.info("We have a total of " + numTokens + " tokens");
		TestSession.logger.info("Dump all tokens currently in our Credentials");
		TestSession.logger.info(createdRemoteUsersUgi.getCredentials()
				.getAllTokens());

		// instantiate a seperate object to use for submitting jobs, using
		// our shiney new tokens
		DoasUser du = new DoasUser(createdRemoteUsersUgi);
		du.go();

		// back to our original context, our two doAs jobs should have ran as
		// the specified
		// proxy user, dump our existing credentials
		TestSession.logger.info("Back from the doAs block to original "
				+ "context... my Creds say i'm: "
				+ UserGroupInformation.getCurrentUser() + " and I now have "
				+ creds.numberOfTokens() + " tokens");
		TestSession.logger
				.info("\nDump all tokens currently in our Credentials:");
		TestSession.logger.info(createdRemoteUsersUgi.getCredentials()
				.getAllTokens() + "\n");

	}

	// class DoasUser
	// this is used create a new UGI (new security context) for running
	// wordcount jobs
	// using the credentials passed into said ugi
	public class DoasUser {

		private Credentials doasCreds;
		UserGroupInformation ugiForUserWithStuffedCredentials;

		// constructor - ugi to use is passed in, uses existing creds that come
		// in with said ugi
		DoasUser(UserGroupInformation ugi) {
			doasCreds = ugi.getCredentials();
			this.ugiForUserWithStuffedCredentials = ugi;
		}

		public void go() throws Exception {

			// run as the original user
			ugiForUserWithStuffedCredentials
					.doAs(new PrivilegedExceptionAction<String>() {
						HashMap<Text, byte[]> previouslyObtainedTokensInDoas = new HashMap<Text, byte[]>();
						HashMap<Text, byte[]> tokensReadBackToConfirmTheyHaveNotChanged = new HashMap<Text, byte[]>();
						Iterator<Token<? extends TokenIdentifier>> iterator;

						public String run() throws Exception {
							// this fails with expired token before Tom's fix on
							// top of
							// Sid's token renewal patch in 23.6
							ugiForUserWithStuffedCredentials
									.addCredentials(doasCreds);
							TestSession.logger.info("From doasUser before running jobs... my Creds say i'm: "
									+ UserGroupInformation.getCurrentUser()
									+ " and I have "
									+ doasCreds.numberOfTokens() + " tokens");

							iterator = doasCreds.getAllTokens().iterator();
							while (iterator.hasNext()) {
								Token<? extends TokenIdentifier> anObtainedToken = iterator
										.next();
								previouslyObtainedTokensInDoas.put(
										anObtainedToken.getKind(),
										anObtainedToken.getIdentifier());

							}
							// let's see what we got...
							printTokens(previouslyObtainedTokensInDoas);

							JobConf jobDoasJobConf = new JobConf(conf);
							TestSession.logger.info("Readback renew-interval as:"
									+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

							jobDoasJobConf
									.setJarByClass(TestTokenRenewalDoasBlockCleanUgi.class);
							jobDoasJobConf
									.setJobName("TokenRenewalTest_doasBlock_cleanUgi_wordcountOrigUser_job1");

							jobDoasJobConf.setOutputKeyClass(Text.class);
							jobDoasJobConf
									.setOutputValueClass(IntWritable.class);

							jobDoasJobConf.setMapperClass(Map.class);
							jobDoasJobConf.setCombinerClass(Reduce.class);
							jobDoasJobConf.setReducerClass(Reduce.class);

							jobDoasJobConf
									.setInputFormat(TextInputFormat.class);
							jobDoasJobConf
									.setOutputFormat(TextOutputFormat.class);

							Path outpath = new Path("/tmp/outfoo");
							if (outpath.getFileSystem(conf)
									.isDirectory(outpath)) {
								outpath.getFileSystem(conf).delete(outpath,
										true);
								TestSession.logger
										.info("Info: deleted output path: "
												+ outpath);
							}
							// Path inputPath = new Path("/tmp/data/in");
							TestSession.logger
									.info("Setting PATH for protocol:"
											+ protocol + " for cluster :"
											+ cluster);
							Path inputPath = new Path(getPrefixForProtocol(
									protocol,
									System.getProperty("CLUSTER_NAME"))
									+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST);

							if (!inputPath.getFileSystem(conf)
									.exists(inputPath)) {
								inputPath.getFileSystem(conf).mkdirs(inputPath);
							}
							FileInputFormat.setInputPaths(jobDoasJobConf,
									inputPath);
							FileOutputFormat.setOutputPath(jobDoasJobConf,
									outpath);

							// submit the job, this should automatically get us
							// a
							// jobhistory token,
							// but does not seem to do so...
							// jobDoasJobConf.submit();
							RunningJob runningJob = JobClient
									.runJob(jobDoasJobConf);

							System.out
									.print("...wait while the doAs job runs.");
							while (!runningJob.isComplete()) {
								System.out.print(".");
								Thread.sleep(2000);
							}
							if (runningJob.isSuccessful()) {
								TestSession.logger
										.info("Job completion successful");
								// open perms on the output
								outpath.getFileSystem(conf).setPermission(
										outpath, new FsPermission("777"));
								outpath.getFileSystem(conf).setPermission(
										outpath.suffix("/part-00000"),
										new FsPermission("777"));
							} else {
								TestSession.logger.info("Job failed");
								Assert.fail();
							}

							System.out.println("After doasUser first job... my Creds say i'm: "
									+ UserGroupInformation.getCurrentUser()
									+ " and I now have "
									+ doasCreds.numberOfTokens() + " tokens");

							// setup and run another wordcount job, this should
							// exceed
							// the token renewal time of 60 seconds
							// and cause all of our passed-in tokens to be
							// renewed, job
							// should also succeed
							JobConf jobDoasJobConf2 = new JobConf(conf);
							jobDoasJobConf2
									.setJarByClass(TestTokenRenewalDoasBlockCleanUgi.class);
							jobDoasJobConf2
									.setJobName("TokenRenewalTest_doasBlock_cleanUgi_wordcountDoasUser_job2");

							jobDoasJobConf2.setOutputKeyClass(Text.class);
							jobDoasJobConf2
									.setOutputValueClass(IntWritable.class);

							jobDoasJobConf2.setMapperClass(Map.class);
							jobDoasJobConf2.setCombinerClass(Reduce.class);
							jobDoasJobConf2.setReducerClass(Reduce.class);

							jobDoasJobConf2
									.setInputFormat(TextInputFormat.class);
							jobDoasJobConf2
									.setOutputFormat(TextOutputFormat.class);

							Path outpath2 = new Path("/tmp/outfoo2");
							if (outpath2.getFileSystem(conf).isDirectory(
									outpath2)) {
								outpath2.getFileSystem(conf).delete(outpath2,
										true);
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

							// submit the second job, this should also
							// automatically get
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
							long timeStampAfterRunningJobs = System
									.currentTimeMillis();

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

							iterator = doasCreds.getAllTokens().iterator();
							while (iterator.hasNext()) {
								Token<? extends TokenIdentifier> aContainedToken = iterator
										.next();
								tokensReadBackToConfirmTheyHaveNotChanged.put(
										aContainedToken.getKind(),
										aContainedToken.getIdentifier());
							}

							// let's see what we got...
							printTokens(tokensReadBackToConfirmTheyHaveNotChanged);
							assertTokenRenewals(previouslyObtainedTokensInDoas,
									tokensReadBackToConfirmTheyHaveNotChanged);

							return "This is the doAs block";
						}
					});
		}

	}
}