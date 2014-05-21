package hadooptest.hadoop.stress.tokenRenewal;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

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
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * TokenRenewalTest_existingUgi - acquire, renew and use HDFS and RM DT tokens
 * as a normal user. A goal of this test is to use the same approach Core
 * recommended to set the correct renewer, and implemented in Oozie
 * 3.3.1.0.1301022110.
 * 
 * 20130104 phw Run a wordcount job using RM and HDFS delegation tokens that are
 * explicitly gotten by the current user. Try to renew the tokens before the
 * job, confirm they fail renewal since renewer does not match current user and
 * then run the job
 * 
 * Config needs: property 'yarn.resourcemanager.delegation.token.renew-interval'
 * was reduced to 60 (seconds) to force token renewal between jobs, actual
 * setting is in mSec so it's '60000'.
 * 
 * Input needs: the wordcount needs some sizeable text input in hdfs at
 * '/data/in'
 * 
 * Output Expectations: token acquisition success with renewal failure, job run
 * using acquired credentials is successful, job output perm changes success
 */
@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestTokenRenewalExistingUgi extends DelegationTokenBaseClass {
	String protocol;
	static boolean haventBouncedServers = true;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "hdfs" }, { "webhdfs" }, });
	}

	public TestTokenRenewalExistingUgi(String protocol) {
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
	public void runTestTokenRenewalExistingUgi() throws Exception {
		HashMap<Text, byte[]> previouslyObtainedTokensInDoas = new HashMap<Text, byte[]>();
		HashMap<Text, byte[]> tokensReadBackToConfirmTheyHaveNotChanged = new HashMap<Text, byte[]>();
		Iterator<Token<? extends TokenIdentifier>> iterator;

		// Since the run is parametrized, we just need to set values and bounce
		// servers once.
		if (haventBouncedServers) {
			testPrepSetTokenRenewAndMaxLifeInterval();
			haventBouncedServers = false;
		}
		// don't cancel our tokens so we can use them in second
		conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens",
				false);

		// Explicitly login as a user
		SecurityUtil.login(TestSession.cluster.getConf(), "keytab-hadoopqa",
				"user-hadoopqa");

		Cluster cluster = new Cluster(conf);
		FileSystem fs = FileSystem.get(conf);
		Credentials credentials = new Credentials();

		// list out our config prop change, should be 60 (seconds)
		TestSession.logger
				.info("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: "
						+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

		UserGroupInformation ugiOrig = UserGroupInformation
				.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");

		ugiOrig = loginUserFromKeytabAndReturnUGI(
				"hadoopqa@DEV.YGRID.YAHOO.COM",
				HadooptestConstants.Location.Keytab.HADOOPQA);

		long timeStampBeforeRunningJobs = System.currentTimeMillis();

		JobConf jobConf = new JobConf(conf);
		TestSession.logger
				.info("Readback renew-interval as:"
						+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

		Path inputPath = new Path(getPrefixForProtocol(protocol,
				System.getProperty("CLUSTER_NAME"))
				+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST);
		if (!inputPath.getFileSystem(conf).exists(inputPath)) {
			inputPath.getFileSystem(conf).mkdirs(inputPath);
		}
		FileInputFormat.setInputPaths(jobConf, inputPath);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setJarByClass(TestTokenRenewalExistingUgi.class);
		jobConf.setJobName("runTestTokenRenewalExistingUgi_1");
		jobConf.setMapperClass(Map.class);
		jobConf.setCombinerClass(Reduce.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		Path outpath = new Path("/tmp/outfoo");
		if (outpath.getFileSystem(conf).isDirectory(outpath)) {
			outpath.getFileSystem(conf).delete(outpath, true);
			TestSession.logger.info("Info: deleted output path: " + outpath);
		}
		FileOutputFormat.setOutputPath(jobConf, outpath);
		jobConf.setOutputFormat(TextOutputFormat.class);

		Token<DelegationTokenIdentifier> mrdt = cluster
				.getDelegationToken(new Text("mapredqa"));
		credentials.addToken(new Text("RM_TOKEN_ALIAS"), mrdt);
		Token<?> hdfsdt = fs.addDelegationTokens("mapredqa", credentials)[0];
		iterator = credentials.getAllTokens().iterator();
		while (iterator.hasNext()) {
			Token<? extends TokenIdentifier> aContainedToken = iterator.next();
			previouslyObtainedTokensInDoas.put(aContainedToken.getKind(),
					aContainedToken.getIdentifier());

		}
		// let's see what we got...
		printTokens(previouslyObtainedTokensInDoas);

		// we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1
		// RM_DELEGATION_TOKEN
		// This should fail, let's try to renew as ourselves
		TestSession.logger.info("\nLet's try to renew our tokens...");
		TestSession.logger.info("First our HDFS_DELEGATION_TOKEN: ");
		try {
			hdfsdt.renew(conf);
			Assert.fail();
		} catch (Exception e) {
			TestSession.logger
					.info("Success, renew failed as expected since we're not the priv user");
		}

		TestSession.logger.info("\nAnd our RM_DELEGATION_TOKEN: ");
		try {
			mrdt.renew(conf);
			Assert.fail();
		} catch (Exception e) {
			TestSession.logger
					.info("Success, renew failed as expected since we're not the priv user");
		}

		int numTokens = credentials.numberOfTokens();
		TestSession.logger.info("We have a total of " + numTokens + " tokens");
		TestSession.logger
				.info("Dump all tokens currently in our Credentials:");
		TestSession.logger.info(credentials.getAllTokens() + "\n");

		TestSession.logger.info("Trying to submit job1...");
		RunningJob runningJob = JobClient.runJob(jobConf);

		System.out.print("...wait while the 1st job runs.");
		while (!runningJob.isComplete()) {
			System.out.print(".");
			Thread.sleep(2000);
		}
		if (runningJob.isSuccessful()) {
			TestSession.logger.info("Job-1 completion successful");
			// open perms on the output
			outpath.getFileSystem(conf).setPermission(outpath,
					new FsPermission("777"));
			outpath.getFileSystem(conf).setPermission(
					outpath.suffix("/part-00000"), new FsPermission("777"));
		} else {
			failFlags = failFlags + 10; // first job failed
			TestSession.logger.info("Job failed");
		}
		if (numTokens != credentials.numberOfTokens()) {
			System.out
					.println("\nWARNING: number of tokens before and after job submission differs, had "
							+ numTokens
							+ " now have "
							+ credentials.numberOfTokens());
		}
		TestSession.logger.info("After job1, we have a total of "
				+ credentials.numberOfTokens() + " tokens");
		TestSession.logger
				.info("\nDump all tokens currently in our Credentials:");
		TestSession.logger.info(credentials.getAllTokens() + "\n");

		// setup and run another wordcount job
		JobConf jobDoasJobConf2 = new JobConf(conf);
		jobDoasJobConf2.setJarByClass(TestTokenRenewalExistingUgi.class);
		jobDoasJobConf2.setJobName("runTestTokenRenewalExistingUgi_2");

		jobDoasJobConf2.setOutputKeyClass(Text.class);
		jobDoasJobConf2.setOutputValueClass(IntWritable.class);

		jobDoasJobConf2.setMapperClass(Map.class);
		jobDoasJobConf2.setCombinerClass(Reduce.class);
		jobDoasJobConf2.setReducerClass(Reduce.class);

		jobDoasJobConf2.setInputFormat(TextInputFormat.class);
		jobDoasJobConf2.setOutputFormat(TextOutputFormat.class);

		Path outpath2 = new Path("/tmp/outfoo2");
		if (outpath2.getFileSystem(conf).isDirectory(outpath2)) {
			outpath2.getFileSystem(conf).delete(outpath2, true);
			TestSession.logger.info("Info: deleted output path2: " + outpath2);
		}
		FileInputFormat.setInputPaths(
				jobDoasJobConf2,
				new Path(getPrefixForProtocol(protocol,
						System.getProperty("CLUSTER_NAME"))
						+ DATA_DIR_IN_HDFS + FILE_USED_IN_THIS_TEST));

		FileOutputFormat.setOutputPath(jobDoasJobConf2, outpath2);
		RunningJob runningJob2 = JobClient.runJob(jobDoasJobConf2);
		System.out.print("...wait while the 2nd job runs.");

		while (!runningJob2.isComplete()) {
			System.out.print(".");
			Thread.sleep(2000);
		}

		if (runningJob2.isSuccessful()) {
			TestSession.logger.info("Job 2 completion successful");
			// open perms on the output
			outpath2.getFileSystem(conf).setPermission(outpath2,
					new FsPermission("777"));
			outpath2.getFileSystem(conf).setPermission(
					outpath2.suffix("/part-00000"), new FsPermission("777"));
		} else {
			TestSession.logger.info("Job 2 failed");
			Assert.fail();
		}

		// Get the details on the tokens again (should be the same
		// guys, just reused)
		iterator = credentials.getAllTokens().iterator();
		while (iterator.hasNext()) {
			Token<? extends TokenIdentifier> aContainedToken = iterator.next();
			tokensReadBackToConfirmTheyHaveNotChanged.put(
					aContainedToken.getKind(), aContainedToken.getIdentifier());

		}
		// let's see what we got...
		printTokens(tokensReadBackToConfirmTheyHaveNotChanged);

		assertTokenRenewals(previouslyObtainedTokensInDoas,
				tokensReadBackToConfirmTheyHaveNotChanged);
	}

}