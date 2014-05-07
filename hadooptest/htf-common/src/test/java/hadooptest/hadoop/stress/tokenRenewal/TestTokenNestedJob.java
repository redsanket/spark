package hadooptest.hadoop.stress.tokenRenewal;

import hadooptest.SerialTests;
import hadooptest.TestSession;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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
public class TestTokenNestedJob extends DelegationTokenBaseClass {
	String protocol;
	static boolean haventBouncedServers = true;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
		// { "hdfs" },
		{ "webhdfs" }, });
	}

	public TestTokenNestedJob(String protocol) {
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
	public void runTestTokenNestedJobs() throws Exception {

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

		Credentials credentials = new Credentials();

		// list out our config prop change, should be 60 (seconds)
		TestSession.logger
				.info("Check the renew property setting, "
						+ "yarn.resourcemanager.delegation.token.renew-interval: "
						+ conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

		JobConf jobConf = new JobConf(conf);
		jobConf.setJarByClass(TestTokenNestedJob.class);
		jobConf.setJobName("TestTokenNestedJob");
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		jobConf.setMapperClass(JobLaunchMapper.class);

		Path inputPath = new Path(getPrefixForProtocol(protocol,
				System.getProperty("CLUSTER_NAME"))
				+ DATA_DIR_IN_HDFS + "file_1B");
		if (!inputPath.getFileSystem(conf).exists(inputPath)) {
			inputPath.getFileSystem(conf).mkdirs(inputPath);
		}

		Path outpath = new Path("/tmp/outfoo");
		if (outpath.getFileSystem(conf).isDirectory(outpath)) {
			outpath.getFileSystem(conf).delete(outpath, true);
			TestSession.logger.info("Info: deleted output path: " + outpath);
		}
		FileInputFormat.setInputPaths(jobConf, inputPath);

		jobConf.setInputFormat(TextInputFormat.class);
		FileOutputFormat.setOutputPath(jobConf, outpath);

		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		Token<? extends TokenIdentifier> rmToken = jobClient
				.getDelegationToken(new Text("RmRenewerShouldNotMatterNow"));

		jobConf.getCredentials().addToken(new Text("MyRmToken"), rmToken);

		// let's see what we got for credentials...
		System.out.println("RmToken: " + rmToken.getIdentifier());
		System.out.println("RmToken kind: " + rmToken.getKind() + "\n");
		System.out
				.println("\tDump of my creds:\n" + credentials.getAllTokens());
		// submit the first (the outer) job
		JobClient.runJob(jobConf);
	}

}