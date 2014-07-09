package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestJobJarPermissions extends YarnTestsBaseClass {

	private String setupAndFireHadoopStreamingJob(HashMap<String, String> args,
			String inputFile, String mapper, String reducer, String outFile,
			String[] files) throws Exception {
		String inpFileOnHdfs = "input-testJobJarPermissions"
				+ System.currentTimeMillis();
		String outputPath = "output-testJobJarPermissions"
				+ System.currentTimeMillis();

		StringBuilder sb = new StringBuilder();

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericResponseBO = dfsCliCommands.put(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), inputFile, inpFileOnHdfs);
		Assert.assertTrue(genericResponseBO.process.exitValue() == 0);

		sb.append("-input " + inpFileOnHdfs);
		sb.append(" -output " + outputPath);
		sb.append(" -mapper " + mapper);
		if (reducer != null)
			sb.append(" -reducer " + reducer);
		else
			sb.append(" -reducer NONE");

		for (String key : args.keySet()) {
			sb.append(" -jobconf \"" + key + "=" + args.get(key) + "\"");
		}
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		for (String aFile : files) {
			sb.append(" -file " + aFile);
		}

		TestSession.logger
				.info("Executing command with args-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outputPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);
		return genericCliResponseBO.response;

	}

	/**
	 * This is a data driven test, with values in the resources folder.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testJobJarPermissions10() throws Exception {
		String testId = "JobJarPermissions10";
		String location = "JobJarPermissions-10";

		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.maps", "1");
		args.put("mapreduce.job.reduces", "1");
		args.put("mapreduce.job.name", testId);
		String mapper = "mapper.sh";
		String reducer = "reducer.sh";
		String inputFile = TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/jobJarPermissions/" + location + "/input.txt";

		String[] files = {
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location + "/mapper.sh",

				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location + "/reducer.sh",

				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location
						+ "/noExePermission.sh"

		};

		setupAndFireHadoopStreamingJob(args, inputFile, mapper, reducer,
				testId, files);

	}

	/**
	 * This is also a data driven test, with values in the resources folder.
	 * 
	 * @throws Exception
	 */
	@Test
	@Ignore("Ignore running this test, because the actual Shell test also fails with exceptions")
	public void testJobJarPermissions20() throws Exception {
		String testId = "JobJarPermissions20";
		String location = "JobJarPermissions-20";

		HashMap<String, String> args = new HashMap<String, String>();
		args.put("mapreduce.job.maps", "1");
		args.put("mapreduce.job.reduces", "1");
		args.put("mapreduce.job.name", testId);
		String mapper = "mapper.sh";
		String reducer = "reducer.sh";
		String inputFile = TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/jobJarPermissions/" + location + "/input.txt";

		String[] files = {
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location + "/mapper.sh",

				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location + "/reducer.sh",

				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression"
						+ "/yarn/jobJarPermissions/" + location + "/testDir"

		};

		setupAndFireHadoopStreamingJob(args, inputFile, mapper, reducer,
				testId, files);

	}

}
