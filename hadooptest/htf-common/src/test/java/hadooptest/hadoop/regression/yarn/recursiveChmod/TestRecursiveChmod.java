package hadooptest.hadoop.regression.yarn.recursiveChmod;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestRecursiveChmod extends YarnTestsBaseClass {

	@Test
	public void test10() throws Exception {
		StringBuilder sb = new StringBuilder();

		String TESTCASE_ID = "10";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String testDir = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), testDir);
		GenericCliResponseBO genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt",
						"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt");
		Assert.assertTrue(
				"put (input.txt) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar", "RecursiveChmod/RecursiveChmod-"
								+ TESTCASE_ID + "/test.jar");
		Assert.assertTrue(
				"put(test.jar) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		String outPath = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/RecursiveChmod" + TESTCASE_ID + ".out";
		sb.append("-input " + "\"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/input.txt\"");
		sb.append(" -mapper " + "\"mapper.sh\"");
		sb.append(" -reducer " + "\"reducer.sh\"");
		sb.append(" -output " + outPath);
		sb.append(" -cacheArchive " + "\""
				+ TestSession.cluster.getConf().get("fs.defaultFS") + "/user/"
				+ HadooptestConstants.UserNames.HADOOPQA
				+ "/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/test.jar#testlink\"");
		sb.append(" -jobconf \"mapreduce.map.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.reduce.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.job.name=RecursiveChmod-"
				+ TESTCASE_ID + "\"");
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/mapper.sh\"");

		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/reducer.sh\"");

		TestSession.logger.info("Executing command-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);

		assertContents(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
						+ "/data/RecursiveChmod-" + TESTCASE_ID
						+ "/expectedOutput", genericCliResponseBO.response);

	}

	@Test
	public void test20() throws Exception {
		StringBuilder sb = new StringBuilder();

		String TESTCASE_ID = "20";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String testDir = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), testDir);
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), "/tmp/" + testDir);

		GenericCliResponseBO genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt",
						"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt");
		Assert.assertTrue(
				"put (input.txt) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar",
						"/tmp/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar");
		Assert.assertTrue(
				"put(test.jar) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		String outPath = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/RecursiveChmod" + TESTCASE_ID + ".out";
		sb.append("-input " + "\"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/input.txt\"");
		sb.append(" -mapper " + "\"mapper.sh\"");
		sb.append(" -reducer " + "\"reducer.sh\"");
		sb.append(" -output " + outPath);
		sb.append(" -cacheArchive " + "\""
				+ TestSession.cluster.getConf().get("fs.defaultFS")
				+ "/tmp/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/test.jar#testlink\"");
		sb.append(" -jobconf \"mapreduce.map.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.reduce.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.job.name=RecursiveChmod-"
				+ TESTCASE_ID + "\"");
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/mapper.sh\"");

		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/reducer.sh\"");

		TestSession.logger.info("Executing command-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);

		assertContents(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
						+ "/data/RecursiveChmod-" + TESTCASE_ID
						+ "/expectedOutput", genericCliResponseBO.response);

	}

	@Test
	public void test30() throws Exception {
		StringBuilder sb = new StringBuilder();

		String TESTCASE_ID = "30";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String testDir = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), testDir);

		GenericCliResponseBO genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt",
						"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt");
		Assert.assertTrue(
				"put (input.txt) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar", "RecursiveChmod/RecursiveChmod-"
								+ TESTCASE_ID + "/test.jar");
		Assert.assertTrue(
				"put(test.jar) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		String outPath = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/RecursiveChmod" + TESTCASE_ID + ".out";
		sb.append("-archives " + "\""
				+ TestSession.cluster.getConf().get("fs.defaultFS")
				+ "/user/hadoopqa/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/test.jar#testlink\"");
		sb.append(" -input " + "\"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/input.txt\"");
		sb.append(" -mapper " + "\"mapper.sh\"");
		sb.append(" -reducer " + "\"reducer.sh\"");
		sb.append(" -output " + outPath);
		sb.append(" -jobconf \"mapreduce.map.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.reduce.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.job.name=RecursiveChmod-"
				+ TESTCASE_ID + "\"");
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/mapper.sh\"");

		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/reducer.sh\"");

		TestSession.logger.info("Executing command-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);

		assertContents(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
						+ "/data/RecursiveChmod-" + TESTCASE_ID
						+ "/expectedOutput", genericCliResponseBO.response);

	}

	@Test
	public void test40() throws Exception {
		StringBuilder sb = new StringBuilder();

		String TESTCASE_ID = "40";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String testDir = "/tmp/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), testDir);
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"),
				"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID);

		GenericCliResponseBO genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt",
						"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt");
		Assert.assertTrue(
				"put (input.txt) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar",
						"/tmp/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
								+ "/test.jar");
		Assert.assertTrue(
				"put(test.jar) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		String outPath = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/RecursiveChmod" + TESTCASE_ID + ".out";
		sb.append("-archives " + "\""
				+ TestSession.cluster.getConf().get("fs.defaultFS")
				+ "/tmp/RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/test.jar#testlink\"");
		sb.append(" -input " + "\"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/input.txt\"");
		sb.append(" -mapper " + "\"mapper.sh\"");
		sb.append(" -reducer " + "\"reducer.sh\"");
		sb.append(" -output " + outPath);
		sb.append(" -jobconf \"mapreduce.map.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.reduce.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.job.name=RecursiveChmod-"
				+ TESTCASE_ID + "\"");
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/mapper.sh\"");

		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/reducer.sh\"");

		TestSession.logger.info("Executing command-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);

		assertContents(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
						+ "/data/RecursiveChmod-" + TESTCASE_ID
						+ "/expectedOutput", genericCliResponseBO.response);

	}

	@Test
	public void test50() throws Exception {
		StringBuilder sb = new StringBuilder();

		String TESTCASE_ID = "50";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String testDir = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID;
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), testDir);

		GenericCliResponseBO genericResponseBO = dfsCliCommands
				.put(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA,
						"",
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
								+ "/data/RecursiveChmod-" + TESTCASE_ID
								+ "/input.txt", testDir + "/input.txt");
		Assert.assertTrue(
				"put (input.txt) command exited with non-zero exit code",
				genericResponseBO.process.exitValue() == 0);

		String outPath = "RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/RecursiveChmod" + TESTCASE_ID + ".out";
		sb.append("-archives "
				+ "\"file://"
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID
				+ "/test.jar#testlink\"");
		sb.append(" -input " + "\"RecursiveChmod/RecursiveChmod-" + TESTCASE_ID
				+ "/input.txt\"");
		sb.append(" -mapper " + "\"mapper.sh\"");
		sb.append(" -reducer " + "\"reducer.sh\"");
		sb.append(" -output " + outPath);
		sb.append(" -jobconf \"mapreduce.map.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.reduce.tasks=1\"");
		sb.append(" -jobconf \"mapreduce.job.name=RecursiveChmod-"
				+ TESTCASE_ID + "\"");
		sb.append(" -jobconf \"mapreduce.job.acl-view-job=*\"");
		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/mapper.sh\"");

		sb.append(" -file "
				+ "\""
				+ TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
				+ "/data/RecursiveChmod-" + TESTCASE_ID + "/reducer.sh\"");

		TestSession.logger.info("Executing command-----------------------:");
		String streamJobCommand = sb.toString();
		streamJobCommand = streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);
		TestSession.logger.info("-----------------------till here:");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));

		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info(genericCliResponseBO.response);

		assertContents(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/recursiveChmod"
						+ "/data/RecursiveChmod-" + TESTCASE_ID
						+ "/expectedOutput", genericCliResponseBO.response);

	}

	@Test
	@Ignore("Ignoring, because test is similar to test50, just a change in location of picking up file://jar")
	public void test60() throws Exception {

	}

	void assertContents(String fileContainingExpectedOutput,
			String outputStringsThatWeGot) {
		ArrayList<String> expectedStringList = new ArrayList<String>();
		List<String> actualStringList = new ArrayList<String>();
		actualStringList = Arrays.asList(outputStringsThatWeGot.split("\n"));
		try {
			FileReader fileReader = new FileReader(fileContainingExpectedOutput);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String currentLine = null;

			while ((currentLine = bufferedReader.readLine()) != null) {
				expectedStringList.add(currentLine);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Assert.fail("File[" + fileContainingExpectedOutput + "]not found");
		} catch (IOException e) {
			Assert.fail("IO exception while reading contents of expected outut into list");
			e.printStackTrace();
		}
		Assert.assertEquals(expectedStringList.size(), actualStringList.size());
		for (int xx = 0; xx < expectedStringList.size(); xx++) {
			Assert.assertTrue(actualStringList.get(xx).trim()
					.equals(expectedStringList.get(xx).trim()));
		}

	}

	@After
	public void deleteDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "RecursiveChmod");
		dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/RecursiveChmod");

	}

}
