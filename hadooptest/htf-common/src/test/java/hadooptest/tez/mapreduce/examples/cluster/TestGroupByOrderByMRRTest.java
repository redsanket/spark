package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.HtfTezUtils;
import hadooptest.tez.mapreduce.examples.extensions.GroupByOrderByMRRTestExtendedForTezHTF;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestGroupByOrderByMRRTest extends
		GroupByOrderByMRRTestExtendedForTezHTF {

	private static String INPUT_FILE_NAME = "/tmp/input-GroupByOrderByMRR.txt";
	private static String OUTPUT_FILE_NAME = "/tmp/output-GroupByOrderByMRR.txt";
	private static String SHELL_SCRIPT_LOCATION = "/tmp/dataCreationScriptForTestGroupByOrderByMRR.sh";

	@BeforeClass
	public static void beforeClass() throws Exception {
		TestSession.start();
		generateTestData();

	}

	private static void copyDataIntoHdfs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO response = dfsCliCommands.put(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), INPUT_FILE_NAME,
				INPUT_FILE_NAME);
		Assert.assertTrue(response.process.exitValue() == 0);

	}

	private static void generateTestData() throws IOException,
			InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("#/bin/bash" + "\n");
		sb.append("i=1000" + "\n");
		sb.append("j=1000" + "\n");
		sb.append("id=0" + "\n");
		sb.append("while [[ \"$id\" -ne \"$i\" ]]" + "\n");
		sb.append("do" + "\n");
		sb.append("id=`expr $id + 1`" + "\n");
		sb.append("deptId=`expr $RANDOM % $j + 1`" + "\n");
		sb.append("deptName=`echo \"ibase=10;obase=16;$deptId\" | bc`" + "\n");
		sb.append("echo \"$id O$deptName\"" + "\n");
		sb.append("done" + "\n");

		File file = new File(SHELL_SCRIPT_LOCATION);

		if (file.exists()) {
			if (!file.canExecute()) {
				file.setExecutable(true);
			}
		} else {
			file.createNewFile();
			Writer writer = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(writer);
			bw.write(sb.toString());
			bw.close();
			writer.close();
			file.setExecutable(true);
		}

		ProcessBuilder builder = new ProcessBuilder("sh", file.getAbsolutePath());
		builder.redirectOutput(new File(INPUT_FILE_NAME));
		builder.redirectErrorStream();
		Process proc = builder.start();
		Assert.assertTrue(proc.waitFor()==0);
		Assert.assertEquals(proc.exitValue(), 0);

	}

	@Test
	public void testGroupByOrderByMRRTestRunOnCluster() throws Exception {
		/**
		 * Usage: groupbyorderbymrrtest <in> <out>
		 */
		copyDataIntoHdfs();
		long timeStamp = System.currentTimeMillis();
		String[] groupByOrderByMrrArgs = new String[] { INPUT_FILE_NAME,
				OUTPUT_FILE_NAME + "/" + timeStamp };

		int returnCode = run(groupByOrderByMrrArgs, HadooptestConstants.Execution.TEZ);
		Assert.assertTrue(returnCode==0);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, OUTPUT_FILE_NAME);


	}

}
