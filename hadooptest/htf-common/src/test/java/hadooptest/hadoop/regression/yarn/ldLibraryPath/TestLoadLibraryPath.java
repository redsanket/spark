package hadooptest.hadoop.regression.yarn.ldLibraryPath;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestLoadLibraryPath extends YarnTestsBaseClass {
	String param1;
	String whatWeAreGreppingFor;


	@BeforeClass
	public static void ensureDataPresenceInCluster() throws Exception{
		DfsTestsBaseClass.ensureDataPresenceInCluster();
	}
	public TestLoadLibraryPath(String param1, String whatWeAreGreppingFor) {
		this.param1 = param1;
		this.whatWeAreGreppingFor = whatWeAreGreppingFor;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {

		return Arrays.asList(new Object[][] {
				{ "", "LD_LIBRARY_PATH" },
				{ "Dmapred.child.env=myName=myValue", "myName=myValue" },
				{ "Dmapred.child.env=HOME=/user/newuser", "newuser" },
				{ "Dmapred.child.env=HADOOP_HOME=$HADOOP_HOME:dummyName=",
						"dummyName" } ,
				{"Dmapred.child.env=PATH=$PATH:newname=newvalue", "newname=newvalue"}
		});
	}

	void verifyEnvVariable(String param1, String whatWeAreLookingFor)
			throws Exception {
		StringBuilder sb = new StringBuilder();
		String outPath = "/tmp/" + System.currentTimeMillis();
		sb.append("-Dmapreduce.job.ubertask.enable=false" + param1
				+ " -input /HTF/testdata/dfs/file_1B  -output " + outPath
				+ " -mapper \"env\" -reducer \"NONE\"");
		runStdHadoopStreamingJob(sb.toString().split("\\s+"));
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO = dfsCliCommands.cat(
				EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA, "",
				System.getProperty("CLUSTER_NAME"), outPath + "/*");
		Assert.assertTrue("cat command exited with a non-zero code",
				genericCliResponseBO.process.exitValue() == 0);
		Assert.assertTrue("Could not find " + whatWeAreLookingFor + " in cat output",
				genericCliResponseBO.response.contains(whatWeAreLookingFor));

	}

	@Test
	public void testLoadLibPath() throws Exception {
		TestSession.logger.info("------------- Running test for[" + param1
				+ "] and [" + whatWeAreGreppingFor + "]-----------");
		verifyEnvVariable(param1, whatWeAreGreppingFor);

	}
}
