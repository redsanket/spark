package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.mapreduce.examples.extensions.UnionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class has the real test methods meant to be run on the cluster. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.localmode
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */
@Category(SerialTests.class)
public class TestUnionExample extends UnionExampleExtendedForTezHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/pig_methods_dataset1";
	public static String INPUT_FILE = "/tmp/union-input-file";
	public static String OUTPUT_LOCATION = "/tmp/union-output";

	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}

	@Rule
	public TestName testName = new TestName();

	@Before
	public void copyTheFileLocally() {
		try {
			FileUtils.copyFile(new File(SOURCE_FILE), new File(INPUT_FILE));
		} catch (IOException ioEx) {
			if (ioEx.getMessage().contains("exists")) {
				// Ignore, 'cos the input file already exists
			}
		}

	}

	@Test
	public void testUnionExampleOnLocalModeWithSession() throws Exception {

		boolean returnCode = run(INPUT_FILE, OUTPUT_LOCATION,
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,
						testName.getMethodName()),
				HadooptestConstants.Execution.TEZ_LOCAL, Session.YES,
				testName.getMethodName());
		Assert.assertTrue(returnCode);
	}

	@Test
	public void testUnionExampleOnLocalModeWithoutSession() throws Exception {

		boolean returnCode = run(INPUT_FILE, OUTPUT_LOCATION,
				HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(),
						HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,
						testName.getMethodName()),
				HadooptestConstants.Execution.TEZ_LOCAL, Session.NO,
				testName.getMethodName());
		Assert.assertTrue(returnCode);
	}

	@After
	public void deleteTezStagingDirs() throws Exception {
		FileUtils.deleteQuietly(new File(INPUT_FILE));
		FileUtils.deleteQuietly(new File(OUTPUT_LOCATION));

	}

}
