package hadooptest.tez.mapreduce.examples.cluster;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.HtfTezUtils;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordOneToOneExtendedForHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestFilterLinesByWordOneToOne extends
		FilterLinesByWordOneToOneExtendedForHTF {
	public static String SOURCE_FILE = "/home/y/share/htf-data/excite-small.log";
	public static String INPUT_FILE = "/tmp/excite-small.log";
	public static String OUTPUT_LOCATION = "/tmp/outOfFilterLinesByWordOneToOne";

		@BeforeClass
		public static void beforeClass() {
			TestSession.start();
		}

		@Before
		public void copyTheFileOnHdfs() throws Exception {
			DfsCliCommands dfsCliCommands = new DfsCliCommands();

			GenericCliResponseBO genericCliResponse = dfsCliCommands.put(
					DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"),
					SOURCE_FILE, INPUT_FILE);
			Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		}

		@Test
		public void testFilterLinesByWordWithClientSplitsRunOnCluster() throws Exception {
			/**
			 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" 
		        + " [-generateSplitsInClient true/<false>]
			 */
			String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
					OUTPUT_LOCATION, "lionking", "-generateSplitsInClient true" };

			int returnCode = run(filterLinesByWordOneToOneArgs, HadooptestConstants.Execution.TEZ);
			Assert.assertTrue(returnCode==0);
		}

		@Test
		public void testFilterLinesByWordNoClientSplitsRunOnCluster() throws Exception {
			/**
			 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" 
		        + " [-generateSplitsInClient true/<false>]
			 */
			String[] filterLinesByWordOneToOneArgs = new String[] { INPUT_FILE,
					OUTPUT_LOCATION, "lionking" };

			int returnCode = run(filterLinesByWordOneToOneArgs, HadooptestConstants.Execution.TEZ);
			Assert.assertTrue(returnCode==0);
		}

		
		@After
		public void deleteOutputDirs() throws Exception {
			DfsCliCommands dfsCliCommands = new DfsCliCommands();
			dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
					SkipTrash.YES, OUTPUT_LOCATION);

		}

	}

