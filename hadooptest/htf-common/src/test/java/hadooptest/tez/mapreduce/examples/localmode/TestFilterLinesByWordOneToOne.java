package hadooptest.tez.mapreduce.examples.localmode;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.tez.HtfTezUtils;
import hadooptest.tez.mapreduce.examples.extensions.FilterLinesByWordOneToOneExtendedForHTF;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestFilterLinesByWordOneToOne extends
		FilterLinesByWordOneToOneExtendedForHTF {

		@BeforeClass
		public static void beforeClass() {
			TestSession.start();
		}

		@Test
		public void testLocalMode() throws Exception {
			/**
			 * Usage: filterLinesByWordOneToOne <in> <out> <filter_word>" 
		        + " [-generateSplitsInClient true/<false>]
			 */
			long timeStamp = System.currentTimeMillis();
			String[] filterLinesByWordOneToOneArgs = new String[] { "/tmp/tez-site.xml",
					"/tmp/filterLinesByWordOneToOne-out-" + timeStamp, "tez", "-generateSplitsInClient true" };

			int returnCode = run(filterLinesByWordOneToOneArgs, HadooptestConstants.Mode.LOCAL);
			Assert.assertTrue(returnCode==0);
		}


		@After
		public void deleteTezStagingDirs() throws Exception {
			DfsCliCommands dfsCliCommands = new DfsCliCommands();
			dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
					SkipTrash.YES, "/tmp/tez/");
			dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
					SkipTrash.YES, "/tmp/filterLinesByWordOneToOne-out-*");

		}

	}

