package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.SetupTests;
import hadooptest.TestSession;
import hadooptest.SerialTests;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;

@Category(SetupTests.class)
public class TestSetupJacoco extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start(    );
	}
	
	@Test
	public void setupJacocoConf() {
		try {
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			// String component = HadoopConfiguration.RESOURCE_MANAGER;

			// Backup the default configuration directory
	        for (String component : HadoopCluster.components ) {
	            cluster.getConf().backupConfDir(component);
	            String customConfDir = 
	                    cluster.getConf().getHadoopConfDir(component);
	            cluster.getConf().setHadoopDefaultConfDir(
	                    customConfDir, component);

	            // Copy file to the custom configuration directory
                String sourceFile = "conf/jacoco/jacoco-env.sh";
                assertTrue("Copy jacoco config to Hadoop configuration " + 
                        "directory for component '" + component + "' failed.",
                        cluster.getConf().copyFileToConfDir(
                                sourceFile, component));
	            
	            // Copy files to the custom configuration directory
	            /*
	            String sourceDir = "conf/jacoco";
	            assertTrue("Copy jacoco settings to Hadoop configuration " + 
	                    "directory for component '" + component + "' failed.",
	                    cluster.getConf().copyFilesToConfDir(
	                            sourceDir, component));
	                            */

                String matchStr = null;
                String appendStr = null;
                String targetFile = null;
                
                // hadoop-env.sh - NN, DN
                matchStr = "The java implementation to use.  Required.";
                appendStr = ". " + customConfDir + "/jacoco-env.sh";
                targetFile = customConfDir + "/hadoop-env.sh";                
                cluster.getConf().insertBlock(component, matchStr, appendStr,
                        targetFile, "\n", "\n");

                // hadoop-env.sh - NN
                matchStr = "-Xms14000m";
                appendStr = "-Xms14000m \\$\\{JACOCO_NN_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");

                // hadoop-env.sh - DN
                /*
                matchStr = "ERROR,DRFAS";
                appendStr = "ERROR,DRFAS \\$\\{JACOCO_DN_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");
                */
                
                // did not generate a coverage file
                matchStr = "-Xmx3G";
                appendStr = "-Xmx3G \\$\\{JACOCO_BAL_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");
                
                
                // yarn-env.sh - RM, NM
                matchStr = "limitations under the License.";
                appendStr = ". " + customConfDir + "/jacoco-env.sh";
                targetFile = customConfDir + "/yarn-env.sh";                
	            cluster.getConf().insertBlock(component, matchStr, appendStr,
	                    targetFile, "\n", "\n");

	            // matchStr = "$YARN_RESOURCEMANAGER_OPTS -Dyarn.rm.audit.logger=${YARN_RM_AUDIT_LOGGER:-INFO,RMAUDIT}";
                matchStr = "RMAUDIT}";
                appendStr = "RMAUDIT} \\$\\{JACOCO_RM_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");
	            
                matchStr = "YARN_NM_AUDIT_LOGGER:-INFO,NMAUDIT}";
                appendStr = "YARN_NM_AUDIT_LOGGER:-INFO,NMAUDIT}} \\$\\{JACOCO_NM_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");
                
                // did not generage a coverage file ?
                matchStr = "HADOOP_JHS_GC_OPTS}";
                appendStr = "HADOOP_JHS_GC_OPTS} \\$\\{JACOCO_JHS_OPT\\}";
                cluster.getConf().replaceBlock(component, matchStr, appendStr,
                        targetFile, "", "");
                
	        }

            // Restart the cluster
            TestSession.cluster.reset();
            cluster.waitForSafemodeOff();
            cluster.isFullyUp();			
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	// @Test
	public void runSampleTest() {
		// This will not return the handle until the job is completed.
		// Also, it does not return standard output and standard error
		try {
			String[] args = { "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"};
			Configuration conf = TestSession.cluster.getConf();

			int rc;
			TestSession.cluster.setSecurityAPI("keytab-hadoop1", "user-hadoop1");
			rc = ToolRunner.run(conf, new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}

			TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
			rc = ToolRunner.run(conf, new SleepJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}
		}
		catch (Exception e) {
			TestSession.logger.error("Job failed!!!", e);
			fail();
		}
	}

	// @Test
	public void resetHadoopConf() {
		try {
            // Reset Hadoop configuration directory to the installed default
			FullyDistributedCluster cluster =
			        (FullyDistributedCluster) TestSession.cluster;
			cluster.getConf().resetHadoopDefaultConfDir();
			cluster.getConf().resetHadoopConfDir();

			// Restart the cluster
			TestSession.cluster.reset();
			cluster.waitForSafemodeOff();
			cluster.isFullyUp();
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

}