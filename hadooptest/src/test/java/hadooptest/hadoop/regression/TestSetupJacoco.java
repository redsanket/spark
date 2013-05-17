package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.SetupTests;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;

@Category(SetupTests.class)
public class TestSetupJacoco extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void setupJacocoConf() throws Exception {
	    FullyDistributedCluster cluster =
	            (FullyDistributedCluster) TestSession.cluster;

	    /* Backup the default configuration directory for each component, for
	     * each component hosts.
	     */
	    for (String component : HadoopCluster.components ) {
	        // Get the component nodes
	        Hashtable<String, HadoopNode> cNodes =
	                TestSession.cluster.getNodes(component);
	        // iterate through the component nodes
	        Enumeration<HadoopNode> elements = cNodes.elements();
	        while (elements.hasMoreElements()) {
	            
	            // Get the component node, back up the config dir, and set it
	            // as the custom default conf dir.
	            FullyDistributedNode node =
	                    (FullyDistributedNode) elements.nextElement();
	            node.getConf().backupConfDir();
	            String customConfDir = node.getConf().getHadoopConfDir();
	            node.getConf().setDefaultHadoopConfDir(customConfDir);
	            
	            // Copy the jacoco environment settings file to the custom
	            // configuration directory.
	            String sourceFile = "conf/jacoco/jacoco-env.sh";
	            assertTrue("Copy jacoco env file '" + sourceFile + "' to " +
	                    "the Hadoop '" + component + "' host '" +
	                    node.getHostname() + "' config dir '" + customConfDir +
	                    "' failed.",
	                    node.getConf().copyFileToConfDir(sourceFile));
	            
	            // Update the Hadoop configuration files with the jacoco options
                String targetFile = null;
	            String matchStr = null;
	            String appendStr = null;
	                
	            // Update the hadoop-env.sh conf file with jacoco option.
	            // This affects the NN, DN.
                targetFile = customConfDir + "/hadoop-env.sh";                
	            matchStr = "The java implementation to use.  Required.";
	            appendStr = ". " + customConfDir + "/jacoco-env.sh";
	            node.getConf().insertBlock(matchStr, appendStr, targetFile,
	                    "\n", "\n");
	            
                // Update the hadoop-env.sh conf file with jacoco option.
                // This affects the NN.
	            matchStr = "-Xms14000m";
	            appendStr = "-Xms14000m \\$\\{JACOCO_NN_OPT\\}";
	            node.getConf().replaceBlock(matchStr, appendStr,
	                    targetFile, "", "");

                // Update the hadoop-env.sh conf file with jacoco option.
                // This affects the DN.
	            // NOTE: this is currently not working due to file permission 
	            // issue. Because the DN processes are started by jsvc, and it
	            // has a umask issue where files are created with 077 as hdfsqa
	            // user, hoadoopqa does not have read permssion to access the
	            // data in the file.  Explore changing the dist QE yinst package
	            // to explicitly change the hdfsqa umask to allow read
	            // permission to group. 
	            /*
	                matchStr = "ERROR,DRFAS";
	                    appendStr = "ERROR,DRFAS \\$\\{JACOCO_DN_OPT\\}";
	                cluster.getConf().replaceBlock(matchStr, appendStr,
	                        targetFile, "", "");
	             */
	                
	            // TODO: This did not generate a coverage file
	            matchStr = "-Xmx3G";
	            appendStr = "-Xmx3G \\$\\{JACOCO_BAL_OPT\\}";
	            node.getConf().replaceBlock(matchStr, appendStr, targetFile,
	                    "", "");
	                

	            // Update the yarn-env.sh conf file with jacoco option.
	            // This affects the RM, NM.
                targetFile = customConfDir + "/yarn-env.sh";                
	            matchStr = "limitations under the License.";
	            appendStr = ". " + customConfDir + "/jacoco-env.sh";
	            node.getConf().insertBlock(matchStr, appendStr, targetFile,
	                    "\n", "\n");

	            // matchStr = "$YARN_RESOURCEMANAGER_OPTS -Dyarn.rm.audit.logger=${YARN_RM_AUDIT_LOGGER:-INFO,RMAUDIT}";
	            matchStr = "RMAUDIT}";
	            appendStr = "RMAUDIT} \\$\\{JACOCO_RM_OPT\\}";
	            node.getConf().replaceBlock(matchStr, appendStr, targetFile,
	                    "", "");

	            // NOTE: jacoco does not support concurrent write. Therefore, 
	            // each DN/NM host needs to have a unique jacoco .exec coverage
	            // file created.
	            matchStr = "YARN_NM_AUDIT_LOGGER:-INFO,NMAUDIT}";
	            appendStr =
	                    "YARN_NM_AUDIT_LOGGER:-INFO,NMAUDIT}} \\$\\{JACOCO_NM_OPT\\}";
	            node.getConf().replaceBlock(matchStr, appendStr, targetFile,
	                    "", "");
	            
	            // This did not generate a coverage file.
	            matchStr = "HADOOP_JHS_GC_OPTS}";
	            appendStr = "HADOOP_JHS_GC_OPTS} \\$\\{JACOCO_JHS_OPT\\}";
	            node.getConf().replaceBlock(matchStr, appendStr, targetFile,
	                    "", "");                
	        }            
	    }

	    // Restart the cluster
	    TestSession.cluster.reset();
	    cluster.waitForSafemodeOff();
	    cluster.isFullyUp();			
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