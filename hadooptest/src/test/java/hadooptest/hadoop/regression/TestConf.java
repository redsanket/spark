package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class TestConf extends TestSession {

    private String component = HadoopCluster.RESOURCE_MANAGER;
	
    @Test
    public void getHadoopResources() {
        TestSession.logger.info("Hadoop Resources: " + 
                TestSession.cluster.getConf().toString("resources"));
        TestSession.logger.info("Hadoop Resources Properties" + 
                TestSession.cluster.getConf().toString("props"));
    }

	@Test
	public void backupHadoopConf() throws Exception {
	    /* Backup the default configuration directory on the
	     * Resource Manager component host. 
	     */
	    FullyDistributedCluster cluster =
	            (FullyDistributedCluster) TestSession.cluster;
        TestSession.logger.info("Hadoop conf dir on component '" + component +
                "' is '" + cluster.getConf(component).getHadoopConfDir() +
                "'");
	    cluster.getConf(component).backupConfDir();
        TestSession.logger.info("Hadoop conf dir on component '" + component +
                "' is '" + cluster.getConf(component).getHadoopConfDir() +
                "'");
	}

    /* 
     * Check if we need to create a custom conf dir on the component host.
     */
    private void ensureCustomDirExists() throws Exception {
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
        String currentConfDir = cluster.getConf(component).getHadoopConfDir();
        String defaultConfDir = cluster.getConf(component).getDefaultHadoopConfDir();
        if (currentConfDir.equals(defaultConfDir)) {
            cluster.getConf(component).backupConfDir();            
        }
        TestSession.logger.info("Hadoop conf dir on component '" + component +
                "' is '" + cluster.getConf(component).getHadoopConfDir() +
                "'");
    }
    
	@Test
	public void copyFilesToHadoopConf() throws Exception {
	    /* Copy files to the custom configuration directory on the
	     * Resource Manager component host.
	     */
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
	    String sourceDir = "./conf/TestConf/";

        // Check if we need to create a custom conf dir on the component host.
        ensureCustomDirExists();

        cluster.getConf(component).copyFilesToConfDir(sourceDir);
	}

	@Test
	public void modifyHadoopConf() throws Exception {	    
	    /* Insert a property to the yarn-site.xml configuration file on the
	     * Resource Manager component host.
	     */
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
        String confFile = HadoopConfiguration.HADOOP_CONF_YARN;

        // Check if we need to create a custom conf dir on the component host.
        ensureCustomDirExists();

        cluster.getConf(component).setHadoopConfFileProp (
	            "yarn.admin.acl3",
	            "gridadmin,hadoop,hadoopqa,philips,foo", confFile);
	}

	@Test
	public void resetHadoopConf() throws Exception {
	    // Restart the cluster
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;

        // Check if we need to create a custom conf dir on the component host.
        ensureCustomDirExists();

        TestSession.cluster.reset();
	    cluster.waitForSafemodeOff();
	    cluster.isFullyUp();
	}

	@Test
	public void getHadoopConfProperty() throws Exception {
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
	    String propName = "fs.trash.interval";
	    String propValue = cluster.getConf(component).getResourceProp(propName);
	    TestSession.logger.info("Prop name '" + propName + "' = " + propValue);
	}	
}