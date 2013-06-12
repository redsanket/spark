package hadooptest.node.hadoop.fullydistributed;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.node.hadoop.HadoopNode;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * A Cluster subclass that implements a Fully Distributed Hadoop cluster.
 */
public class FullyDistributedNode extends HadoopNode {

    /** The base fully distributed configuration. */
    protected FullyDistributedConfiguration conf;

    /**
     * Initializes the HadoopNode.
     * 
     * @throws Exception if the cluster configuration or cluster nodes
     *         can not be initialized.
     */
    public FullyDistributedNode(String name, String component)
            throws Exception {
        super(name, component);
        initDefault(getDefaultHadoopConfDir());
    }

	/**
	 * Initializes the HadoopNode.
	 * 
	 * @throws Exception if the cluster configuration or cluster nodes
	 *         can not be initialized.
	 */
	public FullyDistributedNode(String name, String component, String conf)
	        throws Exception {
	    super(name, component);
        initDefault(conf);
	}

	/**
	 *  Initialize the node configuration object.
	 */
	protected void initDefault(String defaultHadoopConfDir) throws Exception {
	    TestSession.logger.info("Init FDNode: Init FDConf: default conf dir='" +
                defaultHadoopConfDir + "'");
        this.conf = new FullyDistributedConfiguration(
                defaultHadoopConfDir, this.hostname, this.component);        
        TestSession.logger.trace("Init'd FDConf");
	}
	
    private String getDefaultHadoopConfDir() throws IOException {
        // Use the custom default Hadoop conf dir as the default if it exists.
        String customDefaultHadoopConfDir =
                HadoopCluster.getDefaultConfSettingsFile(
                        this.component, this.hostname);
        File customDefaultConfFile = new File(customDefaultHadoopConfDir);

        if (customDefaultConfFile.exists()) {
            TestSession.logger.info("Found existing custom default " +
                    "Hadoop conf dir: '" + customDefaultHadoopConfDir );
        } else {
            TestSession.logger.trace("Did not find custom default " +
                    "Hadoop conf dir: '" + customDefaultHadoopConfDir + "'.");            
            TestSession.logger.trace("Use installed default " +
                    "Hadoop conf dir: '" +
                    TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR") +
                    "'.");
        }
        
        String defaultHadoopConfDir = (customDefaultConfFile.exists()) ?
                FileUtils.readFileToString(customDefaultConfFile) :
                    TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR");
        return defaultHadoopConfDir;
    }

    public void setConf(HadoopConfiguration conf) {
        this.conf = (FullyDistributedConfiguration)conf;
    }

    public FullyDistributedConfiguration getConf() {
        return this.conf;
    }
}
