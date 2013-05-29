package hadooptest.node.hadoop.pseudodistributed;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.pseudodistributed.PseudoDistributedConfiguration;
import hadooptest.node.hadoop.HadoopNode;

/**
 * A Cluster subclass that implements a Fully Distributed Hadoop cluster.
 */
public class PseudoDistributedNode extends HadoopNode {

    /** The base fully distributed configuration. */
    protected PseudoDistributedConfiguration conf;

    /**
     * Initializes the HadoopNode.
     * 
     * @throws Exception if the cluster configuration or cluster nodes
     *         can not be initialized.
     */
    public PseudoDistributedNode(String name, String component)
            throws Exception {
        super(name, component);
        this.conf = new PseudoDistributedConfiguration(
                TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"),
                name, component);
        this.conf.write();
    }

	/**
	 * Initializes the HadoopNode.
	 * 
	 * @throws Exception if the cluster configuration or cluster nodes
	 *         can not be initialized.
	 */
	public PseudoDistributedNode(String name, String component, String conf) 
	    throws Exception {
        super(name, component);
        this.conf = new PseudoDistributedConfiguration(conf, name, component);
	}

    public void setConf(HadoopConfiguration conf) {
        this.conf = (PseudoDistributedConfiguration)conf;
    }

    public PseudoDistributedConfiguration getConf() {
        return this.conf;
    }

}
