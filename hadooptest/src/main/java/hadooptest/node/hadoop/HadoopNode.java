package hadooptest.node.hadoop;

import hadooptest.config.hadoop.HadoopConfiguration;

import java.io.IOException;

/**
 * A Cluster subclass that implements a Fully Distributed Hadoop cluster.
 */
public abstract class HadoopNode {

	/** node name. */
	protected String hostname;

    /** hadoop component type */
    protected String component;
    
    /**
     * Initializes the HadoopNode.
     * 
     * @throws Exception if the cluster configuration or cluster nodes
     *         can not be initialized.
     */
    public HadoopNode() {}

    /**
	 * Initializes the HadoopNode.
	 * 
	 * @throws Exception if the cluster configuration or cluster nodes
	 *         can not be initialized.
	 */
	public HadoopNode(String hostname, String component) {
		this.hostname = hostname;
		this.component = component;
	}

    /**
     * Get the node hostname.
     * 
     * @return hostname String.
     * 
     */
	public String getHostname() {
		return this.hostname;
	}
	
    /**
     * Get the node type.
     * 
     * @return type String.
     * 
     */
    public String getComponent() {
        return this.component;
    }
    
    /**
     * Get the node conf dir.
     * 
     * @return conf dir String.
     * 
     */
    public String getConfDir() {
        return this.getConf().getHadoopConfDir();
    }
    
    /**
     * Get the node default conf dir.
     * 
     * @return conf dir String.
     * 
     */
    public String getDefaultConfDir() {
        return this.getConf().getDefaultHadoopConfDir();
    }
    
    /**
     * Set the node name.
     * 
     * @param name String.
     * 
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
    
    /**
     * Set the node component.
     * 
     * @param component String.
     * 
     */
    public void setComponent(String component) {
        this.component = component;
    }
    
    /**
     * Set the node conf.
     * 
     * @param conf String.
     * 
     */
    public void setConfDir(String conf) {
        this.getConf().setHadoopConfDir(conf);
    }
    
    /**
     * Set the default conf dir.
     * 
     * @param conf String.
     * 
     */
    public void setDefaultConfDir(String conf) throws IOException {
        this.getConf().setDefaultHadoopConfDir(conf);
    }

    /**
     * Get the node conf. object.
     *
     * @param conf String.
     *
     */
    public abstract HadoopConfiguration getConf();

    /**
     * Set the node conf. object.
     *
     * @param conf String.
     *
     */
    public abstract void setConf(HadoopConfiguration conf);
}
