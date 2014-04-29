/*
 * YAHOO!
 * 
 */

package hadooptest.config.hadoop.pseudodistributed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;

/**
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 */
public class PseudoDistributedConfiguration extends HadoopConfiguration
{

	/**
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a pseudodistributed cluster under test.
	 * Hadoop default configuration is not used.
	 * 
	 * @throws UnknownHostException if there is a fatal error initializing the
	 * default configuration.
	 */
	public PseudoDistributedConfiguration() throws Exception {
		super(false);
		this.initDefaults();
	}

    /**
     * Calls the superclass constructor, and initializes the default
     * configuration parameters for a pseudodistributed cluster under test.
     * Hadoop default configuration is not used.
     * 
     * @throws UnknownHostException if there is a fatal error initializing the
     * default configuration.
     */
    public PseudoDistributedConfiguration(String confDir, String hostname,
            String component) throws Exception {
        super(false, confDir, hostname, component);
        this.initDefaults();
    }

    /**
     * Calls the superclass constructor, and initializes the default
     * configuration parameters for a pseudodistributed cluster under test.
     * Hadoop default configuration is not used.
     * 
     * @throws UnknownHostException if there is a fatal error initializing the
     * default configuration.
     */
    public PseudoDistributedConfiguration(String hostname, String component)
            throws Exception {
        super(false, TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"),
                hostname, component);
        this.initDefaults();
    }

    /**
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter,
	 * before the pseudodistributed test cluster default configuration is 
	 * initialized into the configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration 
	 * parameters specified by the Hadoop installation, before loading the
	 * class configuration defaults.
	 * 
	 * @throws UnknownHostException if there is a fatal error initializing the 
	 * default configuration.
	 */
	public PseudoDistributedConfiguration(boolean loadDefaults) 
	        throws Exception {
		super(loadDefaults); 
		this.initDefaults();
	}

	
	/**
     * Initializes a set of default configuration properties that have been 
     * determined to be a reasonable set of defaults for running a 
     * pseudodistributed cluster under test.
     */
    private void initDefaults() {
        String confDir = this.hadoopConfDir;
        set("fs.default.name", "hdfs://localhost/");
        set("dfs.replication", "1");
        set("mapreduce.framework.name", "yarn");
        set("yarn.resourcemanager.address", "localhost:8032");
        set("yarn.nodemanager.aux-services", "mapreduce.shuffle");
        set("mapreduce.jobhistory.intermediate-done-dir",
                confDir + "jobhistory/tmp");
        set("mapreduce.jobhistory.done-dir",
                confDir + "jobhistory/done");
        set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
    }

	/**
	 * Initializes cluster-specific properties defaults.
	 */
	protected void initDefaultsClusterSpecific() {
 
	}
	
	/**
	 * Writes the pseudodistributed cluster configuration specified by the 
	 * object out to disk.
	 * 
	 * @throws IOException if there is a problem writing configuration files
	 * to disk.
	 */
	public void write() throws IOException {
	    String confDir = this.hadoopConfDir;
	    
		File outdir = new File(confDir);
		outdir.mkdirs();
		
		File historytmp = new File(confDir + "jobhistory/tmp");
		historytmp.mkdirs();
		File historydone = new File(confDir + "jobhistory/done");
		historydone.mkdirs();

		File core_site = new File(confDir + "core-site.xml");
		File hdfs_site = new File(confDir + "hdfs-site.xml");
		File yarn_site = new File(confDir + "yarn-site.xml");
		File mapred_site = new File(confDir + "mapred-site.xml");		

		core_site.createNewFile();
		hdfs_site.createNewFile();
		yarn_site.createNewFile();
		mapred_site.createNewFile();

		FileOutputStream out = new FileOutputStream(core_site);
		this.writeXml(out);

		out = new FileOutputStream(hdfs_site);
		this.writeXml(out);

		out = new FileOutputStream(yarn_site);
		this.writeXml(out);

		out = new FileOutputStream(mapred_site);
		this.writeXml(out);

		FileWriter slaves_file = new FileWriter(confDir + "slaves");
		BufferedWriter slaves = new BufferedWriter(slaves_file);
		slaves.write("localhost");
		slaves.close();
	}

	/**
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 */
	public void cleanup() {
	    String confDir = this.hadoopConfDir;
		TestSession.logger.info("Clean up and delete the content of the " +
		        "Hadoop configuration directory '" + confDir + "'.");
		File core_site = new File(confDir + "/core-site.xml");
		File hdfs_site = new File(confDir + "/hdfs-site.xml");
		File yarn_site = new File(confDir + "/yarn-site.xml");
		File mapred_site = new File(confDir + "/mapred-site.xml");	
		File slaves = new File(confDir + "/slaves");	
		File log4jProperties = new File(confDir + "/log4j.properties");

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
	}	
}
