/*
 * YAHOO!
 */

package hadooptest.config.hadoop;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;

import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;

/** 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class HadoopConfiguration extends Configuration {

	/** Filename of the core hadoop configuration xml file. */
	public static final String HADOOP_CONF_CORE = "core-site.xml";
	
	/** Filename of the hdfs configuration xml file. */
	public static final String HADOOP_CONF_HDFS = "hdfs-site.xml";
	
	/** Filename of the mapreduce configuration xml file. */
	public static final String HADOOP_CONF_MAPRED = "mapred-site.xml";

	/** Filename of the yarn configuration xml file. */
	public static final String HADOOP_CONF_YARN = "yarn-site.xml";

	/** Filename of the capacity scheduler configuration xml file. */
	public static final String HADOOP_CONF_CAPACITY_SCHEDULER =
	        "capacity-scheduler.xml";

	/** Filename of the fair scheduler configuration xml file. */
	public static final String HADOOP_CONF_FAIR_SCHEDULER = 
	        "fair-scheduler.xml";

	/** String representing the name node. */
	public static final String NAMENODE = "namenode";

	/** String representing the resource manager. */
	public static final String RESOURCE_MANAGER = "resourcemanager";

	/** String representing the data node. */
	public static final String DATANODE = "datanode";

	/** String representing the node manager. */
	public static final String NODEMANAGER = "nodemanager";

	/** String representing the gateway. */
	public static final String GATEWAY = "gateway";

	/** */
	public static final String[] components = {
	    "namenode", "resourcemanager", "datanode", "gateway", "nodemanager"};
	
	/** General Hadoop configuration properties such as cluster name, 
	 * directory paths, etc.
	 */
    protected Properties hadoopProps = new Properties();

    /** Track Hadoop override configuration directories */
    protected Properties hadoopConfDirProps = new Properties();

	/** 
	 * A generic constructor TestConfiguration that calls the Hadoop
	 * Configuration with the false argument, so that you are not loading any
	 * default Hadoop configuration properties.  It then proceeds to
	 * initialize the default configuration for the reflected cluster type.
	 * 
	 * @throws Exception if the default hosts can not be initialized, or there
	 * is a problem getting the Hadoop version.
	 */
	public HadoopConfiguration() throws Exception {
		super(true);		
		this.initDefaults();
	}

	/**
	 * A constructor that allows you to specify whether or not you would like
	 * the Hadoop Configuration to load default Hadoop config properties.
	 * It then proceeds to initialize the default configuration for the 
	 * reflected cluster type.
	 * 
	 * @param loadDefaults whether or not to load the cluster configuration
	 * defaults using the Configuration superclass constructor.
	 * 
	 * @throws Exception if the default hosts can not be initialized or there is
	 * a problem getting the Hadoop version.
	 */
	public HadoopConfiguration(boolean loadDefaults) throws Exception {
		super(loadDefaults);
		this.initDefaults();
	}

	/**
	 * A constructor that allows you to specify a custom configuration.
	 * 
	 * @param other a custom Configuration.
	 * 
	 * @throws Exception if the default hosts can not be initialized or there
	 *                   is a problem getting the Hadoop version.
	 */
	public HadoopConfiguration(Configuration other) throws Exception {
		super(other);
		this.initDefaults();
	}

	/**
	 * Cleans up any test configuration written to disk.
	 */
	public abstract void cleanup();
	
	/**
	 * Writes any test configuration to disk.
	 * 
	 * @throws IOException if the configuration can not be written to disk.
	 */
	public abstract void write() throws IOException;
	
    /**
     * Returns the Hadoop general property value for a given property name.
     * 
     * @return String property value such as cluster name, directory paths, etc. 
     */
	public String getHadoopProp(String key) {
		if (!hadoopProps.getProperty(key).equals(null)) {
    		return this.hadoopProps.getProperty(key);
    	}
    	else {
			TestSession.logger.error(
				"Couldn't find value for key '" + key + "'.");
			return "";
    	}
    }

	/**
     * Returns the Hadoop configuration directory paths.
     * 
     * @return Properties the Hadoop configuration directory paths by
     * components.
     */
	public Properties getHadoopConfDirProps() {
    	return this.hadoopConfDirProps;
    }

    /**
     * Returns the Hadoop configuration directory path for the default gateway
     * component.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDir() {
		return this.getHadoopConfDir(null);
	}
	

    /**
     * Returns the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDir(String component) {
		if ((component == null) || component.isEmpty()) {
			component = HadoopConfiguration.GATEWAY;
		}
		return this.getHadoopConfDirProps().getProperty(component,
				this.getHadoopProp("HADOOP_DEFAULT_CONF_DIR"));
	}
	
    /**
     * Set the Hadoop configuration directory path for all components.
     * 
     * @param path String of the directory path name.
     * 
     */
	public void setHadoopConfDir(String path) {		
		for (String component : components ) {
			this.getHadoopConfDirProps().setProperty(component, path);
		}
	}

    /**
     * Set the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * @param path String of the directory path name.
     * 
     */
	public void setHadoopConfDir(String component, String path) {
		this.getHadoopConfDirProps().setProperty(component, path);
	}

	/**
	 * Initializes cluster specific defaults.
	 * 
	 * @throws UnknownHostException if the default hosts can not be initialized.
	 */
	protected abstract void initDefaultsClusterSpecific()
	        throws UnknownHostException;
	
	/**
	 * Setup the Kerberos configuration for the given user name and keytab file
	 * in the parent class Apache Hadoop Configuration object. This will be
	 * needed later for tasks such as job submission. 
	 */
	private void setKerberosConf(String user) {
		super.set("user-" + user, user + "@DEV.YGRID.YAHOO.COM");
		super.set("keytab-" + user, "/homes/" + user + "/" + user +
		        ".dev.headless.keytab");
	}
	
	/**
	 * Setup the Kerberos configuration for all headless users in the
	 * parent class Apache Hadoop Configuration object. This will be
	 * needed later for tasks such as job submission. 
	 */
	private void setKerberosConf() {
		// Setup the headless users
		String[] users = {"hadoopqa", "hdfs", "hdfsqa", "mapred", "mapredqa"};
		for (String user : users ) {
			this.setKerberosConf(user);
		}
		// Setup the headless users hadoop1 through hadoop20
		for(int i = 0; i < 20; i++) {
			this.setKerberosConf("hadoop" + (i+1));
		}
	}
	
	/**
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 * 
	 * @throws Exception if the default hosts can not be initialized, or if
	 *                   there is a fatal error getting the Hadoop version.
	 */
	private void initDefaults() throws Exception {

		this.setKerberosConf();
		this.initDefaultsClusterSpecific();
		
		/* 
		 * Properties beyond this point should be common across pseudo and fully
		 * distributed cluster configuration.
		 */
		loadDefaultResource();

		// Configuration directory and files
		String confDir=
				hadoopProps.getProperty("HADOOP_INSTALL") + "/conf/hadoop";
		hadoopProps.setProperty("HADOOP_DEFAULT_CONF_DIR", confDir);
				
		// Initialize the active Hadoop conf dir properties
        // hadoopProps.setProperty("HADOOP_CONF_DIR", confDir);
		this.setHadoopConfDir(confDir);
		
		/*
		 * Use this.getHadoopConfFile() instead because the configuration
		 * directory may change.
		 * hadoopProps.setProperty("HADOOP_CONF_CORE", confDir + "/" + HADOOP_CONF_CORE);
		 * hadoopProps.setProperty("HADOOP_CONF_HDFS", confDir + "/" + HADOOP_CONF_HDFS);
		 * hadoopProps.setProperty("HADOOP_CONF_MAPRED", confDir + "/" + HADOOP_CONF_MAPRED);
		 * hadoopProps.setProperty("HADOOP_CONF_YARN", confDir + "/" + HADOOP_CONF_YARN);
		 * hadoopProps.setProperty("HADOOP_CONF_CAPACITY_SCHEDULER", confDir + "/" + HADOOP_CONF_CAPACITY_SCHEDULER);
		 * hadoopProps.setProperty("HADOOP_CONF_FAIR_SCHEDULER", confDir + "/" + HADOOP_CONF_FAIR_SCHEDULER);
		*/

		// Binaries
		hadoopProps.setProperty("HADOOP_BIN_DIR", hadoopProps.getProperty("HADOOP_COMMON_HOME") + "/bin");
		hadoopProps.setProperty("HADOOP_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hadoop");
		hadoopProps.setProperty("HDFS_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hdfs");
		hadoopProps.setProperty("MAPRED_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/mapred");
		hadoopProps.setProperty("YARN_BIN", getHadoopProp("HADOOP_BIN_DIR") + "/yarn");

		// Version dependent environment variables
		String HADOOP_VERSION = this.getVersion();

		hadoopProps.setProperty("HADOOP_VERSION", HADOOP_VERSION);
		
		// Jars
		hadoopProps.setProperty("HADOOP_JAR_DIR", getHadoopProp("HADOOP_COMMON_HOME") +
				"/share/hadoop");
		hadoopProps.setProperty("HADOOP_TEST_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
				"/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
				HADOOP_VERSION + "-tests.jar"); 
		hadoopProps.setProperty("HADOOP_EXAMPLE_JAR", getHadoopProp("HADOOP_JAR_DIR") +
				"/mapreduce/" + "hadoop-mapreduce-examples-" +
				HADOOP_VERSION + ".jar"); 
		hadoopProps.setProperty("HADOOP_MR_CLIENT_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
				"/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
				HADOOP_VERSION + ".jar"); 
		hadoopProps.setProperty("HADOOP_STREAMING_JAR", getHadoopProp("HADOOP_JAR_DIR") +
				"/tools/lib/" + "hadoop-streaming-" + 
				HADOOP_VERSION + ".jar");

		loadClusterResource();
	}

	/**
	 * core-default.xml contains at least two properties that must be
	 * defined in the Hadoop Configuration instance in order for the
	 * test framework to interact with the Hadoop Classes and APIs.
	 * Therefore, we are loading core-default.xml here so they will be
	 * defined.
	 * 
	 * Here are the two properties that must be defined:
	 * this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	 * this.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
	 * 
	 * NOTE:
	 * Consider loading part or all of the following default xml files:
	 * this.addResource(this.getClassLoader().getResourceAsStream("core-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("hdfs-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("mapred-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("yarn-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("distcp-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("httpfs-default.xml"));
	 * this.addResource(this.getClassLoader().getResourceAsStream("testserver-default.xml"));
	 */
	protected void loadDefaultResource() {
        URL dirURL = this.getClass().getClassLoader().getResource("core-default.xml");
        TestSession.logger.debug("Load hadoop default configurations via " +
        		"URL path:");
        TestSession.logger.debug("URL path: '" + dirURL.getPath() + "',...,etc.");
		super.addResource(this.getClassLoader().getResourceAsStream("core-default.xml"));
		super.addResource(this.getClassLoader().getResourceAsStream("hdfs-default.xml"));
		super.addResource(this.getClassLoader().getResourceAsStream("mapred-default.xml"));
		super.addResource(this.getClassLoader().getResourceAsStream("yarn-default.xml"));
	}
	
	protected void loadClusterResource() {
		TestSession.logger.info("load hadoop resources from " +
				this.getHadoopConfDir() + ":");
		super.addResource(new Path(this.getHadoopConfFile("HADOOP_CONF_CORE")));
		super.addResource(new Path(this.getHadoopConfFile("HADOOP_CONF_HDFS")));
		super.addResource(new Path(this.getHadoopConfFile("HADOOP_CONF_MAPRED")));
		super.addResource(new Path(this.getHadoopConfFile("HADOOP_CONF_YARN")));	
	}
	
    /**
     * Get a given Hadoop configuration file path
     */
	public String getHadoopConfFile(String file) {
        return this.getHadoopConfDir() + "/" + file;        
	}
	
	/**
     * Returns the version of the fully distributed Hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * @throws Exception if there is a fatal error getting the version via 
     *                   the CLI for a pseudodistributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() throws Exception {
    	String version = null;
    	
		String strClusterType = TestSession.conf.getProperty("CLUSTER_TYPE");
		
		if (strClusterType.equals(
				"hadooptest.cluster.pseudodistributed.PseudoDistributedCluster")) 
		{
			version = this.getVersionViaCLI();
		}
		else {
			version = VersionInfo.getVersion();
			TestSession.logger.trace("Hadoop version = '" + 
					VersionInfo.getVersion() + "'");
			TestSession.logger.trace("Hadoop build version = '" + 
					VersionInfo.getBuildVersion() + "'");
			TestSession.logger.trace("Hadoop revision = '" + 
					VersionInfo.getRevision() + "'");
		}
		
		return version;
    }

	/**
     * Returns the version of the fully distributed Hadoop cluster being used 
     * via the command line interface.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * @throws Exception if there is a fatal error running the process that gets the
     *         version.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersionViaCLI() 
    		throws Exception {
    	String[] cmd = { this.getHadoopProp("HADOOP_BIN"),
    			"--config", this.getHadoopConfDir(), "version" };	
    	String version = (TestSession.exec.runProcBuilder(cmd))[1].split("\n")[0];
        return version.split(" ")[1];
    }
    
    /**
     * Converts the configuration instance resources and properties to a string.
     * 
     * @param instance the configuration instance.
     * @return the configuration resources and properties.
     */
    public String toString(String instance) {
    	if (instance.equals("resources")) {
    		return "Conf Resources: " + super.toString();
    	} else if (instance.equals("props")) {
        	return "Conf Props: " + super.getProps().toString();	        		
    	}
    	else {
    		return "";
    	}
    }

}
