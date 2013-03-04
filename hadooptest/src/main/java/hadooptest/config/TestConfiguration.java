/*
 * YAHOO!
 */

package hadooptest.config;

import hadooptest.TestSession;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;

/** 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class TestConfiguration extends Configuration {

	/** Filename of the core hadoop configuration xml file. */
	public static final String HADOOP_CONF_CORE = "core-site.xml";
	
	/** Filename of the hdfs configuration xml file. */
	public static final String HADOOP_CONF_HDFS = "hdfs-site.xml";
	
	/** Filename of the mapreduce configuration xml file. */
	public static final String HADOOP_CONF_MAPRED = "mapred-site.xml";

	/** Filename of the yarn configuration xml file. */
	public static final String HADOOP_CONF_YARN = "yarn-site.xml";

	/** Filename of the capacity scheduler configuration xml file. */
	public static final String HADOOP_CONF_CAPACITY_SCHEDULER = "capacity-scheduler.xml";

	/** Filename of the fair scheduler configuration xml file. */
	public static final String HADOOP_CONF_FAIR_SCHEDULER = "fair-scheduler.xml";

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

	/** General Hadoop configuration properties such as cluster name, 
	 * directory paths, etc.
	 */
    protected Properties hadoopProps = new Properties();

    /** Track Hadoop override configuration directories */
    protected Properties hadoopConfDirPaths = new Properties();

	/** 
	 * A generic constructor TestConfiguration that calls the Hadoop Configuration
	 * with the false argument, so that you are not loading any default Hadoop
	 * configuration properties.  It then proceeds to initialize the default
	 * configuration for the reflected cluster type.
	 */
	public TestConfiguration() {
		super(true);		
		this.initDefaults();

	}

	/**
	 * A constructor that allows you to specify whether or not you would like
	 * the Hadoop Configuration to load default Hadoop config properties.
	 * It then proceeds to initialize the default configuration for the 
	 * reflected cluster type.
	 * 
	 * @param loadDefaults whether or not to load the cluster configuration defaults
	 * 						using the Configuration superclass constructor.
	 */
	public TestConfiguration(boolean loadDefaults) {
		super(loadDefaults);
		this.initDefaults();

	}

	/**
	 * A constructor that allows you to specify a custom configuration.
	 * 
	 * @param other a custom Configuration.
	 */
	public TestConfiguration(Configuration other) {
		super(other);
		this.initDefaults();
	}

	/**
	 * Cleans up any test configuration written to disk.
	 */
	public abstract void cleanup();
	
	/**
	 * Writes any test configuration to disk.
	 */
	public abstract void write();
	
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
			TestSession.logger.error("Couldn't find value for key '" + key + "'.");
			return "";
    	}
    }

	/**
     * Returns the Hadoop configuration directory paths.
     * 
     * @return Properties the Hadoop configuration directory paths by
     * components.
     */
	public Properties getHadoopConfDirPaths() {
    	return this.hadoopConfDirPaths;
    }

    /**
     * Returns the Hadoop configuration directory path for the default gateway
     * component.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDirPath() {
		return this.getHadoopConfDirPath(null);
	}
	

    /**
     * Returns the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDirPath(String component) {
		if ((component == null) || component.isEmpty()) {
			component = TestConfiguration.GATEWAY;
		}
		return this.getHadoopConfDirPaths().getProperty(component);
	}
	
    /**
     * Set the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * @param path String of the directory path name.
     * 
     */
	public void setHadoopConfDirPath(String component, String path) {
		this.getHadoopConfDirPaths().setProperty(component, path);
	}

	protected abstract void initDefaultsClusterSpecific();
	
	/**
	 * Setup the Kerberos configuration for the given user name and keytab file
	 * in the parent class Apache Hadoop Configuration object. This will be
	 * needed later for tasks such as job submission. 
	 */
	private void setKerberosConf(String user) {
		super.set("user-" + user, user + "@DEV.YGRID.YAHOO.COM");
		super.set("keytab-" + user, "/homes/" + user + "/" + user + ".dev.headless.keytab");
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
	 */
	private void initDefaults() {

		this.setKerberosConf();
		this.initDefaultsClusterSpecific();
		
		/* 
		 * Properties beyond this point should be common across pseudo and fully
		 * distributed cluster configuration.
		 */

		/*
		 * core-default.xml contains the two required properties listed below
		 * that must be defined in the Hadoop Configuration in order for the
		 * test framework to interact with the Hadoop Classes and APIs. Since
		 * we are loading core-default.xml as a whole, we will not need to set
		 * these properties individually.
		 * 
		 * this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		 * this.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		 * 
		 * NOTE:
		 * Consider whether to also load the other default xml files:
		 * this.addResource(this.getClassLoader().getResourceAsStream("core-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("hdfs-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("mapred-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("yarn-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("distcp-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("httpfs-default.xml"));
		 * this.addResource(this.getClassLoader().getResourceAsStream("testserver-default.xml"));
		 */
		super.addResource(this.getClassLoader().getResourceAsStream("core-default.xml"));

		
		// Configuration directory and files
		hadoopProps.setProperty("HADOOP_CONF_DIR", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/conf/hadoop");
		hadoopProps.setProperty("HADOOP_DEFAULT_CONF_DIR",
				hadoopProps.getProperty("HADOOP_CONF_DIR"));
		this.setHadoopConfDirPath("gateway",
				hadoopProps.getProperty("HADOOP_CONF_DIR"));
		hadoopProps.setProperty("HADOOP_CONF_CORE",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_CORE);
		hadoopProps.setProperty("HADOOP_CONF_HDFS",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_HDFS);
		hadoopProps.setProperty("HADOOP_CONF_MAPRED",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_MAPRED);
		hadoopProps.setProperty("HADOOP_CONF_YARN",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_YARN);
		hadoopProps.setProperty("HADOOP_CONF_CAPACITY_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_CAPACITY_SCHEDULER);
		hadoopProps.setProperty("HADOOP_CONF_FAIR_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_FAIR_SCHEDULER);

		// Binaries
		hadoopProps.setProperty("HADOOP_BIN_DIR", hadoopProps.getProperty("HADOOP_COMMON_HOME") + "/bin");
		hadoopProps.setProperty("HADOOP_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hadoop");
		hadoopProps.setProperty("HDFS_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hdfs");
		hadoopProps.setProperty("MAPRED_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/mapred");
		hadoopProps.setProperty("YARN_BIN", getHadoopProp("HADOOP_BIN_DIR") + "/yarn");

		// Version dependent environment variables
		String HADOOP_VERSION = this.getVersion();

		// String HADOOP_VERSION = "23.6";
		hadoopProps.setProperty("HADOOP_VERSION", HADOOP_VERSION);
		
		// Jars
		hadoopProps.setProperty("HADOOP_JAR_DIR", getHadoopProp("HADOOP_COMMON_HOME") +
				"/share/hadoop");
		hadoopProps.setProperty("HADOOP_SLEEP_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
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

		super.addResource(new Path(hadoopProps.getProperty("HADOOP_CONF_CORE")));
		super.addResource(new Path(hadoopProps.getProperty("HADOOP_CONF_HDFS")));
	}
	
	/**
     * Returns the version of the fully distributed Hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
		VersionInfo versionInfo = new VersionInfo();
		String version = versionInfo.getVersion();
		TestSession.logger.trace("Hadoop version = '" + versionInfo.getVersion() + "'");
		TestSession.logger.trace("Hadoop build version = '" + versionInfo.getBuildVersion() + "'");
		TestSession.logger.trace("Hadoop revision = '" + versionInfo.getRevision() + "'");
		return version;
    }

	/**
     * Returns the version of the fully distributed Hadoop cluster being used 
     * via the command line interface.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersionViaCLI() {
    	String[] cmd = { this.getHadoopProp("HADOOP_BIN"),
    			"--config", this.getHadoopProp("HADOOP_CONF_DIR"), "version" };	
    	String version = (TestSession.exec.runProcBuilder(cmd))[1].split("\n")[0];
        return version.split(" ")[1];
    }
    
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
