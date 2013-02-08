/*
 * YAHOO!
 */

package hadooptest.config;

import hadooptest.TestSession;

import java.io.File;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;

/* 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class TestConfiguration extends Configuration {

	public static final String HADOOP_CONF_CORE = "core-site.xml";
	public static final String HADOOP_CONF_HDFS = "hdfs-site.xml";
	public static final String HADOOP_CONF_MAPRED = "mapred-site.xml";
	public static final String HADOOP_CONF_YARN = "yarn-site.xml";
	public static final String HADOOP_CONF_CAPACITY_SCHEDULER = "capacity-scheduler.xml";
	public static final String HADOOP_CONF_FAIR_SCHEDULER = "fair-scheduler.xml";

	public static final String NAMENODE = "namenode";
	public static final String RESOURCE_MANAGER = "resourcemanager";
	public static final String DATANODE = "datanode";
	public static final String NODEMANAGER = "nodemanager";
	public static final String GATEWAY = "gateway";

	// General Hadoop configuration properties such as cluster name, 
	// directory paths, etc.	
    protected Properties hadoopProps = new Properties();

    // Track Hadoop override configuration directories
    protected Properties hadoopConfDirPaths = new Properties();

	/* 
	 * Class Constructor.
	 * 
	 * A generic constructor TestConfiguration that calls the Hadoop Configuration
	 * with the false argument, so that you are not loading any default Hadoop
	 * configuration properties.
	 */
	public TestConfiguration() {   
		super(false);		
		this.initDefaults();

	}

	/*
	 * Class Constructor.
	 * 
	 * A constructor that allows you to specify whether or not you would like
	 * the Hadoop Configuration to load default Hadoop config properties.
	 */
	public TestConfiguration(boolean loadDefaults) {
		super(loadDefaults);
		this.initDefaults();

	}

	/*
	 * Class constructor.
	 * 
	 * A constructor that allows you to specify a custom configuration.
	 */
	public TestConfiguration(Configuration other) {
		super(other);
		this.initDefaults();
	}

	/*
	 * Cleans up any test configuration written to disk.
	 */
	public abstract void cleanup();
	
	/*
	 * Writes any test configuration to disk.
	 */
	public abstract void write();
	
    /*
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

	/*
     * Returns the Hadoop configuration directory paths.
     * 
     * @return Properties the Hadoop configuration directory paths by
     * components.
     */
	public Properties getHadoopConfDirPaths() {
    	return this.hadoopConfDirPaths;
    }

    /*
     * Returns the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDirPath(String component) {
		return this.getHadoopConfDirPaths().getProperty(component);
	}
	

    /*
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

	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {
		
		/*
		 * NOTES: some of these properties will need to be initialized
		 * differently for pseudo distributed configuration.
		 */
		
		// Specific to fully distributed cluster configuration.
		hadoopProps.setProperty("CLUSTER_NAME", TestSession.conf.getProperty("CLUSTER_NAME", ""));
		try {
			hadoopProps.setProperty("GATEWAY", InetAddress.getLocalHost().getHostName());
		}
		catch (Exception e) {
			TestSession.logger.warn("Hostname not found!!!");
		}
	
		// String defaultTmpDir = "/grid/0/tmp";
		String defaultTmpDir = "/homes/hadoopqa/tmp/hadooptest";
		hadoopProps.setProperty("TMP_DIR", 
				TestSession.conf.getProperty("TMP_DIR", defaultTmpDir));
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
		df.setTimeZone(TimeZone.getTimeZone("CST"));  
		String tmpDir = this.getHadoopProp("TMP_DIR") + "/hadooptest-" +	
				df.format(new Date());
		new File(tmpDir).mkdirs();
		hadoopProps.setProperty("TMP_DIR", tmpDir);
		
		String HADOOP_ROOT="/home";  // /grid/0								
		hadoopProps.setProperty("JAVA_HOME", HADOOP_ROOT+"/gs/java/jdk");
		hadoopProps.setProperty("HADOOP_INSTALL", HADOOP_ROOT + "/gs/gridre/yroot." +
				hadoopProps.getProperty("CLUSTER_NAME"));

		/* 
		 * Properties beyond this point should be common across pseudo and fully
		 * distributed cluster configuration.
		 */

		// Configuration directory and files
		hadoopProps.setProperty("HADOOP_COMMON_HOME", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/share/hadoop");
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
	}
	
	/*
     * Returns the version of the fully distributed Hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
    	String[] cmd = { this.getHadoopProp("HADOOP_BIN"),
    			"--config", this.getHadoopProp("HADOOP_CONF_DIR"), "version" };	
    	String version = (TestSession.exec.runProcBuilder(cmd))[1].split("\n")[0];
        return version.split(" ")[1];
    }

}
