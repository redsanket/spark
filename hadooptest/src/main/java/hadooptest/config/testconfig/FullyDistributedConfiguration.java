/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/*
 * A class that represents a Hadoop Configuration for a distributed
 * Hadoop cluster under test.
 */
public class FullyDistributedConfiguration extends TestConfiguration
{
	private static TestSession TSM;

    protected Properties conf = new Properties();

	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.
	 * Hadoop default configuration is not used.
	 */
	public FullyDistributedConfiguration(TestSession testSession) {
		super(false);

		TSM = testSession;
		this.initDefaults();
	}

	/*
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * distributed test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 */
	public FullyDistributedConfiguration(boolean loadDefaults) {
		super(loadDefaults); 
		this.initDefaults();
	}

	public Properties getConf() {
    	return conf;
    }

	public String getConf(String key) {
    	if (!conf.getProperty(key).equals(null)) {
    		return conf.getProperty(key);
    	}
    	else {
			TSM.logger.error("Couldn't find value for key '" + key + "'.");
			return "";
    	}
    }

	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {

		String HADOOP_ROOT="/home";  // /grid/0
								
		conf.setProperty("CLUSTER_NAME", TSM.conf.getProperty("CLUSTER_NAME", ""));
		conf.setProperty("JAVA_HOME", HADOOP_ROOT+"/gs/java/jdk");
		conf.setProperty("HADOOP_INSTALL", HADOOP_ROOT + "/gs/gridre/yroot." +
				conf.getProperty("CLUSTER_NAME"));
		conf.setProperty("HADOOP_CONF_DIR", conf.getProperty("HADOOP_INSTALL") +
				"/conf/hadoop");
		conf.setProperty("HADOOP_COMMON_HOME", conf.getProperty("HADOOP_INSTALL") +
				"/share/hadoop");
		
		// Binaries
		conf.setProperty("HADOOP_BIN_DIR", conf.getProperty("HADOOP_COMMON_HOME") + "/bin");
		conf.setProperty("HADOOP_BIN", conf.getProperty("HADOOP_BIN_DIR") + "/hadoop");
		conf.setProperty("HDFS_BIN", conf.getProperty("HADOOP_BIN_DIR") + "/hdfs");
		conf.setProperty("MAPRED_BIN", conf.getProperty("HADOOP_BIN_DIR") + "/mapred");
		conf.setProperty("YARN_BIN", getConf("HADOOP_BIN_DIR") + "/yarn");

		// Version dependent environment variables
		String HADOOP_VERSION = this.getVersion();
		conf.setProperty("HADOOP_VERSION", HADOOP_VERSION);
		
		// Jars
		conf.setProperty("HADOOP_JAR_DIR", getConf("HADOOP_COMMON_HOME") +
				"/share/hadoop");
		conf.setProperty("HADOOP_SLEEP_JAR", getConf("HADOOP_JAR_DIR") + 
				"/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
				HADOOP_VERSION + "-tests.jar"); 
		conf.setProperty("HADOOP_EXAMPLE_JAR", getConf("HADOOP_JAR_DIR") +
				"/mapreduce/" + "hadoop-mapreduce-examples-" +
				HADOOP_VERSION + ".jar"); 
		conf.setProperty("HADOOP_MR_CLIENT_JAR", getConf("HADOOP_JAR_DIR") + 
				"/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
				HADOOP_VERSION + ".jar"); 
		conf.setProperty("HADOOP_STREAMING_JAR", getConf("HADOOP_JAR_DIR") +
				"/tools/lib/" + "hadoop-streaming-" + 
				HADOOP_VERSION + ".jar"); 
		
		// Configuration
		this.parseHadoopConf();
	}
	    

	/*
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public void parseHadoopConf() {
		String confDir = this.getConf("HADOOP_CONF_DIR");

		
		

	}
		
	/*
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public void write() throws IOException {
		String confDir = this.getConf("HADOOP_CONF_DIR");
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

		if (core_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(core_site);
			this.writeXml(out);
		}
		else {
			TSM.logger.warn("Couldn't create the xml configuration output file.");
		}

		if (hdfs_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(hdfs_site);
			this.writeXml(out);
		}
		else {
			TSM.logger.warn("Couldn't create the xml configuration output file.");
		}

		if (yarn_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(yarn_site);
			this.writeXml(out);
		}
		else {
			TSM.logger.warn("Couldn't create the xml configuration output file.");
		}

		if (mapred_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(mapred_site);
			this.writeXml(out);
		}
		else {
			TSM.logger.warn("Couldn't create the xml configuration output file.");
		}

		FileWriter slaves_file = new FileWriter(confDir + "slaves");
		BufferedWriter slaves = new BufferedWriter(slaves_file);
		slaves.write("localhost");
		slaves.close();
	}

	/*
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 */
	public void cleanup() {
		String confDir = this.getConf("HADOOP_CONF_DIR");
		File core_site = new File(confDir + "core-site.xml");
		File hdfs_site = new File(confDir + "hdfs-site.xml");
		File yarn_site = new File(confDir + "yarn-site.xml");
		File mapred_site = new File(confDir + "mapred-site.xml");	
		File slaves = new File(confDir + "slaves");	
		File log4jProperties = new File(confDir + "log4j.properties");

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
	}

    /*
     * Returns the version of the fully distributed hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
    	// Call hadoop version to fetch the version 	
    	String[] cmd = { this.getConf("HADOOP_BIN"),
    			"--config", this.getConf("HADOOP_CONF_DIR"), "version" };	
    	String version = (TSM.hadoop.runProcBuilder(cmd)).split("\n")[0];
        return version.split(" ")[1];
    }
	
}
