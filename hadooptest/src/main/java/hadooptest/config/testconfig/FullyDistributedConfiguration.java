/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/*
 * A class that represents a Hadoop Configuration for a distributed
 * Hadoop cluster under test.
 */
public class FullyDistributedConfiguration extends TestConfiguration
{
	private static TestSession TSM;

    protected Hashtable<String, String> conf = new Hashtable<String, String>();

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


	public Hashtable<String, String> getConf() {
    	return conf;
    }

    public String getConf(String key) {
    	return conf.get(key).toString();
    }

	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {
		String CLUSTER_NAME = TSM.conf.getProperty("CLUSTER_NAME", "");
		String HADOOP_ROOT="/home";  // /grid/0
		String JAVA_HOME=HADOOP_ROOT+"/gs/java/jdk";
		String HADOOP_INSTALL=HADOOP_ROOT+"/gs/gridre/yroot."+CLUSTER_NAME;
		String HADOOP_CONF_DIR=HADOOP_INSTALL+"/conf/hadoop";
		String HADOOP_COMMON_HOME=HADOOP_INSTALL+"/share/hadoop";
		String HADOOP_JAR_DIR=HADOOP_COMMON_HOME+"/share/hadoop/mapreduce";
		String HADOOP_BIN_DIR=HADOOP_COMMON_HOME+"/bin";
		String HADOOP_BIN=HADOOP_BIN_DIR+"/hadoop";
		String HDFS_BIN=HADOOP_BIN_DIR+"/hdfs";
		String MAPRED_BIN=HADOOP_BIN_DIR+"/mapred";
			
		conf.put("CLUSTER_NAME", CLUSTER_NAME);
		conf.put("JAVA_HOME", JAVA_HOME);
		conf.put("HADOOP_INSTALL", HADOOP_INSTALL);
		conf.put("HADOOP_CONF_DIR", HADOOP_CONF_DIR);
		conf.put("HADOOP_COMMON_HOME", HADOOP_COMMON_HOME);
		conf.put("HADOOP_BIN", HADOOP_BIN);
		conf.put("HDFS_BIN", HDFS_BIN);
		conf.put("MAPRED_BIN", MAPRED_BIN);
		conf.put("HADOOP_JAR_DIR", HADOOP_JAR_DIR);

		// Version dependent environment variables
		String HADOOP_VERSION = this.getVersion();
		conf.put("HADOOP_VERSION", HADOOP_VERSION);
		conf.put("HADOOP_SLEEP_JAR", HADOOP_JAR_DIR + "/" +
				"hadoop-mapreduce-client-jobclient-" + HADOOP_VERSION +
				"-tests.jar"); 
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
