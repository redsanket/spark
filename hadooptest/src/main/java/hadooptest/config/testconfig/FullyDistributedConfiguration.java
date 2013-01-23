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

	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	
	private static TestSession TSM;

    protected Hashtable<String, String> paths = new Hashtable<String, String>();

	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.  Hadoop
	 * default configuration is not used.
	 */
	public FullyDistributedConfiguration(TestSession testSession) {
		super(false);

		TSM = testSession;
		
		HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
		
		this.initClusterPaths();

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


	public Hashtable<String, String> getPaths() {
    	return paths;
    }

    public String getPaths(String key) {
    	return paths.get(key).toString();
    }

    /*
	 * Initialize the cluster paths.
	 */
	private void initClusterPaths() {		
		String jar = HADOOP_INSTALL + "/share/hadoop/share/hadoop/mapreduce";
		String version = this.getVersion();
		
		paths.put("hadoop", HADOOP_INSTALL+"/share/hadoop/bin/hadoop");
		paths.put("mapred", HADOOP_INSTALL+"/share/hadoop/bin/mapred");
		paths.put("jar", jar);
		paths.put("sleepJar", jar + "/" +
				"hadoop-mapreduce-client-jobclient-" + version + "-tests.jar"); 
	}
	    

	/*
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public void write() throws IOException {
		File outdir = new File(CONFIG_BASE_DIR);
		outdir.mkdirs();
		
		File historytmp = new File(CONFIG_BASE_DIR + "jobhistory/tmp");
		historytmp.mkdirs();
		File historydone = new File(CONFIG_BASE_DIR + "jobhistory/done");
		historydone.mkdirs();

		File core_site = new File(CONFIG_BASE_DIR + "core-site.xml");
		File hdfs_site = new File(CONFIG_BASE_DIR + "hdfs-site.xml");
		File yarn_site = new File(CONFIG_BASE_DIR + "yarn-site.xml");
		File mapred_site = new File(CONFIG_BASE_DIR + "mapred-site.xml");		

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

		FileWriter slaves_file = new FileWriter(CONFIG_BASE_DIR + "slaves");
		BufferedWriter slaves = new BufferedWriter(slaves_file);
		slaves.write("localhost");
		slaves.close();
	}

	/*
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 */
	public void cleanup() {
		File core_site = new File(CONFIG_BASE_DIR + "core-site.xml");
		File hdfs_site = new File(CONFIG_BASE_DIR + "hdfs-site.xml");
		File yarn_site = new File(CONFIG_BASE_DIR + "yarn-site.xml");
		File mapred_site = new File(CONFIG_BASE_DIR + "mapred-site.xml");	
		File slaves = new File(CONFIG_BASE_DIR + "slaves");	
		File log4jProperties = new File(CONFIG_BASE_DIR + "log4j.properties");

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
	}

	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {

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
    	String[] cmd = { HADOOP_INSTALL+"/share/hadoop/bin/hadoop",
    			"--config", CONFIG_BASE_DIR, "version" };	
    	String version = (TSM.hadoop.runProcBuilder(cmd)).split("\n")[0];
        return version.split(" ")[1];
    }
	
}
