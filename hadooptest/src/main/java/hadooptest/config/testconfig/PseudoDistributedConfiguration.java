/*
 * YAHOO!
 * 
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import hadooptest.config.TestConfiguration;

/*
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 */
public class PseudoDistributedConfiguration extends TestConfiguration
{

	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";

	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a pseudodistributed cluster under test.  Hadoop
	 * default configuration is not used.
	 */
	public PseudoDistributedConfiguration()
	{
		super(false);

		initDefaults();
	}

	/*
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * pseudodistributed test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 */
	public PseudoDistributedConfiguration(boolean loadDefaults)
	{
		super(loadDefaults); 

		initDefaults();
	}

	/*
	 * Writes the pseudodistributed cluster configuration specified by the object out
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
			System.out.println("Couldn't create the xml configuration output file.");
		}

		if (hdfs_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(hdfs_site);
			this.writeXml(out);
		}
		else {
			System.out.println("Couldn't create the xml configuration output file.");
		}

		if (yarn_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(yarn_site);
			this.writeXml(out);
		}
		else {
			System.out.println("Couldn't create the xml configuration output file.");
		}

		if (mapred_site.createNewFile()) {
			FileOutputStream out = new FileOutputStream(mapred_site);
			this.writeXml(out);
		}
		else {
			System.out.println("Couldn't create the xml configuration output file.");
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

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
	}

	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a pseudodistributed
	 * cluster under test.
	 */
	private void initDefaults()
	{
		set("fs.default.name", "hdfs://localhost/");
		set("dfs.replication", "1");
		set("mapreduce.framework.name", "yarn");
		set("yarn.resourcemanager.address", "localhost:8032");
		set("yarn.nodemanager.aux-services", "mapreduce.shuffle");
		set("mapreduce.jobhistory.intermediate-done-dir", CONFIG_BASE_DIR + "jobhistory/tmp");
		set("mapreduce.jobhistory.done-dir", CONFIG_BASE_DIR + "jobhistory/done");
	}

}
