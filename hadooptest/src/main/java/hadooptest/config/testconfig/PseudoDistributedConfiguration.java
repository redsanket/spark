/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import hadooptest.config.TestConfiguration;

public class PseudoDistributedConfiguration extends TestConfiguration
{

	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";

	public PseudoDistributedConfiguration()
	{
		super(false);

		initDefaults();
	}

	public PseudoDistributedConfiguration(boolean loadDefaults)
	{
		super(loadDefaults); 

		initDefaults();
	}

	public void write() throws IOException {
		File outdir = new File(CONFIG_BASE_DIR);
		outdir.mkdirs();

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

	/**
	 */
	private void initDefaults()
	{
		set("fs.default.name", "hdfs://localhost/");
		set("dfs.replication", "1");
		set("mapreduce.framework.name", "yarn");
		set("yarn.resourcemanager.address", "localhost:8032");
		set("yarn.nodemanager.aux-services", "mapreduce.shuffle");
	}

}
