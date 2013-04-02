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
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/**
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 */
public class PseudoDistributedConfiguration extends TestConfiguration
{

	/**
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a pseudodistributed cluster under test.  Hadoop
	 * default configuration is not used.
	 * 
	 * @throws UnknownHostException if there is a fatal error initializing the default configuration.
	 */
	public PseudoDistributedConfiguration() throws UnknownHostException {
		super(false);
		this.initDefaults();
	}

	/**
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * pseudodistributed test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 * 
	 * @throws UnknownHostException if there is a fatal error initializing the default configuration.
	 */
	public PseudoDistributedConfiguration(boolean loadDefaults) throws UnknownHostException {
		super(loadDefaults); 
		this.initDefaults();
	}

	/**
	 * Initializes cluster-specific properties defaults.
	 */
	protected void initDefaultsClusterSpecific() {
		String defaultTmpDir = "/Users/" + System.getProperty("user.name") + "/hadooptest/tmp";
		hadoopProps.setProperty("TMP_DIR", 
				TestSession.conf.getProperty("TMP_DIR", defaultTmpDir));
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
		df.setTimeZone(TimeZone.getTimeZone("CST"));  
		String tmpDir = this.getHadoopProp("TMP_DIR") + "/hadooptest-" +	
				df.format(new Date());
		new File(tmpDir).mkdirs();
		hadoopProps.setProperty("TMP_DIR", tmpDir);
		hadoopProps.setProperty("HADOOP_INSTALL", TestSession.conf.getProperty("HADOOP_INSTALL", ""));
		
		hadoopProps.setProperty("HADOOP_COMMON_HOME", hadoopProps.getProperty("HADOOP_INSTALL"));
	}
	
	/**
	 * Writes the pseudodistributed cluster configuration specified by the object out
	 * to disk.
	 * 
	 * @throws IOException if there is a problem writing configuration files to disk.
	 */
	public void write() throws IOException {
		String configurationDir = TestSession.conf.getProperty("HADOOP_INSTALL", "") + "/conf/hadoop/";
		
		File outdir = new File(configurationDir);
		outdir.mkdirs();
		
		File historytmp = new File(configurationDir + "jobhistory/tmp");
		historytmp.mkdirs();
		File historydone = new File(configurationDir + "jobhistory/done");
		historydone.mkdirs();

		File core_site = new File(configurationDir + "core-site.xml");
		File hdfs_site = new File(configurationDir + "hdfs-site.xml");
		File yarn_site = new File(configurationDir + "yarn-site.xml");
		File mapred_site = new File(configurationDir + "mapred-site.xml");		

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

		FileWriter slaves_file = new FileWriter(configurationDir + "slaves");
		BufferedWriter slaves = new BufferedWriter(slaves_file);
		slaves.write("localhost");
		slaves.close();
	}

	/**
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 */
	public void cleanup() {
		String configurationDir = TestSession.cluster.getConf().getHadoopConfDirPath();
		
		File core_site = new File(configurationDir + "/core-site.xml");
		File hdfs_site = new File(configurationDir + "/hdfs-site.xml");
		File yarn_site = new File(configurationDir + "/yarn-site.xml");
		File mapred_site = new File(configurationDir + "/mapred-site.xml");	
		File slaves = new File(configurationDir + "/slaves");	
		File log4jProperties = new File(configurationDir + "/log4j.properties");

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
	}
	
	/**
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a pseudodistributed
	 * cluster under test.
	 */
	private void initDefaults() {
		String configurationDir = TestSession.conf.getProperty("HADOOP_INSTALL", "") + "/conf/hadoop/";
		
		set("fs.default.name", "hdfs://localhost/");
		set("dfs.replication", "1");
		set("mapreduce.framework.name", "yarn");
		set("yarn.resourcemanager.address", "localhost:8032");
		set("yarn.nodemanager.aux-services", "mapreduce.shuffle");
		set("mapreduce.jobhistory.intermediate-done-dir", configurationDir + "jobhistory/tmp");
		set("mapreduce.jobhistory.done-dir", configurationDir + "jobhistory/done");
		set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
	}

}
