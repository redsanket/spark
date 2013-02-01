/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/*
 * A class that represents a Hadoop Configuration for a standalone
 * Hadoop cluster under test.
 */
public class StandaloneConfiguration extends TestConfiguration {
	private String CONFIG_BASE_DIR;

	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a standalone cluster under test.  Hadoop
	 * default configuration is not used.
	 */
	public StandaloneConfiguration() {
		super(false);

		initDefaults();
	}

	/*
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * standalone test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 */
	public StandaloneConfiguration(boolean loadDefaults) {
		super(loadDefaults); 

		CONFIG_BASE_DIR = TestSession.conf.getProperty("CONFIG_BASE_DIR", "");

		initDefaults();
	}

	/*
	 * Writes the standalone cluster configuration specified by the object out
	 * to disk.
	 */
	public void write() {
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

		try{
			if (core_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(core_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (hdfs_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(hdfs_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (yarn_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(yarn_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (mapred_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(mapred_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			FileWriter slaves_file = new FileWriter(CONFIG_BASE_DIR + "slaves");
			BufferedWriter slaves = new BufferedWriter(slaves_file);
			slaves.write("localhost");
			slaves.close();
		}
		catch (IOException ioe) {
			TestSession.logger.error("There was a problem writing the hadoop configuration to disk.");
		}
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

	public String getHadoopProp(String key) {
		TestSession.logger.error("Currently unimplemented for this cluster configuration");
		
		return null;
	}
	
	/**
	 * Method adds the configuration default properties that ship with a hadoop release.
	 * This should provide standalone mode by default without specifying any configuration
	 * from command-line hadoop, or by relying on the state of configuration files in the
	 * default locations.  It can also provide a basis for the other configuration types,
	 * and a retrieveable list of documented default properties.
	 *
	 * addResource() expects the xml files to be in the CLASSPATH which is currently "."
	 * for this class.  However, these files should eventually be in a resources directory
	 * for this package.  The default xml files will need to be maintained an updated by 
	 * pulling in new versions with new releases, but this is more desireable than maintaining
	 * the entire list of individual default properties and values (and adding them individually
	 * as key-value pairs).
	 *
	 * The default locations these files are pulled from is:
	 *
	 *    hadoop-tools/hadoop-distcp/src/main/resources
	 *    hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/resources
	 *    hadoop-common-project/hadoop-common/src/main/resources
	 *    hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/resources
	 *    hadoop-mapreduce-project/src/java
	 *    hadoop-hdfs-project/hadoop-hdfs/src/main/resources
	 *    hadoop-hdfs-project/hadoop-hdfs-httpfs/src/main/resources
	 */
	private void initDefaults() {	   
		this.addResource(this.getClassLoader().getResourceAsStream("core-default.xml"));
		this.addResource(this.getClassLoader().getResourceAsStream("hdfs-default.xml"));
		this.addResource(this.getClassLoader().getResourceAsStream("yarn-default.xml"));
		//this.addResource(this.getClassLoader().getResourceAsStream("distcp-default.xml"));
		//this.addResource(this.getClassLoader().getResourceAsStream("httpfs-default.xml"));
		this.addResource(this.getClassLoader().getResourceAsStream("mapred-default.xml"));
		//this.addResource(this.getClassLoader().getResourceAsStream("testserver-default.xml"));

		// This seems to be necessary to get the added resources to be available.  get seems to be doing
		// something that addResource() doesn't when you have created a Configuration(false).  This might 
		// be a bug, follow up on it later.
		System.out.println(this.get("hadoop.common.configuration.version"));

	}

}
