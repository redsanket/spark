/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a fully
 * distributed Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.config.testconfig;

import java.io.File;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

public class FullyDistributedConfiguration2 extends TestConfiguration
{
	private String CONFIG_BASE_DIR;
	
	private String HADOOP_ROOT_DIR; 
	private String CLUSTER_NAME;	
	
	private static TestSession TSM;

   public FullyDistributedConfiguration2()
   {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public FullyDistributedConfiguration2(boolean loadDefaults)
   {
      super(new StandaloneConfiguration()); 

      initDefaults();
   }

	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a fullydistributed cluster under test.  Hadoop
	 * default configuration is not used.
	 */
	public FullyDistributedConfiguration2(TestSession testSession) {
		super(false);

		TSM = testSession;
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
		CLUSTER_NAME = TSM.conf.getProperty("CLUSTER_NAME", "");
		this.initDefaults();
	}

   /**
    */
   private void initDefaults()
   {
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

		/* 
		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
		*/
	}

   
}
