/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a standalone
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */


package hadooptest.config.testconfig;

import hadooptest.config.TestConfiguration;

public class StandaloneConfiguration extends TestConfiguration
{

   public StandaloneConfiguration()
   {
      super(false);

      initDefaults();
   }

   public StandaloneConfiguration(boolean loadDefaults)
   {
      super(loadDefaults); 

      initDefaults();
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
   private void initDefaults()
   {	   
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
