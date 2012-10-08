/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a minicluster
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */


package hadooptest.config.testconfig;

import hadooptest.config.TestConfiguration;

public class MiniclusterConfiguration extends TestConfiguration
{

   public MiniclusterConfiguration()
   {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public MiniclusterConfiguration(boolean loadDefaults)
   {
      super(new StandaloneConfiguration()); 

      initDefaults();
   }

   /**
    */
   private void initDefaults()
   {
	   //for mapreduceclient MiniCluster
	   this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	   
   }

}
