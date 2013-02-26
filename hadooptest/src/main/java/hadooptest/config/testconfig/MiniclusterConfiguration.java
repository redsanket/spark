/*
 * YAHOO!
 */


package hadooptest.config.testconfig;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/**
 * Represents a configuration for a mini cluster.
 * 
 * This class is currently not finished and should not be used.
 */
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
   
   public void write() {
	   
   }
   
   public void cleanup() {
	   
   }

	public String getHadoopProp(String key) {
		TestSession.logger.error("Currently unimplemented for this cluster configuration");
		
		return null;
	}
   
   /**
    */
   private void initDefaults()
   {
	   //for mapreduceclient MiniCluster
	   this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	   
   }

	protected void initDefaultsClusterSpecific() {}
   
}
