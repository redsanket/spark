/*
 * YAHOO!
 */


package hadooptest.config.hadoop.minicluster;

import java.net.UnknownHostException;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.standalone.StandaloneConfiguration;

/**
 * Represents a configuration for a mini cluster.
 * 
 * This class is currently not finished and should not be used.
 */
public class MiniclusterConfiguration extends HadoopConfiguration
{

   public MiniclusterConfiguration() throws Exception {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public MiniclusterConfiguration(boolean loadDefaults) throws Exception {
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
