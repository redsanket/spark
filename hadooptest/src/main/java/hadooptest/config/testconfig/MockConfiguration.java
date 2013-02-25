/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a mock
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */


package hadooptest.config.testconfig;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/**
 * Represents a configuration for a mock cluster.
 * 
 * This class is currently not finished and should not be used.
 */
public class MockConfiguration extends TestConfiguration
{

   public MockConfiguration()
   {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public MockConfiguration(boolean loadDefaults)
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
   }

	protected void initDefaultsClusterSpecific() {}
   
}
