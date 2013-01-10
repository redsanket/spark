/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a fully
 * distributed Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.config.testconfig;

import hadooptest.config.TestConfiguration;

public class FullyDistributedConfiguration extends TestConfiguration
{

   public FullyDistributedConfiguration()
   {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public FullyDistributedConfiguration(boolean loadDefaults)
   {
      super(new StandaloneConfiguration()); 

      initDefaults();
   }

   /**
    */
   private void initDefaults()
   {
   }

}
