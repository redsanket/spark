/*
 * YAHOO!
 * 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.config;

import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;

public abstract class TestConfiguration extends Configuration
{
   //private Properties added_properties_;

   public TestConfiguration()
   {   
	  super(false);
   }

   public TestConfiguration(boolean loadDefaults)
   {
	  super(loadDefaults);
   }

   public TestConfiguration(Configuration other)
   {
      super(other);
   }

}
