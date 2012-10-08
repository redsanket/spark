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
      //added_properties_ = new Properties();
   }

   public TestConfiguration(boolean loadDefaults)
   {
	  super(false);
      //added_properties_ = new Properties();
   }

   public TestConfiguration(Configuration other)
   {
      super(other);
      //added_properties_ = new Properties();
   }

   /*
   public Properties getPropsAddedToDefaults()
   {
      return added_properties_;
   }

   public void add(String name, String value)
   {
      added_properties_.setProperty(name, value);
      super.set(name, value);
   }

   public void addBoolean(String name, boolean value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setBoolean(name, value);
   }

   public void addBooleanIfUnset(String name, boolean value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setBooleanIfUnset(name, value);
   }
 
   public <T extends Enum<T>> void addEnum(String name, T value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setEnum(name, value);
   }

   public void addFloat(String name, float value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setFloat(name, value);
   }

   public void addIfUnset(String name, String value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setIfUnset(name, value);
   }

   public void addInt(String name, int value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setInt(name, value);
   }

   public void addLong(String name, long value)
   {
      added_properties_.setProperty(name, String.valueOf(value));
      super.setLong(name, value);
   }

   public void addPattern(String name, Pattern pattern)
   {
      added_properties_.setProperty(name, String.valueOf(pattern));
      super.setPattern(name, pattern);
   }

   public void addStrings(String name, String... values)
   {
      added_properties_.setProperty(name, String.valueOf(values));
      super.setStrings(name, values);
   }
   */
}
