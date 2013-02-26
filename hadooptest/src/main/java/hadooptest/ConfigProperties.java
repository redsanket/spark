/*
 * YAHOO!
 */

package hadooptest;

import java.io.File;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * A properties class that allows for specifying key-value pairs of
 * properties to the framework.  In effect, it represents the framework
 * configuration file.
 * 
 * By default, the framework makes the assumption that this configuration file
 * will reside in your user home directory, with the filename "hadooptest.conf".
 * 
 * To specify a different location and filename for the framework configuration
 * file, set the system property "hadooptest.config" to be the filepath and file
 * name of the configuration file you wish to use.  An example by calling
 * the framework from the command line follows:
 * 
 * java -cp $CLASSPATH -Dhadooptest.config=/homes/rbernota/framework_config/framework.conf org.junit.runner.JUnitCore hadooptest.regression.yarn.MapredKillTask
 */
public class ConfigProperties extends Properties
{

   /** 
    * A Vector to contain all of the configuration properties files that might be
    *
    * submitted to a ConfigProperties instance.
    */
   private Vector properties_files_ = new Vector(1);

   /**
    * This no argument constructor is called from the TestSession.  It simply
    * calls the superclass constructor for java.util.Properties.
    */
   public ConfigProperties()
   {
      super();
   }

   /**
    * Provides an additional load method to Properties that allows for specifying a file.
    * Use this method to load additional configuration properties, file by file.
    * In the future, this will also allow us to add features to the configuration
    * properties files that are not already supported by java.util.Properties.
    *
    * @param file The configuration properties file to load.
    * @exception IOException If there was an input stream error or syntax error in the file.
    */ 
   public void load(File file) throws IOException
   {
      properties_files_.add(file);   
      StringBuffer file_contents = detectFileImports_(new BufferedReader(new FileReader(file)));
      super.load(new ByteArrayInputStream(file_contents.toString().getBytes()));
   }

   /**
    * Retrieves a list of all config files submitted to an instance of the class.
    *
    * @return A list of all config files loaded to an instance of the class.
    */
   public String[] getAllConfigFiles()
   {
      String[] all_files = new String[properties_files_.size()];
      properties_files_.copyInto(all_files);
      return(all_files); 
   }

   /**
    * Recursively detects configuration file imports and adds their contents to the properties.
    * 
    * @param file The contents of the configuration file.
    * @return The contents of any imported configuration files recursively added to the parent
    *         configuration file contents.
    */
   private StringBuffer detectFileImports_(BufferedReader file)
   {
      String line, next_token;
      StringBuffer output = new StringBuffer();

      try {
    	  while ((line = file.readLine()) != null)
    	  {
    		  StringTokenizer tokens = new StringTokenizer(line);

    		  if(tokens.countTokens() == 0)
    		  {
    			  output.append(line).append("\n");
    			  continue;
    		  }

    		  next_token = tokens.nextToken();

    		  if(!next_token.equals("import"))
    		  {
    			  output.append(line).append("\n");
    			  continue;
    		  }

    		  next_token = tokens.nextToken();
    		  properties_files_.addElement(next_token);
    		  output.append(detectFileImports_(new BufferedReader(new FileReader(next_token))));
    	  }
      }
      catch (IOException ioe) {
    	  // catch if you can't open the file
      }

      return(output);
   }
}
