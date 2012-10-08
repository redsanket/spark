/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */


package hadooptest.config.testconfig;

import hadooptest.config.TestConfiguration;

public class PseudodistributedConfiguration extends TestConfiguration
{

   public PseudodistributedConfiguration()
   {
      super(new StandaloneConfiguration());

      initDefaults();
   }

   public PseudodistributedConfiguration(boolean loadDefaults)
   {
      super(new StandaloneConfiguration()); 

      initDefaults();
   }

   /**
    */
   private void initDefaults()
   {
      //add("fs.default.name", "hdfs://localhost/");
      //add("dfs.replication", "1");
      //add("mapred.job.tracker", "localhost:8021");
      //add("yarn.resourcemanager.address", "localhost:8032");
      //add("yarn.nodemanager.aux-services", "mapreduce.shuffle");
   }

}
