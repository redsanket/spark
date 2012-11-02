/*
 * YAHOO!
 * 
 * A class that represents a Hadoop Configuration for a pseudodistributed
 * Hadoop cluster under test.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import hadooptest.config.TestConfiguration;

public class PseudodistributedConfiguration extends TestConfiguration
{

   public PseudodistributedConfiguration()
   {
      super(false);

      initDefaults();
   }

   public PseudodistributedConfiguration(boolean loadDefaults)
   {
      super(loadDefaults); 

      initDefaults();
   }

   public void write() throws IOException {
	   File outdir = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test");
	   outdir.mkdirs();

	   File core_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/core-site.xml");
	   File hdfs_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/hdfs-site.xml");
	   File yarn_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/yarn-site.xml");
	   File mapred_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/mapred-site.xml");		

	   if (core_site.createNewFile()) {
		   FileOutputStream out = new FileOutputStream(core_site);
		   this.writeXml(out);
	   }
	   else {
		   System.out.println("Couldn't create the xml configuration output file.");
	   }

	   if (hdfs_site.createNewFile()) {
		   FileOutputStream out = new FileOutputStream(hdfs_site);
		   this.writeXml(out);
	   }
	   else {
		   System.out.println("Couldn't create the xml configuration output file.");
	   }

	   if (yarn_site.createNewFile()) {
		   FileOutputStream out = new FileOutputStream(yarn_site);
		   this.writeXml(out);
	   }
	   else {
		   System.out.println("Couldn't create the xml configuration output file.");
	   }

	   if (mapred_site.createNewFile()) {
		   FileOutputStream out = new FileOutputStream(mapred_site);
		   this.writeXml(out);
	   }
	   else {
		   System.out.println("Couldn't create the xml configuration output file.");
	   }

	   FileWriter slaves_file = new FileWriter("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/slaves");
	   BufferedWriter slaves = new BufferedWriter(slaves_file);
	   slaves.write("localhost");
	   slaves.close();
   }

   public void cleanup() {
		File core_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/core-site.xml");
		File hdfs_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/hdfs-site.xml");
		File yarn_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/yarn-site.xml");
		File mapred_site = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/mapred-site.xml");	
		File slaves = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/slaves");	
		
		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
   }
   
   /**
    */
   private void initDefaults()
   {
	   set("fs.default.name", "hdfs://localhost/");
	   set("dfs.replication", "1");
	   set("mapreduce.framework.name", "yarn");
	   set("yarn.resourcemanager.address", "localhost:8032");
	   set("yarn.nodemanager.aux-services", "mapreduce.shuffle");
   }

}
