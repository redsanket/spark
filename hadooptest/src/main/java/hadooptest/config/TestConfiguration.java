/*
 * YAHOO!
 */

package hadooptest.config;

import hadooptest.TestSession;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/* 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class TestConfiguration extends Configuration {

	public static final String HADOOP_CONF_CORE = "core-site.xml";
	public static final String HADOOP_CONF_HDFS = "hdfs-site.xml";
	public static final String HADOOP_CONF_MAPRED = "mapred-site.xml";
	public static final String HADOOP_CONF_YARN = "yarn-site.xml";
	public static final String HADOOP_CONF_CAPACITY_SCHEDULER = "capacity-scheduler.xml";
	public static final String HADOOP_CONF_FAIR_SCHEDULER = "fair-scheduler.xml";

	public static final String NAMENODE = "namenode";
	public static final String RESOURCE_MANAGER = "resourcemanager";
	public static final String DATANODE = "datanode";
	public static final String NODEMANAGER = "nodemanager";
	public static final String GATEWAY = "gateway";

	/* 
	 * Class Constructor.
	 * 
	 * A generic constructor TestConfiguration that calls the Hadoop Configuration
	 * with the false argument, so that you are not loading any default Hadoop
	 * configuration properties.
	 */
	public TestConfiguration() {   
		super(false);
	}

	/*
	 * Class Constructor.
	 * 
	 * A constructor that allows you to specify whether or not you would like
	 * the Hadoop Configuration to load default Hadoop config properties.
	 */
	public TestConfiguration(boolean loadDefaults) {
		super(loadDefaults);
	}

	/*
	 * Class constructor.
	 * 
	 * A constructor that allows you to specify a custom configuration.
	 */
	public TestConfiguration(Configuration other) {
		super(other);
	}

	/*
	 * Cleans up any test configuration written to disk.
	 */
	public abstract void cleanup();
	
	/*
	 * Writes any test configuration to disk.
	 */
	public abstract void write();
	
	public abstract String getHadoopProp(String key);
	
}
