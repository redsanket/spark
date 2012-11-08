/*
 * YAHOO!
 */

package hadooptest.config;

import org.apache.hadoop.conf.Configuration;

/* 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class TestConfiguration extends Configuration {

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

}
