package hadooptest;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import hadooptest.ConfigProperties;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;

public class TestSession {

	/* The Logger for the test session */
	public Logger logger;

	// TODO: this should be a generic cluster
	protected FullyDistributedCluster cluster;

	/* The test session configuration properties */
	public ConfigProperties conf;
	
	public TestSession() throws IOException {
		this.initConfiguration();
		this.initLogging();
		
		// TODO: this should be a generic cluster
		cluster = new FullyDistributedCluster(this);

    	String classpath = System.getProperty("java.class.path");
		logger.info("CLASSPATH="+classpath);
	}
	
	// TODO: this should be a generic cluster
	public FullyDistributedCluster getCluster() {
		return cluster;
	}
	
	private void initConfiguration() throws IOException {
		conf = new ConfigProperties();
		
		String osName = System.getProperty("os.name");
		System.out.println("Operating System: " + osName);
		
		String userHome = System.getProperty("user.home");
		System.out.println("User home: " + userHome);
		
		String userName = System.getProperty("user.name");
		System.out.println("User name: " + userName);
		
		File conf_location = null;
		
		if(userName.equals("yahoo")) {
			// We are using a headless build user, so pull the configuration
			// from somewhere other than the user home directory.
			conf_location = new File("/tmp/hadooptest/hadooptest.conf");
		}
		else {
			if (osName.contains("Mac OS X")) {
				conf_location = new File(userHome + "/hadooptest.conf");
			}
			else if (osName.contains("Linux")) {
				conf_location = new File(userHome + "/hadooptest.conf");
			}
			else {
				System.out.println("OS is not supported by hadooptest: "  + osName);
			}
		}
		
		conf.load(conf_location);
	}
	
	private void initLogging() {
		logger = Logger.getLogger(TestSession.class);
		Level logLevel = Level.ALL;  // All logging is turned on by default
		
		String strLogLevel = conf.getProperty("LOG_LEVEL");
		
		if (strLogLevel == "OFF") {
			logLevel = Level.OFF;
		}
		else if (strLogLevel == "ALL") {
			logLevel = Level.ALL;
		}
		else if (strLogLevel == "DEBUG") {
			logLevel = Level.DEBUG;
		}
		else if (strLogLevel == "ERROR") {
			logLevel = Level.ERROR;
		}
		else if (strLogLevel == "FATAL") {
			logLevel = Level.FATAL;
		}
		else if (strLogLevel == "INFO") {
			logLevel = Level.INFO;
		}
		else if (strLogLevel == "TRACE") {
			logLevel = Level.TRACE;
		}
		else if (strLogLevel == "WARN") {
			logLevel = Level.WARN;
		}
		else {
			logLevel = Level.ALL;
		}
		
		logger.setLevel(logLevel);
	}
	
}
