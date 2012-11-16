package hadooptest;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import hadooptest.ConfigProperties;

public class TestSession {

	/* The Logger for the test session */
	public Logger logger;
	
	/* The test session configuration properties */
	public ConfigProperties conf;
	
	public TestSession() throws IOException {
		this.initConfiguration();
		this.initLogging();
	}
	
	private void initConfiguration() throws IOException {
		conf = new ConfigProperties();
		File conf_location = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/hadooptest.conf");
		conf.load(conf_location);
		System.out.println("Hadooptest conf property USER = " + conf.getProperty("USER"));
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
