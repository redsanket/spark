package hadooptest;

import hadooptest.cluster.Executor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * TestSessionCore is the main driver for the automation framework. It maintains
 * a central logging framework, and central configuration for the framework.
 * 
 * For each test based on the framework, TestSessionCore should be the
 * superclass (a test class must extend TestSession). TestSessionCore will then
 * provide that class with a logger, framework configuration reference, and an
 * executor for system processes.
 * 
 * Additionally, for each test based on the framework, the test will need to
 * call start() exactly once for each instance of the test class. start()
 * initializes all of the items that TestSessionCore provides.
 */
public abstract class TestSessionCore {

	/** The Logger for the test session */
	public static Logger logger;

	/** The test session configuration properties */
	public static ConfigProperties conf;

	/** The process executor for the test session */
	public static Executor exec;

	public static String frameworkName = "uninitialized";

    public static String currentTestName;
    public static String currentTestMethodName;
    public static long testStartTime;
	
	/**
	 * In the JUnit architecture, this constructor will be called before every
	 * test (per JUnit). Therefore, it is better to leave the constructor here
	 * empty and use start() to initialize the test session instead.
	 * 
	 * We can also use the TestSession constructor to execute instructions
	 * before each test, so the user doesn't have to code these instructions
	 * into every test. However, this can be dangerous with JUnit as exceptions
	 * that occur in the TestSession constructor won't be caught by JUnit (and
	 * thus it won't run the @After for the test which triggers the exception).
	 */
	public TestSessionCore() {
	}

    /*
     * Run before the start of each test class.
     */
    @Before
    public void startTest() throws Exception {
        testStartTime = System.currentTimeMillis();
    }
	
    public static void printBanner(String msg) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentThreadClassName = Thread.currentThread().getStackTrace()[1].getClassName();
        System.out.println("********************************************************************************");
        System.out.println(sdf.format(new Date(System.currentTimeMillis())) + " " +
                currentThreadClassName + " - starting test: " + msg);
        System.out.println("********************************************************************************");
    }

    /*
     * Print method names for all tests in a class:
     * Print name of currently executing test 
     */
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            currentTestName =
                    description.getClassName();
            currentTestMethodName =
                    description.getClassName() + ": " + description.getMethodName();
            printBanner(currentTestMethodName);
        }
    };
	
    /**
     * Initialize the framework name.
     */
    public static void initFrameworkName() {
        frameworkName = "hadooptest";
    }
    
	/**
	 * Log the basic java properties used by the framework.
	 */
	public static void initLogJavaProperties() {
		logger.debug("JAVA home ='" + System.getProperty("java.home") + "'");
		logger.debug("JAVA version ='" + System.getProperty("java.version")
				+ "'");
		logger.debug("JVM bit size ='"
				+ System.getProperty("sun.arch.data.model") + "'");
		logger.debug("JAVA native library path='"
				+ System.getProperty("java.library.path") + "'");
		logger.debug("JAVA CLASSPATH='" + System.getProperty("java.class.path")
				+ "'");
	}

	/**
	 * Initialize the framework logging.
	 */
	public static void initLogging() {
		logger = Logger.getLogger(TestSessionCore.class);
		Level logLevel = Level.ALL; // All logging is turned on by default

		String strLogLevel = conf.getProperty("LOG_LEVEL");

		if (strLogLevel.equals("OFF")) {
			logLevel = Level.OFF;
		} else if (strLogLevel.equals("ALL")) {
			logLevel = Level.ALL;
		} else if (strLogLevel.equals("DEBUG")) {
			logLevel = Level.DEBUG;
		} else if (strLogLevel.equals("ERROR")) {
			logLevel = Level.ERROR;
		} else if (strLogLevel.equals("FATAL")) {
			logLevel = Level.FATAL;
		} else if (strLogLevel.equals("INFO")) {
			logLevel = Level.INFO;
		} else if (strLogLevel.equals("TRACE")) {
			logLevel = Level.TRACE;
		} else if (strLogLevel.equals("WARN")) {
			logLevel = Level.WARN;
		} else {
			System.out.println("set to all ");
			logLevel = Level.ALL;
		}
		logger.setLevel(logLevel);
		logger.trace("Set log level to " + logLevel);
	}

	/**
	 * Initialize the framework configuration.
	 */
	public static void initConfiguration() {
		File conf_location = null;
		conf = new ConfigProperties();

		String osName = System.getProperty("os.name");
		System.out.println("Operating System: " + osName);

		String userHome = System.getProperty("user.home");
		System.out.println("User home: " + userHome);

		String userName = System.getProperty("user.name");
		System.out.println("User name: " + userName);

		if (osName.contains("Mac OS X") || osName.contains("Linux")) {
			System.out.println("Detected OS is supported by framework.");
		} else {
			System.out.println("OS is not supported by framework: " + osName);
		}

		String strFrameworkConfig = System.getProperty(frameworkName
				+ ".config", userHome + "/" + frameworkName + ".conf");
		System.out.println("Framework configuration file location: "
				+ strFrameworkConfig);
		System.out.println("To specify a different location, use -Dhadooptest.config=");

		conf_location = new File(strFrameworkConfig);

		try {
		    conf.load(conf_location);
		} catch (IOException ioe) {
		    System.out.println("Could not load the framework configuration file.");
		}

		/*
		 * Check for system properties that if specified should override the
		 * configuration file setting. For instance, For instance, user may use
		 * a common configuration file but with a different CLUSTER_NAME and/or
		 * WORKSPACE.  The user may also specify a custom admin box for the 
		 * framework by using ADM_BOX.
		 */
		String workspace = System.getProperty("WORKSPACE");
		if (workspace != null && !workspace.isEmpty()) {
		    conf.setProperty("WORKSPACE", workspace);
		}
		conf.setProperty("WORKSPACE_SF_REPORTS",
		        conf.getProperty("WORKSPACE") + "/htf-common/target/surefire-reports");
		
		String clusterName = System.getProperty("CLUSTER_NAME");
		if (clusterName != null && !clusterName.isEmpty()) {
		    conf.setProperty("CLUSTER_NAME", clusterName);
		}
		String admBox = System.getProperty("ADM_BOX");
		if (admBox != null && !admBox.isEmpty()) {
			conf.setProperty("ADM_BOX", admBox);
		}
		else {
			conf.setProperty("ADM_BOX", "devadm102.blue.ygrid.yahoo.com");
		}
		
        conf.setProperty("LOG_TASK_REPORT",
                System.getProperty("LOG_TASK_REPORT",
                        conf.getProperty("LOG_TASK_REPORT", "true")));

		// Display the configuration key value pairs
		Enumeration<Object> keys = conf.keys();
		while (keys.hasMoreElements()) {
		    String key = (String) keys.nextElement();
		    String value = (String) conf.get(key);
		    System.out.println(key + ": " + value);
		}

	}

}
