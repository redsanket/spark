package hadooptest;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.MultiClusterClient;
import hadooptest.cluster.MultiClusterServer;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedExecutor;
import hadooptest.workflow.hadoop.job.JobClient;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.BeforeClass;

/**
 * TestSession is the main driver for the automation framework.  It
 * maintains a central logging framework, and central configuration
 * for the framework.  Additionally, the TestSession maintains a
 * common instance of the Hadoop cluster type specified in the 
 * framework configuration file, as well as a process executor to match.
 * 
 * For each test based on the framework, TestSession should be the 
 * superclass (a test class must extend TestSession).  TestSession will
 * then provide that class with a logger, cluster instance, framework
 * configuration reference, and an executor for system processes.
 * 
 * Additionally, for each test based on the framework, the test will need
 * to call TestSession.start() exactly once for each instance of the test
 * class.  TestSession.start() initializes all of the items that 
 * TestSession provides.
 */
public abstract class TestSession extends TestSessionCore {

	/** The Hadoop Cluster to use for the test session */
	public static HadoopCluster cluster;
	
	/** The multi-cluster server host thread **/
	public static MultiClusterServer multiClusterServer;

	/** The multi-cluster client thread **/
	public static MultiClusterClient multiClusterClient;

    public static String TASKS_REPORT_LOG = "tasks_report.log";
    public static String CROSS_COLO_PERF_LOG = "crosscolo_perf.log";
    public static long startTime=System.currentTimeMillis();
    
    public static enum HTF_TEST { CLASS, METHOD }

    /*
     * Run before the start of each test class.
     */
    @BeforeClass
    public static void startTestSession() throws Exception {
        System.out.println("--------- @BeforeClass: TestSession: startTestSession ---------------------------");
        start();
    }
    
    /*
     * After each test, fetch the job task reports.
     */
    @After
    public void logTaskReportSummary() 
            throws Exception {

        // Do Nothing For GDM
        if ((conf.getProperty("GDM_ONLY") != null) && 
            (conf.getProperty("GDM_ONLY").equalsIgnoreCase("true"))) {
            return;
        }
        
        /*
        if (category.equals(ParallelMethodTests.class)) {
            TestSession.logger.debug(
                    "logTaskReportSummary currently does not support " +
                    "parallel method tests.");
            return;
        }
        */
        
        if (Boolean.parseBoolean(
                conf.getProperty("LOG_TASK_REPORT")) == false) {
            return;
        }
        
        TestSession.logger.info("--------- @After: TestSession: logTaskReportSummary ----------------------------");

        // Log the tasks report summary for jobs that ran as part of this test 
        JobClient jobClient = TestSession.cluster.getJobClient();
        int numAcceptableNonCompleteMapTasks = 200;
        int numAcceptableNonCompleteReduceTasks = 200;
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary(
                        TestSession.TASKS_REPORT_LOG, 
                        TestSession.testStartTime, HTF_TEST.METHOD),
                        numAcceptableNonCompleteMapTasks,
                        numAcceptableNonCompleteReduceTasks);        
    }

	/**
	 * Initializes the test session in the following order:
	 * Initializes framework configuration, initializes the
	 * centralized logger, initializes the cluster reference.
	 * 
	 * This method should be called once from every subclass
	 * of TestSession, in order to initialize the 
	 * TestSession for a test class.
	 */
	public static void start() {
	    // Pass the caller class name
	    printBanner(Thread.currentThread().getStackTrace()[2].getClassName());
	    	    
		// Initialize the framework name
		initFrameworkName();

		// Initialize the framework configuration
		initConfiguration();
		
		// Initialize the framework logger
		initLogging();
		
		// Log Java Properties
		initLogJavaProperties();

		// Check to see if the property GDM_ONLY is defined in the hadooptest
		// configuration file.  If so, we want to exit the TestSession start
		// method before we do any Hadoop-specific configuration and setup.
		// This is intended to be used when running GDM tests on a GDM node
		// that isn't deployed as part of a Hadoop cluster.
		if(!(conf.getProperty("GDM_ONLY") == null)) {
			if(conf.getProperty("GDM_ONLY").equalsIgnoreCase("true")) {
				initExecutor();
				return;
			}
		}
		
		// Initialize the cluster to be used in the framework
		initCluster();

		initSecurity();
		
		initMultiCluster();		
	}
	
	// stop() being implemented for multi-cluster support, so we can stop the
	// multi-cluster server thread at the end of the test session.
	public static void stop() {
		multiClusterServer.stopServer();
		multiClusterClient.stopClient();
	}

    private static String getDateFormat(Date date, String format) {
        DateFormat df = new SimpleDateFormat(format);
        return df.format(date);        
    }    
	
    public static String getFileDateFormat(Date date) {
        return getDateFormat(date, "yyyy-MMdd-HHmmss");
    }

    public static String getLogDateFormat(Date date) {
        return getDateFormat(date, "yyyy-MM-dd HH:mm:ss.SSS z");
    }

	public static String getFileDateFormat(long time) {
	    return getFileDateFormat(new Date(time));
    }

    public static String getLogDateFormat(long time) {
        return getLogDateFormat(new Date(time));
    }

	/**
	 * Get the Hadoop cluster instance for the test session.
	 * 
	 * @return HadoopCluster the Hadoop cluster instance for the test session.
	 */
	public static HadoopCluster getCluster() {
		return cluster;
	}
	
	private static void initExecutor() {
		// Retrieve the cluster type from the framework configuration file.
		// This should be in the format of package.package.class
		String strClusterType = conf.getProperty("CLUSTER_TYPE");

		// Initialize the test session executor instance with the correct cluster type.
		if (strClusterType.equals("hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster")) {
			exec = new FullyDistributedExecutor();
		}
		else if (strClusterType.equals("hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster")) {
			exec = new PseudoDistributedExecutor();
		}
		else {
			logger.error("The cluster type is not yet fully supported: " + strClusterType);
		}
	} 
	
	/**
	 * Initialize the cluster instance for the framework.
	 */
	private static void initCluster() {
		// The unknown class type for the cluster
		Class<?> clusterClass = null;
		
		// The unknown constructor for the cluster class
		Constructor<?> clusterClassConstructor = null;
		
		// The unknown class type object instance for the cluster
		Object clusterObject = null;
		
		// Retrieve the cluster type from the framework configuration file.
		// This should be in the format of package.package.class
		String strClusterType = conf.getProperty("CLUSTER_TYPE");
		
		// Initialize the test session executor instance with the correct cluster type.
		initExecutor(); 

		// Create a new instance of the cluster class specified in the 
		// framework configuration file.
		try {
			clusterClass = Class.forName(strClusterType);
			clusterClassConstructor = clusterClass.getConstructor();
			clusterObject = clusterClassConstructor.newInstance();
		}
		catch (ClassNotFoundException cnf) {
			logger.error("The cluster type is not supported: " + strClusterType);
			cnf.printStackTrace();
		}
		catch (NoSuchMethodException nsm) {
			logger.error("The cluster type was found, but there is a problem locating the constructor for the class: " + strClusterType);
			nsm.printStackTrace();
		}
		catch (Exception e) {
			logger.error("The test session wasn't able to instantiate the class of type: " + strClusterType);
			e.printStackTrace();
		}
		
		// Initialize the test session cluster instance with the correct cluster type.
		if (clusterObject instanceof FullyDistributedCluster) {
			cluster = (FullyDistributedCluster)clusterObject;
		}
		else if (clusterObject instanceof PseudoDistributedCluster) {
			cluster = (PseudoDistributedCluster)clusterObject;
		}
		else {
			logger.error("The cluster type is not yet fully supported: " + strClusterType);
		}

        boolean CHECK_CLUSTER_STATE = Boolean.parseBoolean(
                    System.getProperty("CHECK_CLUSTER_STATE",
                            conf.getProperty("CHECK_CLUSTER_STATE", "true")));
        
        if (CHECK_CLUSTER_STATE) {
            if (cluster != null) {
                TestSession.logger.info("***************************************");
                TestSession.logger.info("Test Session Start: check cluster state " +
                        "is up:");
                TestSession.logger.info("***************************************");

                /* TODO: optionally restart the cluster. This may impact how the
                 * tests are being run in parallel classes.
                 */
                boolean RESTART_ON_FAILURE = Boolean.parseBoolean(
                                System.getProperty("RESTART_ON_FAILURE",
                                conf.getProperty("RESTART_ON_FAILURE", "true")));
                logger.info("RESTART_ON_FAILURE='"+RESTART_ON_FAILURE+"'");

                int resetClusterDelay = 
                        Integer.parseInt(System.getProperty("RESET_CLUSTER_DELAY",
                                conf.getProperty("RESET_CLUSTER_DELAY", "180")));
                logger.info("RESET_CLUSTER_DELAY='" + resetClusterDelay
                        + "' seconds.");

                ClusterState clusterState = null;
                try {
                    clusterState = cluster.getState();
                }
                catch (Exception e) {
                    logger.error("Failed to get the cluster state.", e);
                }

                if (clusterState == ClusterState.UP) {
                    logger.info("Cluster is fully up and ready to go.");                
                } else {			    
                    logger.warn("Cluster is not fully up: cluster state='" +
                            clusterState.toString() + "'.'");
                    if (RESTART_ON_FAILURE) {
                        // Check if the cluster is fully up. If not try n times to reset it.
                        int maxRetries = 3;
                        int numRetries = 1;
                        try {	                    
                            while ((clusterState != ClusterState.UP) &&
                                    (numRetries <= maxRetries)) {
                                TestSession.logger.info("Retry #" + numRetries + ":");
                                TestSession.logger.info("Reset cluster:");
                                TestSession.cluster.reset();
                                clusterState = cluster.getState();
                                numRetries++;
                            }
                            if (!TestSession.cluster.isFullyUp()) {
                                TestSession.logger.error("Cluster is NOT fully up after '" +
                                        maxRetries + "'.");
                            } else {
                                TestSession.logger.info("Cluster is fully up and ready.");                
                            }
                        }
                        catch (Exception e) {
                            TestSession.logger.error("Cluster is not fully up."+
                                    " Restart failed!!!");
                        }
                    }
                }
            }
        } else {
            logger.info("CHECK_CLUSTER_STATE='"+CHECK_CLUSTER_STATE+"'");            
        }
	}
	
	/**
	 * Initialize Hadoop API security for the test session.
	 */
	private static void initSecurity() {
		if (cluster != null) {
			try {
				// Initialize API security for the FullyDistributedCluster type only.
				if (cluster instanceof FullyDistributedCluster) {
					cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
				}
			}
			catch (IOException ioe) {
				logger.error("Failed to set the Hadoop API security.", ioe);
			}
		}
	}
	
    public static String getNamenodeURL(String clusterName) {
        return getCompURL(clusterName, "namenode", "50070");
    }

    public static String getResourceManagerURL(String clusterName) {
        return getCompURL(clusterName, "resourcemanager", "8088");
    }

    public static String getCompURL(
            String clusterName, String compType, String compPort) {
        clusterName = clusterName.trim().toLowerCase();
        String compNode="";
        String compURL="";
        try {
            // Fetch the component node from rolesdb via yinst
            String rolesdbCompType = (compType.equals("resourcemanager")) ?
                    "jobtracker" : compType;
            String[] output = TestSession.exec.runProcBuilder(new String[] {
                    "/usr/local/bin/yinst", "range", "-ir",
                    "@grid_re.clusters." + clusterName + "." +
                    rolesdbCompType });
            if (!output[0].equals("0")) {
                TestSession.logger.info(
                        "Got unexpected non-zero exit code: " + output[0]);
                TestSession.logger.info("stdout: " + output[1]);
                TestSession.logger.info("stderr: " + output[2]);
            } else {
                compNode = output[1].trim();
                compURL = "http://" + compNode + ":" + compPort;
                TestSession.logger.info(compType + " for cluster " +
                        clusterName + " is: '" + compNode + "'");
                // For namenode, use the hostname from rolesdb to check the
                // hadoop config file to determine if HA is used.
                if (compType.equals("namenode")) {
                    TestSession.logger.info("Check for HA namenode:");
                    // default FS from core-site.html may or may not include
                    // the port number. We need to parse it out

                    // Handle cross colo situations
                    ArrayList<String> cmd = new ArrayList<String>();
                    if (compNode.contains(".tan.ygrid.yahoo.com")) {
                        // If going to tan node, need to going thru the blue
                        // admin node with ACL opened to tan
                        cmd.add("/usr/bin/ssh");
                        cmd.add(HadoopCluster.ADMIN);
                    }
                    cmd.add("/usr/bin/ssh");
                    cmd.add(compNode);
                    cmd.add("grep -A 2 defaultFS");
                    cmd.add("/home/gs/conf/current/core-site.xml");
                    cmd.add("| tr '>' '\n'");
                    cmd.add("| tr '<' '\n'");
                    cmd.add("| grep com");
                    cmd.add("| sed 's|hdfs://||'");
                    cmd.add("| sed 's|:.*||'");
                    String[] command = cmd.toArray(new String[0]);
                    String[] output = TestSession.exec.runProcBuilderSecurity(command);

                    /*
                    output = TestSession.exec.runProcBuilder(new String[] {
                            "/usr/bin/ssh", compNode,
                            "grep -A 2 defaultFS " +
                            "/home/gs/conf/current/core-site.xml | " +
                            "tr '>' '\n' | tr '<' '\n' | grep com | " +
                            "sed 's|hdfs://||' | " +
                            "sed 's|:.*||'" });
                    */
                    output = TestSession.exec.runProcBuilder(command);

                    if (!output[0].equals("0")) {
                        TestSession.logger.info(
                                "Got unexpected non-zero exit code: " + output[0]);
                        TestSession.logger.info("stdout: " + output[1]);
                        TestSession.logger.info("stderr: " + output[2]);
                    } else {
                        compNode = output[1].trim();
                        // Construct the component URL
                        compURL = "http://" + compNode + ":" + compPort;
                        TestSession.logger.info(compType + " for cluster " +
                                clusterName + " is: '" + compNode + "'");
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        logger.info(compType + " URL read as '" + compURL + "'");
        return compURL;
    }

	/**
	 * Start listening for framework clients on other cluster gateways.
	 */
	public static void initMultiCluster() {
		String multiClusterType = conf.getProperty("MULTI_CLUSTER");

		int multiClusterPort;
		String multiClusterServerName;

		if (multiClusterType == null) {
			logger.debug("MULTI_CLUSTER is not defined in the config file.");
		} else {
			if (multiClusterType.equalsIgnoreCase("server")) {
				multiClusterPort = Integer.parseInt(
						conf.getProperty("MULTI_CLUSTER_PORT"));

				logger.info("Starting MultiClusterServer on port: "
						+ multiClusterPort);
				multiClusterServer = (new MultiClusterServer(multiClusterPort));
				multiClusterServer.start();
			} else if (multiClusterType.equalsIgnoreCase("client")) {
				multiClusterPort = Integer.parseInt(
						conf.getProperty("MULTI_CLUSTER_PORT"));
				multiClusterServerName = 
						conf.getProperty("MULTI_CLUSTER_SERVER");

				logger.info("Starting MultiClusterClient");
				multiClusterClient = (new MultiClusterClient(multiClusterPort,
						multiClusterServerName));
				multiClusterClient.start();
			} else {
				logger.debug("MULTI_CLUSTER type is not supported: "
						+ multiClusterType);
				logger.debug("Types supported for MULTI_CLUSTER configuration: "
						+ "client, server");
			}
		}
	}
	
    /**
     * add File Appender to Logger
     */
    public static void addLoggerFileAppender(String fileName) {
        Logger logger = TestSession.logger;
        FileAppender fileAppender = new FileAppender();
        fileAppender.setName(fileName);        
        fileAppender.setFile(
                TestSession.conf.getProperty("WORKSPACE_SF_REPORTS") +
                "/" + fileName);
        fileAppender.setLayout(new PatternLayout("%d %-5p %m%n"));
        fileAppender.setThreshold(Level.INFO);
        fileAppender.setAppend(true);
        fileAppender.activateOptions();
        logger.addAppender(fileAppender);
    }

    /**
     * add File Appender to Logger
     */
    public static void removeLoggerFileAppender(String fileName) {
        Logger logger = TestSession.logger;
        logger.removeAppender(fileName);
    }    
}
