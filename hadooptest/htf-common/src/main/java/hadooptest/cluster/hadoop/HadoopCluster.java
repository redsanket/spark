/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop;

import static org.junit.Assert.assertNotNull;
import hadooptest.TestSession;
import hadooptest.TestSessionCore;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;
import hadooptest.node.hadoop.pseudodistributed.PseudoDistributedNode;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.SecurityUtil;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x

import hadooptest.Util;
import hadooptest.cluster.ClusterState;

/**
 * An interface which should represent the base capability of any cluster
 * type in the framework.  This interface is also what should be commonly
 * used to reference the common cluster instance maintained by the
 * TestSession.  Subclasses of Cluster should implement a specific cluster
 * type.
 */
public abstract class HadoopCluster {

    /** String representing the cluster components. */
    public static final String NAMENODE = "namenode";
    public static final String SECONDARY_NAMENODE = "secondarynamenode";
    public static final String RESOURCE_MANAGER = "resourcemanager";
    public static final String DATANODE = "datanode";
    public static final String NODEMANAGER = "nodemanager";
    public static final String HISTORYSERVER = "historyserver";
    public static final String GATEWAY = "gateway";
    
    /** String array for the cluster components */
    public static final String[] components = {
        HadoopCluster.NAMENODE,
        HadoopCluster.SECONDARY_NAMENODE,
        HadoopCluster.RESOURCE_MANAGER,
        HadoopCluster.DATANODE,
        HadoopCluster.NODEMANAGER,
        HadoopCluster.GATEWAY,
        HadoopCluster.HISTORYSERVER
        };

    // Admin hosts
    public static final String ADMIN = TestSession.conf.getProperty("ADM_BOX"); 

    /** String representing the cluster type. */
    public static final String FD_CLUSTER_TYPE =
            "hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster";
    public static final String PD_CLUSTER_TYPE =
            "hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster";
    
    public static enum State { UP, DOWN, UNKNOWN }
    public static enum Action { START, STOP, RESET, STATUS }
    public static final String START = "start";
    public static final String STOP = "stop";
    
    public static enum HADOOP_EXEC_MODE { CLI, API }
    public static enum HADOOP_JOB_TYPE {
        SLEEP, WORDCOUNT, RANDOM_WRITER, RANDOM_TEXT_WRITER,
        TERAGEN, TERASORT, DFSIO, SLEEP_NN_CHECK }

    public static final String[] COMPRESS_ON = {
        "-Dmapreduce.map.output.compress=true",
        "-Dmapreduce.output.fileoutputformat.compress=true"
    };
    public static final String[] COMPRESS_OFF = {
        "-Dmapreduce.map.output.compress=false",
        "-Dmapreduce.output.fileoutputformat.compress=false"
    };  

    public static final String[] YARN_OPTS = {
        "-Dmapreduce.job.acl-view-job=*" 
    };

    public static final String[] YARN_OPTS2 = {
        "-Dmapreduce.map.memory.mb=2048",
        "-Dmapreduce.reduce.memory.mb=4096"
    };

    /* The compression job submission explicitly needs to use the 32-bit
     * compression libraries (i386-32) for the jobs to run successfully
     * as the nodes in the cluster run in 32-bit mode JVMs. The
     * verification of the jobs needs to be done with the 64-bit
     * compression libraries (amd64-64) as the gateway node JVM (and thus
     * all framework access of the hadoop API) is done through a 64-bit JVM.
     * The setting of java.library.home in pom-ci.xml accomplishes setting
     * the 64-bit native libraries for API use.
     */
    public static final String[] YARN_OPTS3 = {
        "-Dmapred.child.java.opts='-Djava.library.path=" + 
                TestSession.conf.getProperty("HADOOP_COMMON_HOME") + 
                "/lib/native/Linux-i386-32" + "'"    
    };

    /**
     * Container for storing the Hadoop cluster node objects by components
     * Each component contains a hash table of key hostname and
     * value HadoopNode */
    protected Hashtable<String, HadoopComponent> hadoopComponents =
            new Hashtable<String, HadoopComponent>();
    
    /* TODO: Consider maintaining a cluster level properties for tracking
     * cluster level paths and settings. 
     */

    /* Get the custom default settings filename. If the file exists, the content
     * is the full path name of the custom default hadoop config directory. 
     */
    public static String getDefaultConfSettingsFile(String component,
            String hostname) {
        return TestSession.conf.getProperty("HADOOP_CUSTOM_DEFAULT_CONF_DIR") +
                "/" + component + "-" + hostname;        
    }

    /**
     * Lookup an Igor role and get the role members.
     * 
     * @param roleName The name of the role.
     * 
     * @return ArrayList<String> a list of all of the role members.
     * 
     * @throws Exception
     */
	public ArrayList<String> lookupIgorRole(String roleName) throws Exception {
		ArrayList<String> roleMembers = new ArrayList<String>();
		
		TestSession.logger.debug("Fetching role members from Igor for role: " + roleName);
		String[] members = TestSession.exec.runProcBuilder(new String[] {"yinst", "range", "-ir", "@" + roleName});
		TestSession.logger.debug(members);
		
		for(String s: members) {
			if (s.contains("ygrid.yahoo.com") ||
			        s.contains("ygridvm.yahoo.com")) {
				roleMembers.add(s.trim());
			}
		}
		
		return roleMembers;
	}
	
	/**
	 * Lookup the Igor role members for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the nodes in the cluster.
     *
	 * @throws Exception
	 */
	public ArrayList<String> lookupIgorRoleClusterAllNodes(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.clusters." + clusterName);
	}
    
	/**
	 * Lookup the Igor-defined gateway node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the gateway nodes.
	 * 
	 * @throws Exception
	 */
	public ArrayList<String> lookupIgorRoleClusterGateway(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.clusters." + clusterName + ".gateway");
	}
	
	/**
	 * Lookup the Igor-defined jobtracker node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the jobtracker nodes.
	 * 
	 * @throws Exception
	 */
	public ArrayList<String> lookupIgorRoleClusterJobtracker(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.clusters." + clusterName + ".jobtracker");
	}
	
	/**
	 * Lookup the Igor-defined namenode for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the namenodes.
	 * 
	 * @throws Exception
	 */
	public ArrayList<String> lookupIgorRoleClusterNamenode(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.clusters." + clusterName + ".namenode");
	}
	
    /**
     * Lookup the Igor-defined namenode for a cluster.
     * 
     * @param clusterName The name of the cluster.
     * 
     * @return ArrayList<String> the namenodes.
     * 
     * @throws Exception
     */
    public ArrayList<String> lookupIgorRoleClusterNamenodeAlias(String clusterName) 
            throws Exception {
        return lookupIgorRole("grid_re.clusters." + clusterName + ".namenode_alias");
    }
    
	/**
	 * Lookup the Igor-defined secondary namenodes for a cluster.
	 * 
	 * @param clusterName
	 * @return
	 * @throws Exception
	 */
	public ArrayList<String> lookupIgorRoleClusterSecondaryNamenode(
			String clusterName) 
					throws Exception {
		return lookupIgorRole("grid_re.clusters." + clusterName + ".namenode2");
	}
	
	/**
	 * Start the cluster from a stopped state.
	 *
	 * @param waitForSafemodeOff to wait for safemode off after start.
	 * Default is true. 
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error in starting the cluster.
	 */
	public abstract boolean start(boolean waitForSafemodeOff) 
			throws Exception;

	/**
	 * Stop the cluster, shut it down cleanly to a state from which
	 * it can be restarted.
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error in starting the cluster.
	 */
	public abstract boolean stop() 
			throws Exception;

    /**
     * Start or stop the Hadoop daemon processes.
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public abstract int hadoopDaemon(Action action, String component) 
            throws Exception;
        
    /**
     * Start or stop the Hadoop daemon processes.
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param daemonHost The hostnames to perform the action on. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public abstract int hadoopDaemon(Action action, String component,
            String[] daemonHost) throws Exception;
    
    /**
     * Start or stop the Hadoop daemon processes. The method will also wait for
     * the daemons to fully start or stop depending on the expected state. 
     * It will also reinitialize the hadooptest configuration object with the
     * configuration directory if the action is start, and the configuration
     * directory is not the default one (which cannot be modified due to root
     * permission).  
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param daemonHost The hostnacomponent The cluster component to perform the action on. 
     * @param confDir The configuration directory to perform the action with. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public abstract int hadoopDaemon(Action action, String component,
            String[] daemonHost, String confDir) throws Exception;
    
	/**
	 * Get the current state of the cluster.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * @throws Exception if there is a fatal error getting
	 * 	       the cluster state.
	 */
	public abstract ClusterState getState()
			throws Exception;

	/**
	 * Check to see if all of the cluster daemons are running.
	 * 
	 * @return boolean true if all cluster daemons are running.
	 * 
	 * @throws Exception if there is a fatal error checking 
	 *         cluster state.
	 */
	public abstract boolean isFullyUp()
			throws Exception;

	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 * 
	 * @throws Exception if there is a fatal error checking
	 *         cluster state.
	 */
	public abstract boolean isFullyDown()
			throws Exception;

    /**
     * Check if the cluster component is fully up.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component is
     * up.
     */
    public abstract boolean isComponentFullyUp(String component) 
            throws Exception;
    
    /**
     * Check if the cluster component is fully up for a given String Array of
     * host names. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully up, false if the cluster is 
     * not fully up.
     * 
     * @throws Exception if there is a fatal error checking the state of a 
     * component.
     */
    public abstract boolean isComponentFullyUp(String component,
            String[] daemonHost) throws Exception;
    
    public boolean isHAEnabled() {
        // get the value of the dfs.nameservices in hdfs-ha.xml, if it's set to
        // flubber-alias* then cluster has been configured for HA failover in QE,
        // if not then the cluster is unlikely to support failover even if the
        // deployed with HA enabled
        String getValue = TestSession.cluster.getConf().get("dfs.nameservices");
        
        if (getValue == null || getValue.isEmpty()) {
            return false;
        }

        Pattern p = Pattern.compile("flubber-alias");
        Matcher m = p.matcher(getValue);
        if (m.find()) {
                return true;
        }
        else {
                return false;
        }
    }
    
    /**
     * Check if the cluster component is fully down.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster 
     * is not fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
    public abstract boolean isComponentFullyDown(String component) 
            throws Exception; 

    /**
     * Check if the cluster component is fully down for a given String Array of
     * host names. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster
     * is not fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
    public abstract boolean isComponentFullyDown(String component,
            String[] daemonHost) throws Exception;
        
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state
     * {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully in the expected state,
     * false if the cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in 
     * the expected state.
     */
    public abstract boolean isComponentFullyInExpectedState(Action action,
            String component) throws Exception;
    
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state 
     * {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * @param daemonHosts host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully in the expected state,
     * false if the cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in 
     * the expected state.
     */
    public abstract boolean isComponentFullyInExpectedState(Action action,
            String component, String[] daemonHosts) throws Exception;
    	
	/**
	 * Get the current version of the cluster.
	 * 
	 * @return ClusterState the state of the cluster.
	 */
	public abstract String getVersion();

	/**
	 * Get the cluster Hadoop configuration.
	 * 
	 * @return TestConfiguration the Hadoop cluster configuration.
	 */
	public abstract HadoopConfiguration getConf();

	/**
	 * Set the cluster Hadoop configuration.
	 * 
	 * @param conf the Hadoop cluster configuration to set for the cluster.
	 */
	public abstract void setConf(HadoopConfiguration conf);

    /**
	 * Sets the keytab and user and initializes Hadoop security through
	 * the Hadoop API.
	 * 
	 * @param keytab the keytab (like "keytab-hadoopqa")
	 * @param user the user (like "user-hadoopqa")
	 * 
	 * @throws IOException if cluster security can't be initialized.
	 */
	public void setSecurityAPI(String keytab, String user) throws IOException {
		TestSession.logger.info("Initializing Hadoop security");
		TestSession.logger.debug("Keytab = " + keytab);
		TestSession.logger.debug("User = " + user);
		SecurityUtil.login(TestSession.cluster.getConf(), keytab, user);
	}

    public void initNodes () throws Exception {
        initNodes(null, null, null, null, null);
    }
    
    public void initNodes (String[] gwHosts, String[] nnHosts, String[] nn2Hosts, 
    		String[] rmHosts, String[] dnHosts) throws Exception {
        // Gateway
        TestSession.logger.info("Initialize the gateway client node:");
        if (gwHosts == null || gwHosts.length == 0) {
            gwHosts = new String[] {"localhost"};
        }
        initComponentNodes(HadoopCluster.GATEWAY, gwHosts);
        
        // Namenode
        TestSession.logger.info("Initialize the namenode node(s):");
        if (nnHosts == null || nnHosts.length == 0) {
            String defaultFS = this.getConf().get("fs.defaultFS");
            String namenode = defaultFS.replaceFirst("hdfs://","");
            namenode = namenode.split(":")[0];
            nnHosts = new String[] {namenode};
        }
        initComponentNodes(HadoopCluster.NAMENODE, nnHosts);
        
        // Secondary Namenode
        TestSession.logger.info("Initialize the secondary namenode node(s):");
        if (nn2Hosts == null || nn2Hosts.length == 0) {
            String secondary_namenode_addr =
                    this.getConf().get("dfs.namenode.secondary.http-address");
            String secondarynamenode = secondary_namenode_addr.split(":")[0];
            nn2Hosts = new String[] {secondarynamenode};
        }
        initComponentNodes(HadoopCluster.SECONDARY_NAMENODE, nn2Hosts);
        
        // Resource Manager
        TestSession.logger.info("Initialize the resource manager node(s):");
        if (rmHosts == null || rmHosts.length == 0) {
            String rm_addr = this.getConf().get(
                    "yarn.resourcemanager.resource-tracker.address");
            String rm = rm_addr.split(":")[0];
            rmHosts = new String[] {rm};
        }
        initComponentNodes(HadoopCluster.RESOURCE_MANAGER, rmHosts);

        // History Server
        TestSession.logger.info("Initialize the history server node:");
        initComponentNodes(HadoopCluster.HISTORYSERVER, rmHosts);

        // Datanode
        TestSession.logger.info("Initialize the datanode node(s):");
        if (dnHosts == null || dnHosts.length == 0) {
            // The slave file must come from the namenode. They have different
            // values on other nodes. This will require that the namenode node and 
            // configuration class be initialized beforehand.         
            FullyDistributedCluster fdcluster = (FullyDistributedCluster) this;
            dnHosts = this.getHostsFromList(
                    nnHosts[0],
                    fdcluster.getConf(HadoopCluster.NAMENODE).getHadoopConfDir() +
                    "/slaves");            
        }
        initComponentNodes(HadoopCluster.DATANODE, dnHosts);
        
        // Nodemanager
        TestSession.logger.info("Initialize the nodemanager node(s):");
        initComponentNodes(HadoopCluster.NODEMANAGER, dnHosts);

        printNodes();
    }

    public void setupSingleQueueCapacity() throws Exception {
        this.setupSingleQueueCapacity(false);
    }
    
    public void setupSingleQueueCapacity(boolean force) throws Exception {
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
        String component = HadoopCluster.RESOURCE_MANAGER;

        /* 
         * NOTE: Add a check via the Hadoop API or jmx to determine if a single
         * queue is already in place. If so, skip the following as to not waste
         *  time.
         */
        YarnClientImpl yarnClient = this.getYarnClient();

        List<QueueInfo> queues =  yarnClient.getAllQueues(); 
        assertNotNull("Expected cluster queue(s) not found!!!", queues);        
        TestSession.logger.info("queues='" +
            Arrays.toString(queues.toArray()) + "'");
        if ((queues.size() == 1) &&
            (Float.compare(queues.get(0).getCapacity(), 1.0f) == 0)) {
                TestSession.logger.debug("Cluster is already setup properly. " +
                        "Nothing to do.");
                if (force == true) {
                    TestSession.logger.debug("Force single queue setup.");                    
                } else {
                    TestSession.logger.debug("Cluster is already setup properly. " +
                            "Nothing to do.");
                    return;
                }
        }
        
        // Backup the default configuration directory on the Resource Manager
        // component host.
        cluster.getConf(component).backupConfDir(); 

        // Copy files to the custom configuration directory on the
        // Resource Manager component host.
        String sourceFile = TestSession.conf.getProperty("WORKSPACE") +
                "/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
        cluster.getConf(component).copyFileToConfDir(sourceFile,
                "capacity-scheduler.xml");
        cluster.hadoopDaemon(Action.STOP, component);
        cluster.hadoopDaemon(Action.START, component);
    }

    public static void setupTestDir(String relativeOutputDir) throws Exception {
        // Define the test directory
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
            System.getProperty("user.name") + "/" + relativeOutputDir;

        // Delete it existing test directory if exists
        FileSystem fs = TestSession.cluster.getFS();
        FsShell fsShell = TestSession.cluster.getFsShell();
        if (fs.exists(new Path(testDir))) {
            TestSession.logger.info("Delete existing test directory: " +
                testDir);
            fsShell.run(new String[] {"-rm", "-r", testDir});
        }
        
        // Create or re-create the test directory.
        TestSession.logger.info("Create new test directory: " + testDir);
        fsShell.run(new String[] {"-mkdir", "-p", testDir});
    }

    /*
     * Create a generic job
     */
    public static GenericJob createGenericJob(
            HadoopCluster.HADOOP_JOB_TYPE jobType, String relativeOutputDir)
                    throws Exception {
        // Create a generic job
        GenericJob job = new GenericJob();
        job.setJobJar(
                TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));

        // Set generic job type
        if (jobType == HADOOP_JOB_TYPE.RANDOM_WRITER) {
            job.setJobName("randomwriter");            
        } else if (jobType == HADOOP_JOB_TYPE.RANDOM_TEXT_WRITER) {
            job.setJobName("randomtextwriter");            
        } else {
            throw new Exception("Uknown job type!!!");
        }
        
        // Set job args
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.addAll(Arrays.asList(YARN_OPTS));
        jobArgs.addAll(Arrays.asList(YARN_OPTS2));
        jobArgs.addAll(Arrays.asList(YARN_OPTS3));
        
        jobArgs.addAll(Arrays.asList(HadoopCluster.COMPRESS_OFF));
        if (jobType == HADOOP_JOB_TYPE.RANDOM_WRITER) {
            jobArgs.add("-Dmapreduce.randomwriter.totalbytes=1024");
            jobArgs.add(relativeOutputDir+"/byteInput");
        } else if (jobType == HADOOP_JOB_TYPE.RANDOM_TEXT_WRITER) {
            jobArgs.add("-Dmapreduce.randomtextwriter.totalbytes=1024");
            jobArgs.add(relativeOutputDir+"/textInput");
        } else {
            throw new Exception("Uknown job type!!!");
        }
        job.setJobArgs(jobArgs.toArray(new String[0]));
        
        return job;
    }

    /*
     * randomwriter: A map/reduce program that writes 10GB of random data per node.
     */
    public void setupRwTestData(String relativeOutputDir) 
            throws Exception {
        setupTestDir(relativeOutputDir);
        Vector<GenericJob> jobVector = new Vector<GenericJob>();
        jobVector.add(
                createGenericJob(
                        HadoopCluster.HADOOP_JOB_TYPE.RANDOM_WRITER,
                        relativeOutputDir));
        runJobs(jobVector);
    }
   
    /*
     * randomtextwriter: A map/reduce program that writes 10GB of random textual data per node.
     */
    public void setupRtwTestData(String relativeOutputDir)
            throws Exception {        
        setupTestDir(relativeOutputDir);
        Vector<GenericJob> jobVector = new Vector<GenericJob>();
        jobVector.add(
                createGenericJob(
                        HadoopCluster.HADOOP_JOB_TYPE.RANDOM_TEXT_WRITER,
                        relativeOutputDir));
        runJobs(jobVector);
    }

    /*
     * randomwriter and random text writer
     */
    public void setupRwRtwTestData(String relativeOutputDir)
            throws Exception {
        setupTestDir(relativeOutputDir);
        Vector<GenericJob> jobVector = new Vector<GenericJob>();
        jobVector.add(
                createGenericJob(
                        HadoopCluster.HADOOP_JOB_TYPE.RANDOM_WRITER,
                        relativeOutputDir));
        jobVector.add(
                createGenericJob(
                        HadoopCluster.HADOOP_JOB_TYPE.RANDOM_TEXT_WRITER,
                        relativeOutputDir));
        TestSession.cluster.runJobs(jobVector);        
    }
    
    public void runJobs(Vector<GenericJob> jobVector) throws Exception {
        runJobs(jobVector, 120, 3);
    }

    public void runJobs(Vector<GenericJob> jobVector,
            int waitForIDsec, int waitForSuccessMin) throws Exception {
        // Start the jobs
        for (GenericJob job : jobVector) {
            job.start();
        }
        
        // Wait for job IDs
        for (GenericJob job : jobVector) {
            job.waitForID(waitForIDsec);
        }
        
        // Wait for success
        for (GenericJob job : jobVector) {
            job.waitForSuccess(waitForSuccessMin);
        }        
    }

    public void printNodes() {
        TestSession.logger.debug("-- listing cluster nodes --");
        Enumeration<String> components = hadoopComponents.keys();
        while(components.hasMoreElements()) {
            String component = (String) components.nextElement();
            hadoopComponents.get(component).printNodes();
        }        
    }
    
    /**
     * Initialize the Hadoop component nodes for a give component type.
     * 
     * @param component String.
     * @param hosts String Array.
     * 
     * @throws IOException if nodes can't be initialized.
     */
	protected void initComponentNodes(String component, String[] hosts)
	        throws Exception {
        String clusterType = TestSession.conf.getProperty("CLUSTER_TYPE");
	    TestSession.logger.debug("Initialize cluster nodes for component '" +
	        component + "', hosts '" + StringUtils.join(hosts, ",") + "', " +
	            "cluster type '" +
	            clusterType.substring(clusterType.lastIndexOf(".")+1) + "'.");

	    Hashtable<String, HadoopNode> cNodes =
	            new Hashtable<String, HadoopNode>();
	    String compHostsSize = Integer.toString(hosts.length);
	    int index=1;
	    for (String host : hosts) {
	        TestSession.logger.debug("Init '" + component + "' " +
	                "host '" + host + "' [" + index++ + "/" +
	                compHostsSize + "]");
	        
	        // Initialize the component node.
	        if (clusterType.equals(HadoopCluster.FD_CLUSTER_TYPE)) {
	            cNodes.put(host, new FullyDistributedNode(host, component));
	        } else {
	            cNodes.put(host, new PseudoDistributedNode(host, component));
	        }
	            
	        // Verify the instantiated node.
	        if (TestSession.logger.isTraceEnabled()) {
	            TestSession.logger.trace("Verify instantiated cluster nodes:");
	            HadoopNode node = cNodes.get(host);         
	            TestSession.logger.trace("Instantiated node name='" +
	                    node.getHostname() + "': " + "default conf='" +
	                    node.getDefaultConfDir() + "', " + "conf='" +
	                    node.getConfDir() + "'.");           
	            HadoopConfiguration conf = node.getConf();
	            if (conf == null) {
	                TestSession.logger.error("Node conf object is null!!!");
	            }
	            TestSession.logger.trace("Instantiated node conf object: " + 
	                    "default conf dir='" + conf.getDefaultHadoopConfDir() +
	                    "', conf dir='" + conf.getHadoopConfDir() + "'");
	        }
	    }
	    	    
	    this.hadoopComponents.put(
	            component,
	            new HadoopComponent(component, cNodes));
	}
	
	/**
	 * Parse the host names from a host name list on the namenode.
	 * 
	 * @param namenode the namenode hostname. 
	 * @param file the file name. 
	 * 
	 * @return String Array of host names.
	 * 
	 * @throws Exception if the ssh process to cat the namenode hostname
	 *         file fails in a fatal manner.
	 */
	protected String[] getHostsFromList(String namenode, String file) 
			throws Exception {
		String[] output = TestSession.exec.runProcBuilder(
				new String[] {
				        "ssh",
	                    "-o", "StrictHostKeyChecking=no",
	                    "-o", "UserKnownHostsFile=/dev/null",
				        namenode,
				        "/bin/cat",
				        file});
		String[] nodes = output[1].replaceAll("\\s+", " ").trim().split(" ");
		TestSession.logger.trace("Hosts in file are: " + Arrays.toString(nodes));		
		return nodes;
	}

	/**
	 * Returns the Hadoop cluster hostnames hashtable.
	 * 
	 * @return Hashtable of String Arrays hostnames for each of the cluster
	 * components.
	 */
    public Hashtable<String, HadoopComponent> getComponents() {
        return this.hadoopComponents;
    }

	/**
	 * Returns the cluster nodes hostnames for the given component.
	 * 
	 * @param component The hadoop component such as gateway, namenode,
	 * resourcemaanger, etc.
	 * 
	 * @return String Arrays for the cluster nodes hostnames.
	 */
	public Hashtable<String, HadoopNode> getNodes(String component) {
	    return this.hadoopComponents.get(component).getNodes();
	}

    /**
     * Returns the first cluster node instance for the gateway component.
     * 
     * @return HadoopNode object.
     */
    public HadoopNode getNode() {
        return this.getNode(null);
    }    
    
    /**
     * Returns the first cluster node instance for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return HadoopNode object.
     */
    public HadoopNode getNode(String component) {
        if ((component == null) || component.isEmpty()) {
            component = HadoopCluster.GATEWAY;
        }        
        HadoopNode node = this.getNodes(component).elements().nextElement();
        return node;
    }    
	
    /**
     * Returns the cluster nodes hostnames for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String Arrays for the cluster nodes hostnames.
     */
    public String[] getNodeNames(String component) {
        Hashtable<String, HadoopNode> nodes = this.getNodes(component);
        Set<String> keys = nodes.keySet();
        return keys.toArray(new String[0]);
    }
	    
	/**
	 * Start the cluster from a stopped state.
	 *
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error starting the cluster.
	 */
	public boolean start() 
			throws Exception {
		return start(true);
	}
	
	/**
	 * Restart the cluster.
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error stopping or
	 *         starting the cluster.
	 */
	public boolean reset() 
			throws Exception {	
		boolean stopped = this.stop();
		if (!stopped) {
			TestSession.logger.error("cluster did not stop!!!");
		}
		
		/* Workaround for Bugzilla Ticket 6555489 - sssd crashes
		 * Sleep a few minutes for the sssd to recover from crashing
		 */
        int resetClusterDelay = 
                Integer.parseInt(System.getProperty("RESET_CLUSTER_DELAY", "180"));
        TestSession.logger.info("RESET_CLUSTER_DELAY='" + resetClusterDelay
                + "' seconds.");
		TestSession.logger.info("Sleep '" + resetClusterDelay + 
		        "' seconds for sssd to recover...");
		Thread.sleep(resetClusterDelay*1000);
		
		boolean started = this.start();
		if (!started) {
			TestSession.logger.error("cluster did not start!!!");
		}
		return (stopped && started);
	}
	
    /**
     * Get the Hadoop Cluster name.
     * 
     * @return Name of the Hadoop Cluster.
     * 
     */
    public String getClusterName() {
        return System.getProperty("CLUSTER_NAME");        
    }
	
	/**
	 * Get the Hadoop Cluster object containing the cluster info.
	 * 
	 * @return Cluster for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public Cluster getClusterInfo() throws IOException {
		return new Cluster(TestSession.cluster.getConf());
	}

	/**
	 * Get the Hadoop Cluster object containing the cluster info.
	 * 
	 * @return Cluster for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public YarnClientImpl getYarnClient() throws IOException {
		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();
		return yarnClient;
	}
	
    /**
     * Get the queue capacities
     * 
     * @return Map of queue capacities where the key is the queue name, and the
     * value is the queue capacity.
     * 
     * @throws IOException.
     */
    public Map<String, Float> getQueueCapacity() throws Exception {
        YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();        
        List<QueueInfo> queues =  yarnClient.getAllQueues();
        Map<String, Float> capacity = new HashMap<String, Float>();
        int index = 1;
        for (QueueInfo queue : queues) {
            TestSession.logger.info(
                    "Queue " + index++ + " name=" + queue.getQueueName() +
                    ", capacity=" + queue.getCapacity());                        
            capacity.put(queue.getQueueName(), queue.getCapacity());
        }
        return capacity;
    }

    /**
     * Get the queue capacities
     * 
     * @param Queue name
     * @return Queue capacities where the key is the queue name, and the
     * value is the queue capacity.
     * 
     * @throws IOException.
     */
    public Float getQueueCapacity(String queueName) throws Exception {
        Map<String, Float> capacity = this.getQueueCapacity();
        return capacity.get(queueName);
    }

    /**
     * Get the queue capacities
     * 
     * @return Map of queue capacities where the key is the queue name, and the
     * value is the queue capacity.
     * 
     * @throws IOException.
     */
    public Map<String, QueueInfo> getQueueInfo() throws Exception {
        YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();        
        List<QueueInfo> queues =  yarnClient.getAllQueues();
        Map<String, QueueInfo> queueInfoMp = new HashMap<String, QueueInfo>();
        int index = 1;
        for (QueueInfo queueInfo : queues) {
            TestSession.logger.info(
                    "Queue " + index++ + " name=" + queueInfo.getQueueName() +
                    ", capacity=" + queueInfo.getCapacity());                        
            queueInfoMp.put(queueInfo.getQueueName(), queueInfo);
        }
        return queueInfoMp;
    }

    /**
     * Get the queue capacities
     * 
     * @param Queue name
     * @return Queue capacities where the key is the queue name, and the
     * value is the queue capacity.
     * 
     * @throws IOException.
     */
    public QueueInfo getQueueInfo(String queueName) throws Exception {
        Map<String, QueueInfo> queueInfo = this.getQueueInfo();
        return queueInfo.get(queueName);
    }

        
	public List<QueueInfo> getQueneInfo() throws Exception {
        YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();        
        List<QueueInfo> queues =  yarnClient.getAllQueues();
        TestSession.logger.info(
                "Queues='" + Arrays.toString(queues.toArray()) + "'");
        return queues;
    }
    
    public void printQueueCapacity(Map<String, Float> capacity) {
        int index = 1;
        
        for (String queueName: capacity.keySet()) {
            TestSession.logger.info(
                    "Queue " + index++ + " name=" + queueName +
                    ", capacity=" + capacity.get(queueName).toString());
        } 
    }

    public void printQueueCapacity(List<QueueInfo> queues) {
        int index = 1;
        for (QueueInfo queue : queues) {
            queue.getCapacity();
            TestSession.logger.info(
                    "Queue " + index++ + " name=" + queue.getQueueName() +
                    ", capacity=" + queue.getCapacity());            
        }
    }
    
    /**
     * Get the Job Client
     * 
     * @return JobClient instance.
     * 
     * @throws IOException.
     */
    public JobClient getJobClient() throws IOException {
        return new JobClient(TestSession.getCluster().getConf());
    }
    

	/**
	 * Get the cluster Hadoop file system.
	 * 
	 * @return FileSystem for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public FileSystem getFS() throws IOException {
		return FileSystem.get(this.getConf());
	}

	/**
	 * Get the cluster Hadoop file system shell.
	 * 
	 * @return FS Shell for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS shell.
	 */
	public FsShell getFsShell() throws IOException {
		return new FsShell(this.getConf());
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error when waiting to turn
	 *         safe mode off.
	 */
	public boolean waitForSafemodeOff() 
			throws Exception {
		return waitForSafemodeOff(-1, null);
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 *
	 * @param timeout time to wait for safe mode to be off.
	 * @param fs file system under test
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error when waiting to turn
	 *         safe mode off.
	 */
	public boolean waitForSafemodeOff(int timeout, String fs) 
			throws Exception {
		return waitForSafemodeOff(timeout, fs, false);
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 *
	 * @param timeout time to wait for safe mode to be off.
	 * @param fs file system under test
	 * @param verbose true for on, false for off.
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error in the process to 
	 *         check safe mode state, or the thread is not able to sleep.
	 */
	public boolean waitForSafemodeOff(int timeout, String fs, boolean verbose) 
			throws Exception {

		if (timeout < 0) {
			int defaultTimeout = 300;
			timeout = defaultTimeout;
		}

		if ((fs == null) || fs.isEmpty()) {
			fs = this.getConf().get(
					"fs.defaultFS", HadoopConfiguration.HADOOP_CONF_CORE);
		}

        String namenode = this.getNodeNames(HadoopCluster.NAMENODE)[0];
		String[] safemodeGetCmd = { this.getConf().getHadoopProp("HDFS_BIN"),
				"--config", this.getConf().getHadoopConfDir(),
				"dfsadmin", "-fs", fs, "-safemode", "get" };

		String[] output = TestSession.exec.runProcBuilderSecurity(
		        safemodeGetCmd, verbose);

		boolean isSafemodeOff = 
				(output[1].trim().equals("Safe mode is OFF")) ? true : false;

		/* for the time out duration wait and see if the namenode comes out of
		 * safemode
		 */
		int waitTime=5;
		int i=1;
		while ((timeout > 0) && (!isSafemodeOff)) {
			TestSession.logger.info("Wait for safemode to be OFF: TRY #" + i +
			        ": WAIT " + waitTime + "s:" );
			Util.sleep(waitTime);

			output = TestSession.exec.runProcBuilderSecurity(safemodeGetCmd, verbose);

			isSafemodeOff = 
					(output[1].trim().contains("Safe mode is OFF")) ? true : false;
			timeout = timeout - waitTime;
			i++;
		}

		if (!isSafemodeOff) {
			TestSession.logger.info("ALERT: NAMENODE '" + namenode +
			        "' IS STILL IN SAFEMODE");
		}

		return isSafemodeOff;
	}

}
