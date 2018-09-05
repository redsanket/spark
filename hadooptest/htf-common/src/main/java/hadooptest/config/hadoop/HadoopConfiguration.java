/*
 * YAHOO!
 */

package hadooptest.config.hadoop;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;

/** 
 * An abstract class that describes a base level Hadoop configuration to 
 * be used in test.
 */
public abstract class HadoopConfiguration extends Configuration {

    /** Filenames for the hadoop configuration files. */
    public static final String HADOOP_CONF_CORE = "core-site.xml";
    public static final String HADOOP_CONF_HDFS = "hdfs-site.xml";
    public static final String HADOOP_CONF_MAPRED = "mapred-site.xml";
    public static final String HADOOP_CONF_YARN = "yarn-site.xml";
    public static final String HADOOP_CONF_CAPACITY_SCHEDULER =
        "capacity-scheduler.xml";
    public static final String HADOOP_CONF_FAIR_SCHEDULER = 
        "fair-scheduler.xml";

    /** Configuration class attributes 
     */
    protected String hostname;
    protected String component;
    protected String hadoopConfDir;
    protected String defaultHadoopConfDir;
    
    /** General Hadoop configuration properties such as cluster name, 
     * directory paths, etc.
     */
    protected Properties hadoopProps = new Properties();

    /** 
     * A generic constructor TestConfiguration that calls the Hadoop
     * Configuration with the true argument, that you are loading
     * default Hadoop configuration properties.  It then proceeds to
     * initialize the default configuration for the reflected cluster type.
     * 
     * @throws Exception if the default hosts can not be initialized, or there
     * is a problem getting the Hadoop version.
     */
    public HadoopConfiguration() throws Exception {
        super(true);
        this.initDefaults();
    }

    /** 
     * A generic constructor TestConfiguration that calls the Hadoop
     * Configuration with the given load defaults option. A value of true will 
     * load the default Hadoop configuration properties, while a value of false
     * will not.  It then proceeds to initialize the default configuration for
     * the reflected cluster type.
     * 
     * @throws Exception if the default hosts can not be initialized, or there
     * is a problem getting the Hadoop version.
     */
    public HadoopConfiguration(boolean loadDefaults) throws Exception {
        super(loadDefaults);
        this.initDefaults();
    }

    /**
     * A constructor that allows you to specify whether or not you would like
     * the Hadoop Configuration to load default Hadoop config properties.
     * It then proceeds to initialize the default configuration for the 
     * reflected cluster type.
     * 
     * @param loadDefaults whether or not to load the cluster configuration
     * defaults using the Configuration superclass constructor.
     * 
     * @throws Exception if the default hosts can not be initialized or there is
     * a problem getting the Hadoop version.
     */
    public HadoopConfiguration(boolean loadDefaults,
            String defaultHadoopConfDir, String hostname, String component)
                    throws Exception {
        super(loadDefaults);
        this.initDefaults(defaultHadoopConfDir, hostname, component);
    }

    public HadoopConfiguration(boolean loadDefaults,
            String hostname, String component) throws Exception {
        super(loadDefaults);
        this.initDefaults(null, hostname, component);
    }

    /**
     * A constructor that allows you to specify a custom configuration.
     * 
     * @param other a custom Configuration.
     * 
     * @throws Exception if the default hosts can not be initialized or there
     *                   is a problem getting the Hadoop version.
     */
    public HadoopConfiguration(Configuration other) throws Exception {
        super(other);
        this.initDefaults();
    }

    
    /** 
     * 
     * Initializations
     * 
     * /

    /**
     * Initializes cluster specific defaults.
     * 
     * @throws UnknownHostException if the default hosts can not be initialized.
     */
    protected abstract void initDefaultsClusterSpecific()
        throws UnknownHostException;
    
    /**
     * Initializes a set of default configuration properties that have been 
     * determined to be a reasonable set of defaults for running a distributed
     * cluster under test.
     * 
     * @throws Exception if the default hosts can not be initialized, or if
     *                   there is a fatal error getting the Hadoop version.
     */
    private void initDefaults() throws Exception {
        initDefaults(TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"),
                null, null);
    }
    
    /**
     * Initializes a set of default configuration properties that have been 
     * determined to be a reasonable set of defaults for running a distributed
     * cluster under test.
     * 
     * @param default hadoop configuration directory path.
     * @param configuraiton hostname.
     * @param configuration host component.
     * 
     * @throws Exception if the default hosts can not be initialized, or if
     *                   there is a fatal error getting the Hadoop version.
     */
    private void initDefaults(String defaultHadoopConfDir, String hostname, 
            String component) throws Exception {
        TestSession.logger.trace("Init default Hadoop conf dir='" +
                defaultHadoopConfDir + "'");
        // Initialize the configuration class attributes.
        this.defaultHadoopConfDir = defaultHadoopConfDir;
        this.hadoopConfDir = defaultHadoopConfDir;
        this.hostname = hostname;
        this.component = component;

        this.setKerberosConf();
        
        this.initDefaultsClusterSpecific();
                
        /* 
         * Properties beyond this point should be common across pseudo and fully
         * distributed cluster configuration.
         */
        loadDefaultResource();

        /*
         * Use this.getHadoopConfFile() instead because the configuration
         * directory may change.
         *
         * hadoopProps.setProperty("HADOOP_CONF_CORE", confDir + "/" + HADOOP_CONF_CORE);
         * hadoopProps.setProperty("HADOOP_CONF_HDFS", confDir + "/" + HADOOP_CONF_HDFS);
         * hadoopProps.setProperty("HADOOP_CONF_MAPRED", confDir + "/" + HADOOP_CONF_MAPRED);
         * hadoopProps.setProperty("HADOOP_CONF_YARN", confDir + "/" + HADOOP_CONF_YARN);
         * hadoopProps.setProperty("HADOOP_CONF_CAPACITY_SCHEDULER", confDir + "/" + HADOOP_CONF_CAPACITY_SCHEDULER);
         * hadoopProps.setProperty("HADOOP_CONF_FAIR_SCHEDULER", confDir + "/" + HADOOP_CONF_FAIR_SCHEDULER);
         */

        // Binaries
        hadoopProps.setProperty("HADOOP_BIN_DIR", TestSession.conf.getProperty("HADOOP_COMMON_HOME") + "/bin");
        hadoopProps.setProperty("HADOOP_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hadoop");
        hadoopProps.setProperty("HDFS_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hdfs");
        hadoopProps.setProperty("MAPRED_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/mapred");
        hadoopProps.setProperty("YARN_BIN", getHadoopProp("HADOOP_BIN_DIR") + "/yarn");

        // Version dependent environment variables
        String HADOOP_VERSION = this.getVersion();

        hadoopProps.setProperty("HADOOP_VERSION", HADOOP_VERSION);
                
        // Jars
        hadoopProps.setProperty("HADOOP_JAR_DIR",
                TestSession.conf.getProperty("HADOOP_COMMON_HOME") +
                "/share/hadoop");        
        hadoopProps.setProperty("HADOOP_TEST_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
                                "/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
                                HADOOP_VERSION + "-tests.jar"); 
        hadoopProps.setProperty("HADOOP_EXAMPLE_JAR", getHadoopProp("HADOOP_JAR_DIR") +
                                "/mapreduce/" + "hadoop-mapreduce-examples-" +
                                HADOOP_VERSION + ".jar"); 
        hadoopProps.setProperty("HADOOP_MR_CLIENT_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
                                "/mapreduce/" + "hadoop-mapreduce-client-jobclient-" +
                                HADOOP_VERSION + ".jar"); 
        hadoopProps.setProperty("HADOOP_STREAMING_JAR", getHadoopProp("HADOOP_JAR_DIR") +
                                "/tools/lib/" + "hadoop-streaming-" + 
                                HADOOP_VERSION + ".jar");

        loadClusterResource();
    }

    /**
     * Get the Hadoop configuration full file path.
     */
    public String getHadoopConfFile(String file) {
        return this.getHadoopConfDir() + "/" + file;        
    }
        
    /**
     *  Load the cluster resources in the parent Hadoop Configuration class.
     */
    public void loadClusterResource() {
        TestSession.logger.trace("load hadoop resources from '" +
                                this.getHadoopConfDir() + "':");
        super.addResource(new Path(this.getHadoopConfFile(HADOOP_CONF_CORE)));
        super.addResource(new Path(this.getHadoopConfFile(HADOOP_CONF_HDFS)));
        super.addResource(new Path(this.getHadoopConfFile(HADOOP_CONF_MAPRED)));
        super.addResource(new Path(this.getHadoopConfFile(HADOOP_CONF_YARN)));  
    }
        
    /**
     * core-default.xml contains at least two properties that must be
     * defined in the Hadoop Configuration instance in order for the
     * test framework to interact with the Hadoop Classes and APIs.
     * Therefore, we are loading core-default.xml here so they will be
     * defined.
     * 
     * Here are the two properties that must be defined:
     * this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
     * this.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
     * 
     * NOTE:
     * Consider loading part or all of the following default xml files:
     * this.addResource(this.getConfResourceAsInputStream("core-default.xml"));
     * this.addResource(this.getConfResourceAsInputStream("hdfs-default.xml"));
     * this.addResource(this.getConfResourceAsInputStream("mapred-default.xml"));
     * this.addResource(this.getConfResourceAsInputStream("yarn-default.xml"));
     * this.addResource(this.getConfResourceAsInputStream("distcp-default.xml"));
     * this.addResource(this.getConfResourceAsInputStream("testserver-default.xml"));
     */
    protected void loadDefaultResource() {
        if (TestSession.logger.isTraceEnabled()) {
            URL dirURL = this.getClass().getClassLoader().getResource(
                    "core-default.xml");
            TestSession.logger.trace("Load hadoop default configurations via " +
                    "URL path: '" + dirURL.getPath() + "'.");            
        }
        super.addResource(this.getConfResourceAsInputStream("core-default.xml"));
        super.addResource(this.getConfResourceAsInputStream("hdfs-default.xml"));
        super.addResource(this.getConfResourceAsInputStream("mapred-default.xml"));
        super.addResource(this.getConfResourceAsInputStream("yarn-default.xml"));
    }

    
    /**
     * Cleans up any test configuration written to disk.
     */
    public abstract void cleanup();
        
    /**
     * Writes any test configuration to disk.
     * 
     * @throws IOException if the configuration can not be written to disk.
     */
    public abstract void write() throws IOException;

    /**
     * Returns the Hadoop general property value for a given property name.
     * 
     * @return String property value such as cluster name, directory paths, etc. 
     */
    public String getHadoopProp(String key) {
        String errorMsg = "Couldn't find value for key '" + key + "'.";
        try {
            if (!hadoopProps.getProperty(key).equals(null)) {
                return this.hadoopProps.getProperty(key);
            }
            else {
                throw new NullPointerException(errorMsg);
            }
        } catch (Exception e) {
            TestSession.logger.error(errorMsg);
            throw new NullPointerException(errorMsg);
        }
    }

    /** 
     * 
     * GET HADOOP DEFAULT & CUSTOM CONF DIR FOR A COMPONENT
     * 
     * /

     /**
     * Returns the Hadoop default configuration directory path.
     * 
     * @return String of the directory path name..
     */
    public String getDefaultHadoopConfDir() {
        String confDir = this.defaultHadoopConfDir;
        if ((confDir == null) || (confDir.isEmpty())) {
            TestSession.logger.error("Default Hadoop conf dir for '" +
                    this.component + "' host '" +
                    this.hostname + "' is undefined!!!");
        } else {
            TestSession.logger.trace("Default Hadoop conf dir for '" +
                    this.component + "' host '" +
                    this.hostname + "'='" + confDir + "'.");    
        }
        return confDir;
    }
    
    
    /**
     * Returns the Hadoop configuration directory path.
     * 
     * @return String of the directory path name..
     */
    public String getHadoopConfDir() {
        String confDir = this.hadoopConfDir;
        if ((confDir == null) || (confDir.isEmpty())) {
            confDir = TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR");
            TestSession.logger.error("Hadoop conf dir for '" +
                    this.component + "' host '" +
                    this.hostname + "' is undefined!!!. Use installed " +
                    "conf dir '" + confDir + "'.");
        } else {
            TestSession.logger.trace("Hadoop conf dir for '" +
                    this.component + "' host '" +
                    this.hostname + "'='" + confDir + "'.");
        }
        return confDir;
    }

    /** 
     * 
     * SET HADOOP DEFAULT & CUSTOM CONF DIR FOR A COMPONENT
     * 
     * /

    /**
     * Set the Hadoop configuration directory path.
     * 
     * @param path String of the directory path name.
     */
    public void setHadoopConfDir(String confDir) {
        this.hadoopConfDir = confDir;
    }

    /**
     *  Reset Hadoop configuration directory to the installed default
     */
    public void resetHadoopConfDir() throws IOException {
        String installedConfiDir = 
                TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR");
        this.setHadoopConfDir(installedConfiDir);
    }

     /**
     * Set the default Hadoop configuration directory path.
     * 
     * @param path String of the directory path name.
     */
    public void setDefaultHadoopConfDir(String confDir) throws IOException {        
        TestSession.logger.info("Set custom default Hadoop conf dir to '" +
                confDir + "'.");
        this.defaultHadoopConfDir = confDir;
        
        /* Enable persistence across TestSession instances:
         * Allow default Hadoop conf dir to be override by subsequent tests by
         * saving the custom default Hadoop conf dir path in a place holder
         * file. 
         */
        String customDefaultConfSettingsFile =
                HadoopCluster.getDefaultConfSettingsFile(
                        this.component, this.hostname);                
        TestSession.logger.info("Set custom default Hadoop conf dir " +
                "persistent tag='" + customDefaultConfSettingsFile + "'.");
        FileUtils.writeStringToFile(
                new File(customDefaultConfSettingsFile), confDir);
    }

    /**
     *  Reset default Hadoop configuration directory to the installed default
     */
    public void resetHadoopDefaultConfDir() throws IOException {
        String installedConfiDir = 
                TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR");
        this.setDefaultHadoopConfDir(installedConfiDir);
        
        // Remove the persistent default conf dir override file.
        String customDefaultConfDir = 
                HadoopCluster.getDefaultConfSettingsFile(
                        this.component, this.hostname);
        TestSession.logger.info("Remove custom default Hadoop conf dir " + 
                "persistent tag from '" + customDefaultConfDir + "'.");
        TestSession.logger.info("Re-set custom default Hadoop conf dir " +
                "back to installed conf dir: '" + installedConfiDir + "'.");
        File file = new File(customDefaultConfDir);
        file.delete();
    }

    /** 
     * 
     * Security
     * 
     * /

    /**
     * Setup the Kerberos configuration for the given user name and keytab file
     * in the parent class Apache Hadoop Configuration object. This will be
     * needed later for tasks such as job submission. 
     */
    private void setKerberosConf(String user) {
        String userName = "user-" + user;
        String userValue = user + "@DEV.YGRID.YAHOO.COM";
        String keytabName = "keytab-" + user;
        String keytabValue = "/homes/" + user + "/" + user +
                ".dev.headless.keytab";
        TestSession.logger.trace("User name/value = " + userName + "/" +
                userValue); 
        TestSession.logger.trace("Keytab name/value = " + keytabName + "/" +
                keytabValue);
        super.set(userName, userValue);
        super.set(keytabName, keytabValue);
    }
        
    /**
     * Setup the Kerberos configuration for all headless users in the
     * parent class Apache Hadoop Configuration object. This will be
     * needed later for tasks such as job submission. 
     */
    private void setKerberosConf() {
        // Setup the headless users
        String[] users = {"hadoopqa", "hdfs", "hdfsqa", "mapred", "mapredqa"};
        for (String user : users ) {
            this.setKerberosConf(user);
        }
        // Setup the headless users hadoop1 through hadoop20
        for(int i = 0; i < 20; i++) {
            this.setKerberosConf("hadoop" + (i+1));
        }
    }

    
    /**
     * Returns the version of the fully distributed Hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * @throws Exception if there is a fatal error getting the version via 
     *                   the CLI for a pseudodistributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() throws Exception {
        String version = null;
        
        String strClusterType = TestSession.conf.getProperty("CLUSTER_TYPE");
                
        if (strClusterType.equals(HadoopCluster.PD_CLUSTER_TYPE)) {
            version = this.getVersionViaCLI();
        } else {
            version = VersionInfo.getVersion();
            TestSession.logger.trace("Hadoop version = '" + 
                                     VersionInfo.getVersion() + "'");
            TestSession.logger.trace("Hadoop build version = '" + 
                                     VersionInfo.getBuildVersion() + "'");
            TestSession.logger.trace("Hadoop revision = '" + 
                                     VersionInfo.getRevision() + "'");
        }
                
        return version;
    }

    /**
     * Returns the version of the fully distributed Hadoop cluster being used 
     * via the command line interface.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * @throws Exception if there is a fatal error running the process that gets the
     *         version.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersionViaCLI() 
        throws Exception {
        String[] cmd = { this.getHadoopProp("HADOOP_BIN"),
                         "--config", this.getHadoopConfDir(), "version" };      
        String version =
                (TestSession.exec.runProcBuilder(cmd))[1].split("\n")[0];
        return version.split(" ")[1];
    }
    
    /**
     * Converts the configuration instance resources and properties to a string.
     * 
     * @param instance the configuration instance.
     * @return the configuration resources and properties.
     */
    public String toString(String instance) {
        if (instance.equals("resources")) {
            return "Conf Resources: " + super.toString();
        } else if (instance.equals("props")) {
            return "Conf Props: " + super.getProps().toString();                                
        }
        else {
            return "";
        }
    }

}
