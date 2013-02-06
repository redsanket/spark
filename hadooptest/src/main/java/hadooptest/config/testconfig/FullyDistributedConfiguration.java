/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.File;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/*
 * A class that represents a Hadoop Configuration for a distributed
 * Hadoop cluster under test.
 */
public class FullyDistributedConfiguration extends TestConfiguration
{
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

	// General Hadoop configuration properties such as cluster name, 
	// directory paths, etc.	
    protected Properties hadoopProps = new Properties();

    // Track Hadoop override configuration directories
    protected Properties hadoopConfDirPaths = new Properties();

    // Contains configuration properties loaded from the xml conf file for 
    // each Hadoop components
	Hashtable<String, Hashtable<String, Properties>> hadoopComponentConfFileProps =
			new Hashtable<String, Hashtable<String, Properties>>();

	// Contains the nodes on the cluster
	Hashtable<String, String[]> clusterNodes = new Hashtable<String, String[]>();
    
	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.
	 * Hadoop default configuration is not used.
	 */
	public FullyDistributedConfiguration() {
		super(false);
		this.initDefaults();
		this.initComponentConfFiles(
				hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR"), "gateway");	
		this.initClusterNodes();
	}

	/*
	 * Class constructor.
	 * 
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * distributed test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 */
	public FullyDistributedConfiguration(boolean loadDefaults) {
		super(loadDefaults);
		this.initDefaults();
		this.initComponentConfFiles(
				hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR"), "gateway");
		this.initClusterNodes();

	}

    /*
     * Returns the Hadoop properties.
     * 
     * @return Properties the Hadoop general properties such as cluster name,
     * directory paths, etc. 
     */
	public Properties getHadoopProps() {
    	return this.hadoopProps;
    }

    /*
     * Returns the Hadoop general property value for a given property name.
     * 
     * @return String property value such as cluster name, directory paths, etc. 
     */
	public String getHadoopProp(String key) {
    	if (!hadoopProps.getProperty(key).equals(null)) {
    		return this.hadoopProps.getProperty(key);
    	}
    	else {
			TestSession.logger.error("Couldn't find value for key '" + key + "'.");
			return "";
    	}
    }

	/*
     * Returns the Hadoop configuration directory paths.
     * 
     * @return Properties the Hadoop configuration directory paths by
     * components.
     */
	public Properties getHadoopConfDirPaths() {
    	return this.hadoopConfDirPaths;
    }

    /*
     * Returns the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String of the directory path name..
     */
	public String getHadoopConfDirPath(String component) {
		return this.getHadoopConfDirPaths().getProperty(component);
	}
	
    /*
     * Set the Hadoop configuration directory path for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * @param path String of the directory path name.
     * 
     */
	public void setHadoopConfDirPath(String component, String path) {
		this.getHadoopConfDirPaths().setProperty(component, path);
	}

    /*
     * Returns the Hadoop configuration files properties hashtable for the
     * gateway.
     * 
     * @return Hashtable of Properties for each configuration files on the
     * gateway.
     */
	public Hashtable<String, Properties> getHadoopConfFileProps() {
		return this.getHadoopConfFileProps("gateway");
    }
	
    /*
     * Returns the Hadoop configuration files properties hashtable for the
     * given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return Hashtable of Properties for each configuration files on the
     * component host.
     */
	public Hashtable<String, Properties> getHadoopConfFileProps(
			String component) {
		return this.hadoopComponentConfFileProps.get(component);
    }
	
	/*
     * Returns the Hadoop configuration files property for a given property
     * name and configuration filename on the gateway component. 
     * 
     * @param propname the Hadoop configuration property name
     * @param confFilename the Hadoop configuration file name
     * 
     * @return String property value for the given property name and the
     * configuration file on the gateway host.
     */
	public String getHadoopConfFileProp(String propName, String confFilename) {
		return getHadoopConfFileProp(propName, confFilename, "gateway");
	}
	
	/*
     * Returns the Hadoop configuration files property for a given property
     * name, configuration filename, and component. 
     * 
     * @param propname the Hadoop configuration property name
     * @param confFilename the Hadoop configuration file name
     * @param component the Hadoop cluster component name
     * 
     * @return String property value for the given property name and the
     * configuration file on the component host.
     */
	public String getHadoopConfFileProp(String propName, String confFilename,
			String component) {
		Hashtable<String, Properties> hadoopConfFileProps =
				getHadoopConfFileProps(component);		
		Properties prop = hadoopConfFileProps.get(confFilename);
		Properties metadata = hadoopConfFileProps.get("metadata");
		TestSession.logger.trace("Get property '" +
				propName + "' defined in '" + 
				metadata.getProperty("path") + "/" + confFilename +
				"' on the '" + component + "' host: ");
		String propValue = prop.getProperty(propName);
		return propValue;
	}
	
    /*
     * Returns the Hadoop cluster hostnames hashtable.
     * 
     * @return Hashtable of String Arrays hostnames for each of the cluster
     * components.
     */
	public Hashtable<String, String[]> getClusterNodes() {
    	return this.clusterNodes;
    }
	
    /*
     * Returns the cluster nodes hostnames for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String Arrays for the cluster nodes hostnames.
     */
	public String[] getClusterNodes(String component) {
    	return this.clusterNodes.get(component);
    }
	
	/*
     * Returns the version of the fully distributed Hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
    	String[] cmd = { this.getHadoopProp("HADOOP_BIN"),
    			"--config", this.getHadoopProp("HADOOP_CONF_DIR"), "version" };	
    	String version = (TestSession.exec.runProcBuilder(cmd))[1].split("\n")[0];
        return version.split(" ")[1];
    }
	
	/*
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public void write() {
		/*
		String confDir = this.getHadoopProp("HADOOP_CONF_DIR");
		File outdir = new File(confDir);
		outdir.mkdirs();
		
		File historytmp = new File(confDir + "jobhistory/tmp");
		historytmp.mkdirs();
		File historydone = new File(confDir + "jobhistory/done");
		historydone.mkdirs();

		File core_site = new File(confDir + "core-site.xml");
		File hdfs_site = new File(confDir + "hdfs-site.xml");
		File yarn_site = new File(confDir + "yarn-site.xml");
		File mapred_site = new File(confDir + "mapred-site.xml");		

		try{
			if (core_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(core_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (hdfs_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(hdfs_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (yarn_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(yarn_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			if (mapred_site.createNewFile()) {
				FileOutputStream out = new FileOutputStream(mapred_site);
				this.writeXml(out);
			}
			else {
				TestSession.logger.warn("Couldn't create the xml configuration output file.");
			}

			FileWriter slaves_file = new FileWriter(confDir + "slaves");
			BufferedWriter slaves = new BufferedWriter(slaves_file);
			slaves.write("localhost");
			slaves.close();
		}
		catch (IOException ioe) {
			TestSession.logger.error("There was a problem writing the hadoop configuration to disk.");
		}
		*/
	}

	/*
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 */
	public void cleanup() {
		/*
		String confDir = this.getHadoopProp("HADOOP_CONF_DIR");
		File core_site = new File(confDir + "core-site.xml");
		File hdfs_site = new File(confDir + "hdfs-site.xml");
		File yarn_site = new File(confDir + "yarn-site.xml");
		File mapred_site = new File(confDir + "mapred-site.xml");	
		File slaves = new File(confDir + "slaves");	
		File log4jProperties = new File(confDir + "log4j.properties");

		core_site.delete();
		hdfs_site.delete();
		yarn_site.delete();
		mapred_site.delete();
		slaves.delete();
		log4jProperties.delete();
		*/
	}
	
	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {
		String HADOOP_ROOT="/home";  // /grid/0								
		hadoopProps.setProperty("CLUSTER_NAME", TestSession.conf.getProperty("CLUSTER_NAME", ""));

		try {
			hadoopProps.setProperty("GATEWAY", InetAddress.getLocalHost().getHostName());
		}
		catch (Exception e) {
			TestSession.logger.warn("Hostname not found!!!");
		}
		
		// hadoopProps.setProperty("TMP_DIR", TestSession.conf.getProperty("TMP_DIR", "/grid/0/tmp"));
		hadoopProps.setProperty("TMP_DIR", TestSession.conf.getProperty("TMP_DIR", "/homes/hadoopqa/tmp/hadooptest"));
		
		hadoopProps.setProperty("JAVA_HOME", HADOOP_ROOT+"/gs/java/jdk");
		hadoopProps.setProperty("HADOOP_INSTALL", HADOOP_ROOT + "/gs/gridre/yroot." +
				hadoopProps.getProperty("CLUSTER_NAME"));
		hadoopProps.setProperty("HADOOP_COMMON_HOME", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/share/hadoop");

		// Configuration directory and files
		hadoopProps.setProperty("HADOOP_CONF_DIR", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/conf/hadoop");
		hadoopProps.setProperty("HADOOP_DEFAULT_CONF_DIR",
				hadoopProps.getProperty("HADOOP_CONF_DIR"));
		
		this.setHadoopConfDirPath("gateway",
				hadoopProps.getProperty("HADOOP_CONF_DIR"));
		
		hadoopProps.setProperty("HADOOP_CONF_CORE",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_CORE);
		hadoopProps.setProperty("HADOOP_CONF_HDFS",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_HDFS);
		hadoopProps.setProperty("HADOOP_CONF_MAPRED",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_MAPRED);
		hadoopProps.setProperty("HADOOP_CONF_YARN",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_YARN);
		hadoopProps.setProperty("HADOOP_CONF_CAPACITY_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_CAPACITY_SCHEDULER);
		hadoopProps.setProperty("HADOOP_CONF_FAIR_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/" + HADOOP_CONF_FAIR_SCHEDULER);

		// Binaries
		hadoopProps.setProperty("HADOOP_BIN_DIR", hadoopProps.getProperty("HADOOP_COMMON_HOME") + "/bin");
		hadoopProps.setProperty("HADOOP_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hadoop");
		hadoopProps.setProperty("HDFS_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/hdfs");
		hadoopProps.setProperty("MAPRED_BIN", hadoopProps.getProperty("HADOOP_BIN_DIR") + "/mapred");
		hadoopProps.setProperty("YARN_BIN", getHadoopProp("HADOOP_BIN_DIR") + "/yarn");

		// Version dependent environment variables
		String HADOOP_VERSION = this.getVersion();

		// String HADOOP_VERSION = "23.6";
		hadoopProps.setProperty("HADOOP_VERSION", HADOOP_VERSION);
		
		// Jars
		hadoopProps.setProperty("HADOOP_JAR_DIR", getHadoopProp("HADOOP_COMMON_HOME") +
				"/share/hadoop");
		hadoopProps.setProperty("HADOOP_SLEEP_JAR", getHadoopProp("HADOOP_JAR_DIR") + 
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
	}

    
    /*
     * Initialize the Hadoop configuration files. 
     * 
     * @return Hashtable of Properties for each of the configuration files.
     */
	private void initConfFiles() {
		initConfFiles(null);
	}
	
    /*
     * Initialize the Hadoop configuration files. 
     * 
     * @param confDir the configuration directory path.
     * 
     * @return hastable of Properties for each of the configuration files.
     */
	private Hashtable<String, Properties> initConfFiles(String confDir) {
		// Contains configuration properties loaded from the xml conf file for 
	    // each Hadoop components
		Hashtable<String, Properties> hadoopConfFileProps =
				new Hashtable<String, Properties>();

		if ((confDir == null) || confDir.isEmpty()) {
			confDir = hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR");
		}
		
		// Configuration Files
		String[] confFilenames = {
				HADOOP_CONF_CORE,
				HADOOP_CONF_HDFS,
				HADOOP_CONF_MAPRED,
				HADOOP_CONF_YARN,
				HADOOP_CONF_CAPACITY_SCHEDULER, 
				HADOOP_CONF_FAIR_SCHEDULER
				};

		for (int i = 0; i < confFilenames.length; i++) {
			String confFilename = confFilenames[i];
			hadoopConfFileProps.put(confFilename,
					this.parseHadoopConfFile(confDir + "/" +
							confFilename));
		}

		// Print the stored configuration properties
		for (int i = 0; i < confFilenames.length; i++) {
			String confFilename = confFilenames[i];
			Properties prop = hadoopConfFileProps.get(confFilename);
			TestSession.logger.trace("List the parsed Hadop configuration " + 
					"properties for " + confFilename + ":");
			traceProperties(prop);
		}

		return hadoopConfFileProps;
	}

    /*
     * Initialize the Hadoop configuration files for each components. 
     */
	public void initComponentsConfFiles() {
		initComponentsConfFiles(hadoopProps.getProperty("HADOOP_CONF_DIR"));
	}
	
    /*
     * Initialize the Hadoop configuration files for each components for the
     * given configuration directory. 
     */
	public void initComponentsConfFiles(String confDir) {
		  String[] components = {
				  "gateway",
				  "namenode", 
				  "resourcemanager" };
		  for (String component : components) {
			  initComponentConfFiles(confDir, component);
		  }
	}

    /*
     * Initialize the Hadoop configuation files for the given configuration
     * directory and the given component. 
     * 
     * @param confDir the Hadoop configuration directory 
     * @param component the cluster component name. 
     */
	public void initComponentConfFiles(String confDir, String component) {
		
		String localConfDir = confDir;
		if (!component.equals("gateway")) {
			String componentHost = clusterNodes.get(component)[0];
			DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
			df.setTimeZone(TimeZone.getTimeZone("CST"));  
			localConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-" +	
					component + "-" + df.format(new Date());
			
			if ((confDir == null) || confDir.isEmpty()) {
				confDir = hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR");
			}
			String[] cmd = {"/usr/bin/scp", "-r", componentHost + ":" + confDir, localConfDir};
			String[] output = TestSession.exec.runProcBuilder(cmd);
		}

		Hashtable<String, Properties> componentConfProps = initConfFiles(localConfDir);
		Properties metadata = new Properties();
		metadata.setProperty("path", confDir);
		componentConfProps.put("metadata", metadata);
		hadoopComponentConfFileProps.put(component, componentConfProps);
		
		Hashtable<String, Properties> configs = hadoopComponentConfFileProps.get(component);
		Enumeration<String> enumKey = configs.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
		    Properties prop = configs.get(key);
		    	TestSession.logger.trace("List hadoop config properties for config file " + key);
		    	traceProperties(prop);
		}
	}		

    /*
     * Initialize the cluster nodes hostnames for the namenode,
     * resource manager, datanode, and nodemanager. 
     */
	public void initClusterNodes() {
		clusterNodes.put("admin", new String[] {
				"adm102.blue.ygrid.yahoo.com",
				"adm103.blue.ygrid.yahoo.com"});

		// Namenode
		String namenode_addr = getHadoopConfFileProp("dfs.namenode.https-address",
				HADOOP_CONF_HDFS);
		String namenode = namenode_addr.split(":")[0];
		clusterNodes.put("namenode", new String[] {namenode});		

		// Resource Manager
		String rm_addr = getHadoopConfFileProp(
				"yarn.resourcemanager.resource-tracker.address",
				HADOOP_CONF_YARN);
		String rm = rm_addr.split(":")[0];
		clusterNodes.put("resourcemanager", new String[] {rm});		
		
		// Datanode
		clusterNodes.put("datanode", getHostsFromList(namenode,
				getHadoopProp("HADOOP_CONF_DIR") + "/slaves"));		
		
		// Nodemanager
		clusterNodes.put("nodemanager", clusterNodes.get("datanode"));		

		// Show all balances in hash table. 
		TestSession.logger.debug("-- listing cluster nodes --");
		Enumeration<String> components = clusterNodes.keys(); 
		while (components.hasMoreElements()) { 
			String component = (String) components.nextElement(); 
			TestSession.logger.debug(component + ": " + Arrays.toString(clusterNodes.get(component))); 
		} 	
	}

    /*
     * Parse the host names from a host name list on the namenode.
     * 
     * @param namenode the namenode hostname. 
     * @param file the file name. 
     * 
     * @return String Array of host names.
     */
	private String[] getHostsFromList(String namenode, String file) {
		String[] output = TestSession.exec.runProcBuilder(
						new String[] {"ssh", namenode, "/bin/cat", file});
		String[] nodes = output[1].replaceAll("\\s+", " ").trim().split(" ");
		TestSession.logger.trace("Hosts in file are: " + Arrays.toString(nodes));
		return nodes;
	}
	
    /*
     * Print logging for the given Properties object.
     * 
     * @param Properties a given Properties
     */
	private void traceProperties(Properties prop) {
		TestSession.logger.trace("-- listing properties --");
		Enumeration<Object> keys = prop.keys();
		while (keys.hasMoreElements()) {
		  String key = (String)keys.nextElement();
		  String value = (String)prop.get(key);
		  TestSession.logger.trace(key + ": " + value);
		}
	}
		
    /*
     * Parse Hadoop configuration file for a given filename.
     * 
     * @param filename Hadoop configuration file name such as core-site.xml,
     * hdfs-site.xml, etc.
     * 
     * @return Properties for the given configuration files.
     */
	public Properties parseHadoopConfFile(String filename) {
		TestSession.logger.debug("Parse Hadoop configuration file: " +
				filename);
		Properties props = new Properties();
		try {
			File xmlInputFile = new File(filename);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbFactory.newDocumentBuilder();
			Document doc = db.parse(xmlInputFile);
			doc.getDocumentElement().normalize();
			TestSession.logger.trace("Root of xml file: " +
					doc.getDocumentElement().getNodeName());			
			NodeList nodes = doc.getElementsByTagName("property");
						
			for (int index = 0; index < nodes.getLength(); index++) {
				Node node = nodes.item(index);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;

					String propName = getValue("name", element);
					try {
						TestSession.logger.trace("Config Property Name: " +
								getValue("name", element));
						TestSession.logger.trace("Config Property Value: " +
								getValue("value", element));
						props.put(getValue("name", element),
								getValue("value", element));
					}
					catch (NullPointerException npe) {
						TestSession.logger.trace("Value for property name " + propName + 
								" is null");
					}
				}
			}
		}	
		catch (Exception exception)
		{
			exception.printStackTrace();
		}
		return props;
	}

    /*
     * Get the xml node value given a tag and an element. 
     * 
     * @param tag Tag name in the xml element
     * @param element The xml element.
     * 
     * @return String node value.
     */
	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}
	
	
	
	
	
	// Change specific property of xml files
	// tmp_conf_dir
	// gateway_conf
	// backupConfDir (String hosts)

	
	
	
    /*
     * Set the Hadoop configuration file property for a given property name,
     * property value, component, and file name. 
     * 
     * @param propName String of property name
     * @param propValue String of property value
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param fileName String of the configuration file name.
     * 
     * @return int 0 for success, 1 for failure.
     */
	public void setHadoopConfProp (String propName, String propValue,
			String component, String confFilename) {
	    setHadoopConfProp(propName, propValue, component, confFilename,
	    		this.getHadoopConfDirPath(component));
	}
	
    /*
     * Set the Hadoop configuration file property for a given property name,
     * property value, component, file name, and configuration directory path.
     * 
     * @param propName String of property name
     * @param propValue String of property value
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param fileName String of the configuration file name.
     * @param confDir String of the configuration directory path.
     * 
     * @return int 0 for success, 1 for failure.
     */
	public void setHadoopConfProp (String propName, String propValue,
			String component, String confFilename, String confDir) {

		// Either
		
	}

	
    /*
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceDir source configuration directory to copy the files from
     * 
     * @return int 0 for success, 1 for failure.
     */
	public int copyFilesToConfDir (String component, String sourceDir) {
		if (component.equals("gateway")) {
			return copyFilesToConfDir(component, sourceDir, null);			
		}
		else {
			return copyFilesToConfDir(component, sourceDir,
					this.getClusterNodes(component));
		}
	}

    /*
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceDir source configuration directory to copy the files from
     * @param daemonHost String Array of component hostname(s).
     * 
     * @return int 0 for success, 1 for failure.
     */
	public int copyFilesToConfDir (String component, String sourceDir,
			String[] daemonHost) {
		
		String confDir = this.getHadoopConfDirPath(component);
		String cpCmd[] = { "/usr/bin/scp",
				this.getHadoopProp("GATEWAY") + ":" + sourceDir + "/*",
				confDir};
				
		if ((!component.equals("gateway")) && (daemonHost == null)) {
			daemonHost = this.getClusterNodes(component); 
		}

		String[] cmd;
		if (!component.equals("gateway")) {
			TestSession.logger.info("Copy files to the Hadoop " +
					"configuration directory " + confDir + " on " +
					"the " + component + " host(s) of " +
					Arrays.toString(daemonHost));
			String[] pdshCmd = { "/home/y/bin/pdsh", "-w",
					StringUtils.join(daemonHost, ",") };			
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
			cmd = temp.toArray(new String[pdshCmd.length+cpCmd.length]);
		}
		else {
			TestSession.logger.info("Back up the Hadoop configuration directory on " +
					"the gateway:");
			cmd = cpCmd;
		}		
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		return Integer.parseInt(output[0]);
	}

    /*
     * Backup the Hadoop configuration directory for a given component.
     * This will setup a new temporary Hadoop configuration directory where
     * settings can be changed for testing. This is necessary because the
     * default configuration directory is owned and writable only to root. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * 
     * @return int 0 for success, 1 for failure.
     */
	public int backupConfDir (String component) {
		if (component.equals("gateway")) {
			return backupConfDir(component, null);			
		}
		else {
			return backupConfDir(component, this.getClusterNodes(component));
		}
	}
	
    /*
     * Backup the Hadoop configuration directory for a given component.
     * This will setup a new temporary Hadoop configuration directory where
     * settings can be changed for testing. This is necessary because the
     * default configuration directory is owned and writable only to root. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param daemonHost String Array of component hostname(s).
     * 
     * @return int 0 for success, 1 for failure.
     */
	public int backupConfDir (String component, String[] daemonHost) {
		String componentHadoopConfDirPath =
				this.getHadoopConfDirPath(component);
		if (componentHadoopConfDirPath != null) {
			TestSession.logger.warn("Override existing backup Hadoop " + 
					"configuration directory '" +
					componentHadoopConfDirPath + "':");
		}
		else {
			TestSession.logger.warn("Override default Hadoop configuration " + 
					"directory '" + this.getHadoopProp("HADOOP_CONF_DIR") +
					"':");	
		}
		
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
	    df.setTimeZone(TimeZone.getTimeZone("CST"));  
	    String tmpConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-" +
	    		df.format(new Date());

	    // Need to know what host/component is the tmp conf dir setup on?
	    // this.hadoopProps.setProperty("TMP_CONF_DIR", tmpConfDir);
	    this.setHadoopConfDirPath(component, tmpConfDir);
	    
		// Follow and dereference symlinks
		String cpCmd[] = {"/bin/cp", "-rfL", 
				this.getHadoopProp("HADOOP_CONF_DIR"), tmpConfDir + ";"};

		// Change all the xml config files to have the proper permission so that
		// they can be changed by the tests later.
		String[] chmodCmd = {"/bin/chmod", "644", tmpConfDir+"/*.xml"};

		if ((!component.equals("gateway")) && (daemonHost == null)) {
			daemonHost = this.getClusterNodes(component); 
		}

		String[] cmd;
		if (!component.equals("gateway")) {
			TestSession.logger.info("Backup the Hadoop configuration directory to " +
					tmpConfDir + " on " + "the " + component + " host(s) of " +
					Arrays.toString(daemonHost));
			String[] pdshCmd = { "/home/y/bin/pdsh", "-w",
					StringUtils.join(daemonHost, ",") };

			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
			temp.addAll(Arrays.asList(chmodCmd));
			cmd = temp.toArray(new String[pdshCmd.length+cpCmd.length+chmodCmd.length]);
		}
		else {
			TestSession.logger.info("Back up the Hadoop configuration directory on " +
					"the gateway:");
			cmd = cpCmd;
		}		
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));
				
		// The setHadoopConfDirPath() will set the path to the new configuration
		// directory. It is the responsibility for the test to call the 
		// cluster.reset() to restart the component for the configuration change
		// to take effect, and to reinitialize the configuration object (via
		// initComponentConfFiles(tmpConfDir, component))

		return Integer.parseInt(output[0]);
	}

}
