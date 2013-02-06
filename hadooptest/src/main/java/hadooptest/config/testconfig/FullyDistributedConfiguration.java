/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.File;
import java.io.PrintWriter;
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
    protected Properties hadoopConfDir = new Properties();

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

	public Properties getHadoopProps() {
    	return this.hadoopProps;
    }

	public Properties getHadoopConfDirProps() {
    	return this.hadoopConfDir;
    }

	public Hashtable<String, Properties> getHadoopConfFileProps() {
		return this.getHadoopConfFileProps("gateway");
    }
	
	public Hashtable<String, Properties> getHadoopConfFileProps(String component) {
		return this.hadoopComponentConfFileProps.get(component);
    }
	
	public Hashtable<String, String[]> getClusterNodes() {
    	return this.clusterNodes;
    }
	
	public String[] getClusterNodes(String component) {
    	return this.clusterNodes.get(component);
    }
	
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
     * Returns the version of the fully distributed hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
    	// Call hadoop version to fetch the version 	
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
		
		hadoopConfDir.setProperty("gateway",
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

    
	private void initConfFiles() {
		initConfFiles(null);
	}

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
					this.parseHadoopConf(confDir + "/" +
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

	public void initComponentsConfFiles() {
		initComponentsConfFiles(hadoopProps.getProperty("HADOOP_CONF_DIR"));
	}
	
	public void initComponentsConfFiles(String confDir) {
		  String[] components = {
				  "gateway",
				  "namenode", 
				  "resourcemanager" };
		  for (String component : components) {
			  initComponentConfFiles(confDir, component);
		  }
	}

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


	public void initClusterNodes() {
		// Nodes
		
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

	// Change specific property of xml files
	
	// tmp_conf_dir
	// gateway_conf
	// backupConfDir (String hosts)

	public int copyToConfDir (String component, String sourceDir) {
		return 0;
	}
	
	public String getHadoopConfDir(String component) {
		return this.hadoopConfDir.getProperty(component);
	}
	
	
	public void setHadoopConfProp (String propName, String propValue,
			String component, String confFilename) {		
	    setHadoopConfProp(propName, propValue, component, confFilename,
	    		this.hadoopConfDir.getProperty(component));
	}
	
	public void setHadoopConfProp (String propName, String propValue,
			String confFilename, String component, String confDir) {
		
	}
	
	public String getHadoopConfFileProp(String propName, String confFilename,
			String component) {
		Hashtable<String, Properties> hadoopConfFileProps = getHadoopConfFileProps(component);		
		Properties prop = hadoopConfFileProps.get(confFilename);
		Properties metadata = hadoopConfFileProps.get("metadata");
		TestSession.logger.trace("Get property '" +
				propName + "' defined in '" + 
				metadata.getProperty("path") + "/" + confFilename +
				"' on the '" + component + "' host: ");
		String propValue = prop.getProperty(propName);
		return propValue;
	}
	
	public String getHadoopConfFileProp(String propName, String confFilename) {
		return getHadoopConfFileProp(propName, confFilename, "gateway");
	}
	
	
	private String[] getHostsFromList(String namenode, String file) {
		String[] output = TestSession.exec.runProcBuilder(
						new String[] {"ssh", namenode, "/bin/cat", file});
		String[] nodes = output[1].replaceAll("\\s+", " ").trim().split(" ");
		TestSession.logger.trace("Hosts in file are: " + Arrays.toString(nodes));
		return nodes;
	}
	
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
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public Properties parseHadoopConf(String confFilename) {
		TestSession.logger.debug("Parse Hadoop configuration file: " +
				confFilename);
		Properties props = new Properties();
		try {
			File xmlInputFile = new File(confFilename);
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

	
	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}
	
	// Setup a new temporary hadoop configuration directory where settings can
	// be customized for testing.  This is necessary because tests are being 
	// run as the headless user hadoopqa and do not not sudo permission to edit
	// files in the default configuration directory. 
	public int backupConfDir (String component) {
		if (component.equals("gateway")) {
			return backupConfDir(component, null);			
		}
		else {
			return backupConfDir(component, this.getClusterNodes(component));
		}
	}
	
	// Setup a new temporary hadoop configuration directory where settings can
	// be customized for testing.  This is necessary because tests are being 
	// run as the headless user hadoopqa and do not not sudo permission to edit
	// files in the default configuration directory. 
	public int backupConfDir (String component, String[] daemonHost) {		
		if (this.hadoopConfDir.getProperty(component) != null) {
			TestSession.logger.warn("Override existing backup Hadoop " + 
					"configuration directory '" +
					this.hadoopConfDir.getProperty(component) + "':");
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
	    this.hadoopConfDir.setProperty(component, tmpConfDir);
	    
		// Follow and dereference symlinks
		String cpCmd[] = {"/bin/cp", "-rfL", 
				this.getHadoopProp("HADOOP_CONF_DIR"), tmpConfDir};

		if ((!component.equals("gateway")) && (daemonHost == null)) {
			daemonHost = this.getClusterNodes(component); 
		}

		String[] cmd;
		if (!component.equals("gateway")) {
			TestSession.logger.info("Backup the Hadoop configuration directory on " +
					"the " + component + " host(s) of " + Arrays.toString(daemonHost));
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
		
		// ?? reset component
		
		// not here
		// initComponentConfFiles(tmpConfDir, component);

		
		
		// resetConfDir

		
		
		// Change all the xml config files to have the proper permission so that
		// they can be changed by the tests later.
		// my @command = ('ssh', $target_host, 'chmod', '644',
	//"$new_conf_dir/*.xml");
		//    note("@command");
		//    `@command`;

		    
		    
		    
		
		
		return Integer.parseInt(output[0]);
	}


}
