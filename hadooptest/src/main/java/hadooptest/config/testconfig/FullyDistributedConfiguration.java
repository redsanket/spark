/*
 * YAHOO!
 */

package hadooptest.config.testconfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
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

	// General Hadoop configuration properties such as cluster name, 
	// directory paths, etc.	
    protected Properties hadoopProps = new Properties();
    
    // Contains configuration properties loaded from the xml conf file for 
    // each Hadoop components
	Hashtable<String, Properties> hadoopConfFileProps =
			new Hashtable<String, Properties>();

	// Contains the nodes on the cluster
	Hashtable<String, String[]> clusterNodes = new Hashtable<String, String[]>();
    
	/*
	 * Class constructor.
	 * 
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.  Hadoop
	 * default configuration is not used.
	 */
	public FullyDistributedConfiguration() {
		super(false);
		this.initDefaults();
		this.initConfFiles();
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
		this.initConfFiles();
		this.initClusterNodes();

	}

	public Properties getHadoopProps() {
    	return hadoopProps;
    }

	public Hashtable<String, Properties> getHadoopConfFileProps() {
    	return hadoopConfFileProps;
    }
	
	public Hashtable<String, String[]> getClusterNodes() {
    	return clusterNodes;
    }
	
	public String[] getClusterNodes(String component) {
    	return clusterNodes.get(component);
    }
	
	public String getHadoopProp(String key) {
    	if (!hadoopProps.getProperty(key).equals(null)) {
    		return hadoopProps.getProperty(key);
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
	
	public String getHadoopConfFileProp(String component, String propName) {
		Properties prop = hadoopConfFileProps.get(component);
		String propValue = prop.getProperty(propName);
		return propValue;
	}
	
	/*
	 * Initializes a set of default configuration properties that have been 
	 * determined to be a reasonable set of defaults for running a distributed
	 * cluster under test.
	 */
	private void initDefaults() {
		String HADOOP_ROOT="/home";  // /grid/0								
		hadoopProps.setProperty("CLUSTER_NAME", TestSession.conf.getProperty("CLUSTER_NAME", ""));
		hadoopProps.setProperty("TMP_DIR", TestSession.conf.getProperty("TMP_DIR", "/grid/0/tmp"));
		hadoopProps.setProperty("JAVA_HOME", HADOOP_ROOT+"/gs/java/jdk");
		hadoopProps.setProperty("HADOOP_INSTALL", HADOOP_ROOT + "/gs/gridre/yroot." +
				hadoopProps.getProperty("CLUSTER_NAME"));
		hadoopProps.setProperty("HADOOP_COMMON_HOME", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/share/hadoop");

		// Configuration directory and files
		hadoopProps.setProperty("HADOOP_CONF_DIR", hadoopProps.getProperty("HADOOP_INSTALL") +
				"/conf/hadoop");
		hadoopProps.setProperty("HADOOP_CONF_CORE",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/core-site.xml");
		hadoopProps.setProperty("HADOOP_CONF_HDFS",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/hdfs-site.xml");
		hadoopProps.setProperty("HADOOP_CONF_MAPRED",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/mapred-site.xml");
		hadoopProps.setProperty("HADOOP_CONF_YARN",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/yarn-site.xml");
		hadoopProps.setProperty("HADOOP_CONF_CAPACITY_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/capacity-scheduler.xml");
		hadoopProps.setProperty("HADOOP_CONF_FAIR_SCHEDULER",
				hadoopProps.getProperty("HADOOP_CONF_DIR") + "/fair-scheduler.xml");

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
		// Configuration
		String[] confComponents = {
				"HADOOP_CONF_CORE",
				"HADOOP_CONF_HDFS",
				"HADOOP_CONF_MAPRED",
				"HADOOP_CONF_YARN",
				"HADOOP_CONF_CAPACITY_SCHEDULER", 
				"HADOOP_CONF_FAIR_SCHEDULER"
				};
		hadoopConfFileProps =
				new Hashtable<String, Properties>();
		for (int i = 0; i < confComponents.length; i++) {
			String confComponent = confComponents[i];
			hadoopConfFileProps.put(confComponent,
					this.parseHadoopConf(getHadoopProp(confComponent)));
		}

		// Print the stored conf properties
		/*
		for (int i = 0; i < confComponents.length; i++) {
			String confComponent = confComponents[i];
			Properties prop = hadoopConfFileProps.get(confComponent);
			TSM.logger.debug("Parsed Hadop configuration file for " + confComponent + ":");
			printProp(prop);
		}
		*/		
	}

	private void initClusterNodes() {
		// Nodes
		
		clusterNodes.put("ADMIN_HOST", new String[] {
				"adm102.blue.ygrid.yahoo.com",
				"adm103.blue.ygrid.yahoo.com"});

		// Namenode
		String namenode_addr = getHadoopConfFileProp("HADOOP_CONF_HDFS",
				"dfs.namenode.https-address");
		String namenode = namenode_addr.split(":")[0];
		clusterNodes.put("namenode", new String[] {namenode});		

		// Resource Manager
		String rm_addr = getHadoopConfFileProp("HADOOP_CONF_YARN",
				"yarn.resourcemanager.resource-tracker.address");
		String rm = rm_addr.split(":")[0];
		clusterNodes.put("resourcemanager", new String[] {rm});		
		
		// Datanode
		clusterNodes.put("datanode", getHostsFromList(namenode,
				getHadoopProp("HADOOP_CONF_DIR") + "/slaves"));		
		
		// Nodemanager
		clusterNodes.put("nodemanager", clusterNodes.get("datanode"));		

		// Show all balances in hash table. 
		Enumeration<String> components = clusterNodes.keys(); 
		while (components.hasMoreElements()) { 
			String component = (String) components.nextElement(); 
			TestSession.logger.debug(component + ": " + Arrays.toString(clusterNodes.get(component))); 
		} 	
	}

	// tmp_conf_dir
	// gateway_conf
	// backupConfDir (String hosts)
	
	public int backupConfDir (String component, String[] daemonHost) {
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
	    df.setTimeZone(TimeZone.getTimeZone("CST"));  
	    String tmpConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-" +
	    		df.format(new Date());
		this.hadoopProps.setProperty("TMP_CONF_DIR", tmpConfDir);
		
		String cpCmd[] = {"/bin/cp", "-rf", 
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
		// resetConfDir

		return Integer.parseInt(output[0]);
	}

	
	private String[] getHostsFromList(String namenode, String file) {
		String[] output = TestSession.exec.runProcBuilder(
						new String[] {"ssh", namenode, "/bin/cat", file});
		String[] nodes = output[1].replaceAll("\\s+", " ").trim().split(" ");
		TestSession.logger.debug("Hosts are: " + Arrays.toString(nodes));
		return nodes;
	}
	
	private void printProp(Properties prop) {
		PrintWriter writer = new PrintWriter(System.out);
		prop.list(writer);
		writer.flush();	      	
	}

	
	/*
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 */
	public Properties parseHadoopConf(String confFile) {
		TestSession.logger.info("Parse Hadoop configuration file: " + confFile);
		Properties props = new Properties();
		try {
			File xmlInputFile = new File(confFile);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbFactory.newDocumentBuilder();
			Document doc = db.parse(xmlInputFile);
			doc.getDocumentElement().normalize();			
			System.out.println("root of xml file: " + doc.getDocumentElement().getNodeName());			
			NodeList nodes = doc.getElementsByTagName("property");
						
			for (int index = 0; index < nodes.getLength(); index++) {
				Node node = nodes.item(index);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;

					String propName = getValue("name", element);
					try {
						// TSM.logger.debug("Property Name: " + getValue("name", element));
						// TSM.logger.debug("Property Value: " + getValue("value", element));
						props.put(getValue("name", element),
								getValue("value", element));
					}
					catch (NullPointerException npe) {
						TestSession.logger.warn("Value for property name " + propName + 
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
	
}
