/*
 * YAHOO!
 */

package hadooptest.config.hadoop.fullydistributed;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import hadooptest.config.hadoop.HadoopConfiguration;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * A class that represents a Hadoop Configuration for a distributed
 * Hadoop cluster under test.
 */
public class FullyDistributedConfiguration extends HadoopConfiguration
{
	/**
     * Contains configuration properties loaded from the xml conf file for 
     * each Hadoop component
     * NOTE: this has some overlap with the Hadoop Configuration resources 
     * properties. Here we are keeping track of resource instances on each
     * Hadoop components (gateway, namenode, resourcemanager, etc. in case
     * they diverge. We should evalaute though if this can eventually go away
     * and just be replaced by the Hadoop Configuration resources. 
     */
	private Hashtable<String, Hashtable<String, Properties>>componentResourcesProps =
			new Hashtable<String, Hashtable<String, Properties>>();

	
	/**
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.
	 * Hadoop default configuration is not used.
	 * 
	 * @throws Exception if there is a fatal error loading a resource from the
	 * gateway.
	 */
	public FullyDistributedConfiguration() 
			throws Exception {
		super(false);
		String component = HadoopConfiguration.GATEWAY;
		this.loadResourceForComponent(
		        this.getHadoopConfDir(component), component);
	}

	/**
	 * Loads the Hadoop default configuration if true is passed as a parameter,
	 * before the distributed test cluster default configuration is initialized
	 * into the configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration
	 * parameters specified by the Hadoop installation, before loading the
	 * class configuration defaults.
	 * 
	 * @throws Exception if there is a fatal error loading a resource from the
	 * gateway.
	 */
	public FullyDistributedConfiguration(boolean loadDefaults) 
			throws Exception {
		super(loadDefaults);
        String component = HadoopConfiguration.GATEWAY;
        this.loadResourceForComponent(
                this.getHadoopConfDir(component), component);
	}
	
	/**
	 * Initializes cluster-specific properties defaults.
	 * 
	 * @throws UnknownHostException if we can not get the localhost
	 */
	protected void initDefaultsClusterSpecific() throws UnknownHostException {
		hadoopProps.setProperty("CLUSTER_NAME", 
		        TestSession.conf.getProperty("CLUSTER_NAME", ""));
		hadoopProps.setProperty("GATEWAY",
		        InetAddress.getLocalHost().getHostName());

		String defaultTmpDir = "/homes/hadoopqa/tmp/hadooptest";
		hadoopProps.setProperty("TMP_DIR", 
				TestSession.conf.getProperty("TMP_DIR", defaultTmpDir));
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
		df.setTimeZone(TimeZone.getTimeZone("CST"));  
		String tmpDir = this.getHadoopProp("TMP_DIR") + "/hadooptest-" +	
				df.format(new Date());
		new File(tmpDir).mkdirs();
		hadoopProps.setProperty("TMP_DIR", tmpDir);
		
		String HADOOP_ROOT="/home";  // /grid/0								
		hadoopProps.setProperty("JAVA_HOME", HADOOP_ROOT+"/gs/java/jdk");
		hadoopProps.setProperty("HADOOP_INSTALL", HADOOP_ROOT +
		        "/gs/gridre/yroot." +
				hadoopProps.getProperty("CLUSTER_NAME"));
		hadoopProps.setProperty("HADOOP_COMMON_HOME", 
		        hadoopProps.getProperty("HADOOP_INSTALL") +
				"/share/hadoop");
	}
	
    /**
     * Returns the Hadoop configuration files properties hashtable for the
     * gateway.
     * 
     * @return Hashtable of Properties for each configuration files on the
     * gateway.
     */
	public Hashtable<String, Properties> getResourceProps() {
		return this.getResourceProps("gateway");
    }
	
    /**
     * Returns the Hadoop configuration files properties hashtable for the
     * given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return Hashtable of Properties for each configuration files on the
     * component host.
     */
	public Hashtable<String, Properties> getResourceProps(
			String component) {
		Hashtable<String, Properties> hashtable =
				this.componentResourcesProps.get(component);
		if (hashtable == null) {
			TestSession.logger.warn("Hadoop Configuration File Properties " +
					"for component '" + component + "' is null.");
		}
		return hashtable;
		// return this.hadoopComponentConfFileProps.get(component);
    }
	
	/**
     * Returns the Hadoop configuration files property for a given property
     * name and configuration filename on the gateway component. 
     * 
     * @param propName the Hadoop configuration property name
     * @param confFilename the Hadoop configuration file name
     * 
     * @return String property value for the given property name and the
     * configuration file on the gateway host.
     */
	public String getResourceProp(String propName, String confFilename) {
		return getResourceProp(propName, confFilename, "gateway");
	}
	
	/**
     * Returns the Hadoop configuration files property for a given property
     * name, configuration filename, and component. 
     * 
     * @param propName the Hadoop configuration property name
     * @param confFilename the Hadoop configuration file name
     * @param component the Hadoop cluster component name
     * 
     * @return String property value for the given property name and the
     * configuration file on the component host.
     */
	public String getResourceProp(String propName, String confFilename,
			String component) {
		Hashtable<String, Properties> hadoopConfFileProps =
				this.getResourceProps(component);		
		Properties prop = hadoopConfFileProps.get(confFilename);
		Properties metadata = hadoopConfFileProps.get("metadata");
		TestSession.logger.trace("Get property '" +
				propName + "' defined in '" + 
				metadata.getProperty("path") + "/" + confFilename +
				"' on the '" + component + "' host: ");
		String propValue = prop.getProperty(propName);
		return propValue;
	}
	
	/**
     * Returns if the Hadoop configuration files property has a given property
     * defined for a given configuration filename and component. 
     * 
     * @param propName the Hadoop configuration property name
     * @param confFilename the Hadoop configuration file name
     * @param component the Hadoop cluster component name
     * 
     * @return true or false if the property name exists. 
     */
	public boolean propNameExistsInHadoopConfFile(String propName,
			String confFilename, String component) {
		Hashtable<String, Properties> hadoopConfFileProps =
				this.getResourceProps(component);		
		Properties prop = hadoopConfFileProps.get(confFilename);
		return prop.containsKey(propName);
	}
	
	/**
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 * 
	 * Currently unimplemented for FullyDistributedConfiguration.
	 * 
	 * @throws IOException if there is a problem writing the configuration to
	 * disk.
	 */
	public void write() throws IOException {
		/*
		String confDir = this.getHadoopConfDir();
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
		*/
	}

	/**
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 * 
	 * Currently unimplemented for FullyDistributedConfiguration.
	 */
	public void cleanup() {
		/*
		String confDir = this.getHadoopConfDir();
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
	
    /**
     * Initialize the Hadoop configuration files for each components. 
     * 
     * @throws Exception if there is a fatal error loading resources.
     */
	public void loadResourceForAllComponents() 
			throws Exception {
		loadResourceForAllComponents(null, null);
	}
	
    /**
     * Initialize the Hadoop configuration files for each components for the
     * given configuration directory. 
     * 
     * @throws Exception if there is a fatal error loading resources.
     */
	public void loadResourceForAllComponents(String confDir) 
			throws Exception {
		loadResourceForAllComponents(confDir, null);
	}

    /**
     * Initialize the Hadoop configuration files for each components for the
     * given configuration directory. 
     * 
     * @throws Exception if there is a fatal error loading resources.
     */
	public void loadResourceForAllComponents(String confDir,
	        String[] components) throws Exception {
		if (components == null) {
		  components = new String[] {
				  "gateway",
				  "namenode", 
				  "resourcemanager"};
		}
		for (String component : components) {
			loadResourceForComponent(confDir, component);	
		}	
	}

	/**
     * Copy the remote configuration directory to local so that it can be
     * processed later. 
     * 
     * @param confDir the Hadoop configuration directory 
     * @param component the cluster component name. 
     * 
     * @return String of the local configuration directory. 
     * 
     * @throws Exception if there is a fatal error running the process to scp 
     *          the remote conf dir.
     */
	public String copyRemoteConfDirToLocal(String confDir, String component) 
			throws Exception {
		/* Generate the localconfiguration filename */
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
		df.setTimeZone(TimeZone.getTimeZone("CST"));  
		String localConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-" +	
				component + ".local." + df.format(new Date());	
		String componentHost = TestSession.cluster.getNodes(component)[0];
		String[] cmd = {
		        "/usr/bin/scp", "-r",
		        componentHost + ":" + confDir, localConfDir};
		String[] output = TestSession.exec.runProcBuilder(cmd);
		return localConfDir;
	}


	/**
     * Initialize the Hadoop configuation files for the given configuration
     * directory and the given component. 
     * 
     * @param confDir the Hadoop configuration directory 
     * @param component the cluster component name. 
     * 
     * @throws Exception if there is a fatal error copying the remote
     * configuration, or loading the configuration resource files.
     */
	public void loadResourceForComponent(String confDir, String component) 
			throws Exception {

		/*
		 *  For components on remote hosts (i.e. not gateway), we will need to
		 *  scp the files to a local tmp configuration directory (localConfDir)
		 *  so we can parse the files content. 
		 */
		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir(HadoopConfiguration.GATEWAY);
		}
		String localConfDir = (component.equals("gateway")) ? confDir :
			this.copyRemoteConfDirToLocal(confDir, component);

		/*
		 * Get the component configuration properties Hashtable containing all
		 * the Hadoop configuration files for a particular component. Also
		 * include a metadata properties to track of information such as the
		 * origin configuration path on the remote host.
		 */
		Hashtable<String, Properties> componentConfProps =
				loadResourceFiles(localConfDir);
		Properties metadata = new Properties();
		metadata.setProperty("path", confDir);
		componentConfProps.put("metadata", metadata);
		componentResourcesProps.put(component, componentConfProps);
		
		/*
		 * Print out the hashtable of configuration properties for the component. 
		 */
		Hashtable<String, Properties> configs =
		        componentResourcesProps.get(component);
		Enumeration<String> enumKey = configs.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
		    Properties prop = configs.get(key);
		    	TestSession.logger.trace(
		    	        "List hadoop config properties for config file " + key);
		    	traceProperties(prop);
		}
	}		

    /**
     * Initialize the Hadoop configuration files. 
     * 
     * @return Hashtable of Properties for each of the configuration files.
     * 
     * @throws IOException if the DocumentBuilder can not parse the filename
     * @throws ParserConfigurationException if the DocumentBuilderFactory can
     *         not retrieve a new DocumentBuilder
     * @throws SAXException if the DocumentBuilder can not parse the filename
     */
	private void loadResourceFiles() 
			throws IOException, ParserConfigurationException, SAXException {
		loadResourceFiles(null);
	}
	
    /**
     * Initialize the Hadoop configuration files. 
     * 
     * @param confDir the configuration directory path.
     * 
     * @return hastable of Properties for each of the configuration files.
     * 
     * @throws IOException if the DocumentBuilder can not parse the filename
     * @throws ParserConfigurationException if the DocumentBuilderFactory can
     *         not retrieve a new DocumentBuilder
     * @throws SAXException if the DocumentBuilder can not parse the filename
     */
	private Hashtable<String, Properties> loadResourceFiles(String confDir) 
		throws IOException, ParserConfigurationException, SAXException {

		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir();
		}
		
		/*
		 * Create a Hashtable to contain configuration properties loaded for
		 * each Hadoop xml configuration file in the configuration directory. 
		 */
		Hashtable<String, Properties> hadoopConfFileProps =
				new Hashtable<String, Properties>();

		/* String array of each of the Hadoop configuration files to load. */
		String[] confFilenames = {
				HADOOP_CONF_CORE,
				HADOOP_CONF_HDFS,
				HADOOP_CONF_MAPRED,
				HADOOP_CONF_YARN,
				HADOOP_CONF_CAPACITY_SCHEDULER, 
				HADOOP_CONF_FAIR_SCHEDULER
				};

		/* Parse and load each of the configuration files */
		for (int i = 0; i < confFilenames.length; i++) {
			String confFilename = confFilenames[i];
			hadoopConfFileProps.put(confFilename,
					this.parseResourceFile(confDir + "/" +
							confFilename));
		}

		/* Print the stored configuration properties */
		for (int i = 0; i < confFilenames.length; i++) {
			String confFilename = confFilenames[i];
			Properties prop = hadoopConfFileProps.get(confFilename);
			TestSession.logger.trace("List the parsed Hadop configuration " + 
					"properties for " + confFilename + ":");
			traceProperties(prop);
		}

		return hadoopConfFileProps;
	}

    /**
     * Parse the Hadoop XML configuration file for a given filename.
     * 
     * @param filename Hadoop configuration file name such as core-site.xml,
     * hdfs-site.xml, etc.
     * 
     * @return Properties for the given configuration files.
     * 
     * @throws IOException if the DocumentBuilder can not parse the filename
     * @throws ParserConfigurationException if the DocumentBuilderFactory can
     *         not retrieve a new DocumentBuilder
     * @throws SAXException if the DocumentBuilder can not parse the filename
     */
	public Properties parseResourceFile(String filename) 
			throws ParserConfigurationException, IOException, SAXException {
		TestSession.logger.debug("Parse Hadoop configuration file: " +
				filename);
		Properties props = new Properties();

		/*
		 * Parse the XML configuration file using a DOM parser
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: " +
				document.getDocumentElement().getNodeName());			

		/*
		 * Write the properties key and value to a Java Properties Object.
		 */
		NodeList nodes = document.getElementsByTagName("property");
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
				    TestSession.logger.trace("Value for property name " +
				            propName + " is null");
				}
			}
		}

		return props;
	}

    /**
     * Get the xml node value given a tag and an element. 
     * 
     * @param tag Tag name in the xml element
     * @param element The xml element.
     * 
     * @return String node value.
     */
	private static String getValue(String tag, Element element) {
		NodeList nodes =
		        element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}
	
    /**
     * Set the xml node value given a tag and an element. 
     * 
     * @param tag Tag name in the xml element
     * @param element The xml element.
     * @param value tag name value to insert.
     * 
     * @return String node value.
     */
	private static void setValue(String tag, Element element, String value) {
		NodeList nodes =
		        element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = (Node) nodes.item(0);
		node.setNodeValue(value);
	}

    /**
     * Insert into the xml node name and value pair given a tag and an element. 
     * 
     * @param tag Tag name in the xml element
     * @param element The xml element.
     * @param value tag name value to insert.
     * 
     * @return String node value.
     */
	private static void insertValue(String nameTag, String valueTag,
			Element element, String name, String value, Document document) {

		// Example: 
		// <property><name>yarn.admin.acl3</name><value>gridadmin,hadoop,hadoopqa,philips,foo</value></property>

		// Get the document root
		Element root = document.getDocumentElement();

		// Create the property element
		Element propertyElement = document.createElement("property");

		// Constrcut the name element
		Element nameElement = document.createElement(nameTag);
		nameElement.appendChild(document.createTextNode(name));
				
		// Construct the value element
		Element valueElement = document.createElement(valueTag);
		valueElement.appendChild(document.createTextNode(value));

		// Append the name and the value element to the property element
		propertyElement.appendChild(document.createTextNode("\n"));
		propertyElement.appendChild(nameElement);		
		propertyElement.appendChild(document.createTextNode("\n"));
		propertyElement.appendChild(valueElement);		
		propertyElement.appendChild(document.createTextNode("\n"));

		// Apend the property element to the root element
		root.appendChild(document.createTextNode("\n"));
		root.appendChild(propertyElement);
		root.appendChild(document.createTextNode("\n\n"));	
	}

	
	/**
     * Set the Hadoop configuration file property for a given property name,
     * property value, component, and file name. 
     * 
     * @param propName String of property name
     * @param propValue String of property value
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param confFilename String of the configuration file name.
     * 
     * @throws Exception if the configuration file property can not be set.
     */
	public void setHadoopConfFileProp (String propName, String propValue,
			String component, String confFilename) 
					throws Exception {
	    setHadoopConfFileProp(propName, propValue, component, confFilename,
	    		null);
	}
	
    /**
     * Set the Hadoop configuration file property for a given property name,
     * property value, component, file name, and configuration directory path.
     * 
     * @param propName String of property name
     * @param propValue String of property value
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param confFilename String of the configuration file name.
     * @param confDir String of the configuration directory path.
     * 
     * @throws Exception if the configuration file property can not be set.
     */
	public void setHadoopConfFileProp (String propName, String propValue,
			String component, String confFilename, String confDir) 
					throws Exception {

		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir(component);
			if ((confDir == null) || confDir.isEmpty()) {
				/* Custom directory has not yet been set */
				TestSession.logger.warn(
				        "Custom configuration directory not set!!!");
			}
		}

		// We don't want to call propNameExistsInHadoopConfFile at this point
		// because the HadoopConfFileProp may not have been initialized at. 
		// It is only initialized when the component reset is called. 
		
		// We also don't want to set the property for the specified property
		// name and value in the internal Properties object because again it
		// may not exists yet.  This needs to be done when the component reset
		// occurs. 
		
		/* Copy the file to local if component is not gateway */
		String localConfDir = (component.equals("gateway")) ? confDir :
			this.copyRemoteConfDirToLocal(confDir, component);

		/* Write to file */
		this.updateXmlConfFile(localConfDir + "/" + confFilename,
				propName, propValue);

		/* Copy the directory back to the remote host*/
		if (!component.equals("gateway")) {
			this.copyFilesToConfDir(component, localConfDir);
		}
	}
	
    /**
     * Insert or replace the property tag in the xml configuration file. 
     * 
     * @param filename the xml configuration file
     * @param targetPropName the target property tag
     * @param targetPropValue the value to replace the property tag with
     * 
     * @throws ParseConfigurationException if the DocumentBuilderFactory can
     * not get a new DocumentBuilder
     * @throws SAXException if the DocumentBuilder can not parse the file
     * @throws IOException if the DocumentBuilder can not parse the file
     * @throws TransformerException if the Transformer can not transform the
     * document
     * @throws TransformerConfigurationException if the TransformerFactory can 
     * not provide a new Transformer instance
     */
	public void updateXmlConfFile (String filename, String targetPropName,
			String targetPropValue) 
					throws ParserConfigurationException, SAXException, 
					IOException, TransformerException, 
					TransformerConfigurationException {

		TestSession.logger.info("Insert/Replace property '" +
				targetPropName + "'='" + targetPropValue + "'.");
		
		boolean foundPropName = false;
		
		/*
		 * Parse the XML configuration file using a DOM parser
		 */	
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: " +
				document.getDocumentElement().getNodeName());			

		/*
		 * Write the properties key and value to a Java Properties Object.
		 */
		Element element = null;
		NodeList nodes = document.getElementsByTagName("property");
		for (int index = 0; index < nodes.getLength(); index++) {
			Node node = nodes.item(index);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				element = (Element) node;

				String propName = getValue("name", element);
				String propValue = getValue("value", element);

				TestSession.logger.trace("Config Property Name: " +
						getValue("name", element));
				TestSession.logger.trace("Config Property Value: " +
						getValue("value", element));

				if (propName.equals(targetPropName)) {
					foundPropName = true;
					if (propValue.equals(targetPropValue)) {
						TestSession.logger.info("Propperty value for '" +
								propName + "' is already set to '" + 
								propValue + "'.");
						return;
					}
					else {
						setValue("value", element, targetPropValue);
					}
				}
			}
		}

		if (foundPropName == false) {
			insertValue("name", "value", element,
					targetPropName, targetPropValue, document);				
		}		

		// Write the change to file
		Transformer xformer = TransformerFactory.newInstance().newTransformer();
		String outputFile = filename;
		xformer.transform(new DOMSource(document),
				new StreamResult(new File(outputFile)));

	}
	
    /**
     * Copy a single file from a given Hadoop configuration directory to a 
     * Hadoop cluster gateway. This assumes that the cluster under test is 
     * already using a custom backup directory that is editable, by previously 
     * calling the backupConfDir() methdo. 
     * 
     * @param sourceFile source configuration file to copy
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
    public boolean copyFileToConfDir (String sourceFile) throws Exception {
        return copyFileToConfDir(sourceFile, null, null, null);    
    }

	/**
     * Copy a single file from a given Hadoop configuration directory to a 
     * Hadoop cluster component. This assumes that the cluster under test is 
     * already using a custom backup directory that is editable, by previously 
     * calling the backupConfDir() methdo. 
     * 
     * @param sourceFile source configuration file to copy
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	public boolean copyFileToConfDir (String sourceFile, String component) 
			throws Exception {
		return copyFileToConfDir(sourceFile, component, null, null);	
	}

	/**
     * Copy a single file from a given Hadoop configuration directory to a 
     * Hadoop cluster component. This assumes that the cluster under test is 
     * already using a custom backup directory that is editable, by previously 
     * calling the backupConfDir() methdo. 
     * 
     * @param sourceFile source configuration file to copy
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param targetFile target configuration file to copy to
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	public boolean copyFileToConfDir (String sourceFile, String component,
	        String targetFile) throws Exception {
		return copyFileToConfDir(sourceFile, component, null, targetFile);			
	}

    /**
     * Copy a single file from a given Hadoop configuration directory to a 
     * Hadoop cluster component. This assumes that the cluster under test is 
     * already using a custom backup directory that is editable, by previously 
     * calling the backupConfDir() methdo. 
     * 
     * @param sourceFile source configuration file to copy
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param daemonHost String Array of component hostname(s).
     * @param target file name.
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	public boolean copyFileToConfDir (String sourceFile, String component, 
			String[] daemonHost, String targetFile) throws Exception {
		return copyToConfDir(sourceFile, component, daemonHost, targetFile);			
	}

    /**
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param sourceDir source configuration directory to copy the files from
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
    public boolean copyFilesToConfDir (String sourceDir) throws Exception {
        return copyFilesToConfDir(sourceDir, null, null);
    }

	/**
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param sourceDir source configuration directory to copy the files from
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	public boolean copyFilesToConfDir (String sourceDir, String component) 
			throws Exception {
		return copyFilesToConfDir(sourceDir, component, null);
	}

    /**
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param sourceDir source configuration directory to copy the files from
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param daemonHost String Array of component hostname(s).
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	public boolean copyFilesToConfDir (String sourceDir, String component, 
			String[] daemonHost) throws Exception {
	    if (!sourceDir.endsWith("/")) {
	        sourceDir = sourceDir.concat("/");
	    }
		return copyToConfDir(sourceDir+"*", component, daemonHost, null);
	}

    /**
     * Copy file or files from a given Hadoop configuration directory to a 
     * Hadoop cluster component. This assumes that the cluster under test is 
     * already using a custom backup directory that is editable, by previously 
     * calling the backupConfDir() methdo. 
     * 
     * @param sourceFiles source configuration file or files to copy
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param daemonHost String Array of component hostname(s).
     * @param target file name
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error copying the configuration 
     * file.
     */
	private boolean copyToConfDir (String sourceFile, String component, 
			String[] daemonHost, String targetFile) 
					throws Exception {

        if ((component == null) || component.isEmpty()) {
            component = "gateway";
        }

        if ((!component.equals("gateway")) && (daemonHost == null)) {
            daemonHost = TestSession.cluster.getNodes(component); 
        }

	    /* pdcp doesn't always work because it needs to be available on all of
		 * the Hadoop nodes and they are not installed by default. 
		 * Alternative is use pdsh with scp copying from the gateway host to 
		 * one or more of the target hosts. But this will require that the
		 * source directory on the gateway be in the full path, and not in 
		 * relative path.  If path is a relative path, it will be concatenated 
		 * with the default root directory path of 
		 * TestSession.conf.getProperty("WORKSPACE")  
		 */
		if (!sourceFile.startsWith("/")) {
			sourceFile = TestSession.conf.getProperty("WORKSPACE") + "/" +
					sourceFile;
		}
		
		if (!component.equals("gateway")) {
			sourceFile = this.getHadoopProp("GATEWAY") + ":" + sourceFile;
		}
		
		/* target would either be the custom Hadoop configuration directory
		 * where files are to be copied to, or the actual full path filename to
		 * be copied to.
		 */
		String confDir = this.getHadoopConfDir(component);
		String target =
			((targetFile == null) || targetFile.isEmpty()) ?
			confDir : confDir + "/" + targetFile;

		String cpBin = (component.equals("gateway")) ?
			"/bin/cp" : "/usr/bin/scp";
		String cpCmd[] = { cpBin, sourceFile, target};
				
		String[] cmd;
		if (component.equals("gateway")) {
			TestSession.logger.info("Copy files(s) to the Hadoop " + 
					"configuraiton directory " + confDir + " on the gateway:");
            String[] cpCmd_ = { "/bin/sh", "-c",
                    cpBin + " " +  sourceFile + " " +  target};
            cmd = cpCmd_;
		}
		else {
			TestSession.logger.info("Copy file(s) to the Hadoop " +
					"configuration directory " + confDir + " on " +
					"the " + component + " host(s) of " +
					Arrays.toString(daemonHost));
			
			/*
			 * if conf dir is NFS mounted and shared across component hosts,
			 * do just on a single host
			 */
			boolean isConfDirNFS = true;
            String targetHost = (isConfDirNFS) ? daemonHost[0] :
                StringUtils.join(daemonHost, "," );
            String[] pdshCmd = { "/home/y/bin/pdsh", "-w", targetHost};

			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
			cmd = temp.toArray(new String[pdshCmd.length+cpCmd.length]);
		}
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		return output[0].equals("0") ? true : false;
	}

	public boolean insertBlock (String component, String matchStr, 
	        String appendStr, String targetFile, String RS, String LS) 
	                throws Exception {
	    if ((component == null) || component.isEmpty()) {
	        component = "gateway";
	    }
	    
	    String[] daemonHost = null;
	    if ((!component.equals("gateway")) && (daemonHost == null)) {
	        daemonHost = TestSession.cluster.getNodes(component); 
	    }
	    
	    String confDir = this.getHadoopConfDir(component);

	    if (RS == null) { RS = ""; }
        if (LS == null) { LS = "\n"; }

	    String perlCmd = "/usr/local/bin/perl -pi -0e 'BEGIN {$match_str=\"" +
	            matchStr + "\"; $append_str=\"" + appendStr + "\"}; " +
	            "s|" + 
	            "(.*."+ matchStr + ".*\n)|" + 
	            "${1}" + RS + appendStr + LS + "|' " +
	            targetFile;

	    String insertCmd[] = { perlCmd };
	    
	    String[] cmd;
	    if (component.equals("gateway")) {
	        TestSession.logger.info("Insert block to the Hadoop " + 
	                "configuraiton directory " + confDir + " on the gateway:");
            String[] insertCmd_ = {"/bin/sh", "-c", perlCmd };
            cmd = insertCmd_;
	    }
	    else {
	        TestSession.logger.info("Insert block to the Hadoop "  +
	                "configuration directory " + confDir + " on " +
	                "the " + component + " host(s) of " +
	                Arrays.toString(daemonHost));
	        
            /*
             * if conf dir is NFS mounted and shared across component hosts,
             * do just on a single host
             */
            boolean isConfDirNFS = true;
            String targetHost = (isConfDirNFS) ? daemonHost[0] :
                StringUtils.join(daemonHost, "," );
            String[] pdshCmd = { "/home/y/bin/pdsh", "-w", targetHost};
	        
	        ArrayList<String> temp = new ArrayList<String>();
	        temp.addAll(Arrays.asList(pdshCmd));
	        temp.addAll(Arrays.asList(insertCmd));
	        cmd = temp.toArray(new String[pdshCmd.length+insertCmd.length]);
	    }
	    String output[] = TestSession.exec.runProcBuilder(cmd);
	    TestSession.logger.trace(Arrays.toString(output));

	    return output[0].equals("0") ? true : false;
	}


    public boolean replaceBlock (String component, String matchStr, 
            String appendStr, String targetFile, String RS, String LS) 
                    throws Exception {
        if ((component == null) || component.isEmpty()) {
            component = "gateway";
        }
        
        String[] daemonHost = null;
        if ((!component.equals("gateway")) && (daemonHost == null)) {
            daemonHost = TestSession.cluster.getNodes(component); 
        }
        
        String confDir = this.getHadoopConfDir(component);
        
        if (RS == null) { RS = ""; }
        if (LS == null) { LS = "\n"; }

        String perlCmd = "/usr/local/bin/perl -pi -0e 'BEGIN {$match_str=\"" +
                matchStr + "\"; $append_str=\"" + appendStr + "\"}; " +
                "s|" + 
                "("+ matchStr + ")|" + 
                RS + appendStr + LS + "|' " +
                targetFile;

        String replaceCmd[] = { perlCmd };
        
        String[] cmd;
        if (component.equals("gateway")) {
            TestSession.logger.info("Insert block to the Hadoop " + 
                    "configuraiton directory " + confDir + " on the gateway:");
            String[] replaceCmd_ = {"/bin/sh", "-c", perlCmd };
            cmd = replaceCmd_;
        }
        else {
            TestSession.logger.info("Insert block to the Hadoop "  +
                    "configuration directory " + confDir + " on " +
                    "the " + component + " host(s) of " +
                    Arrays.toString(daemonHost));
            
            /* if conf dir is NFS mounted and shared across component hosts,
             * do just on a single host
             */
            boolean isConfDirNFS = true;
            String targetHost = (isConfDirNFS) ? daemonHost[0] :
                StringUtils.join(daemonHost, "," );
            String[] pdshCmd = { "/home/y/bin/pdsh", "-w", targetHost};
            
            ArrayList<String> temp = new ArrayList<String>();
            temp.addAll(Arrays.asList(pdshCmd));
            temp.addAll(Arrays.asList(replaceCmd));
            cmd = temp.toArray(new String[pdshCmd.length+replaceCmd.length]);
        }
        String output[] = TestSession.exec.runProcBuilder(cmd);
        TestSession.logger.trace(Arrays.toString(output));

        return output[0].equals("0") ? true : false;
    }


    /**
     * Backup the Hadoop configuration directory for the gateway client.
     * This will setup a new temporary Hadoop configuration directory where
     * settings can be changed for testing. This is necessary because the
     * default configuration directory is owned and writable only to root. 
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error backing up the configuration 
     * directory.
     */
    public boolean backupConfDir () throws Exception {
        return backupConfDir(null, null);          
    }
    
	/**
     * Backup the Hadoop configuration directory for a given component.
     * This will setup a new temporary Hadoop configuration directory where
     * settings can be changed for testing. This is necessary because the
     * default configuration directory is owned and writable only to root. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error backing up the configuration 
     * directory.
     */
	public boolean backupConfDir (String component) throws Exception {
		if (component.equals("gateway")) {
			return backupConfDir(component, null);			
		}
		else {
			return backupConfDir(
						component, TestSession.cluster.getNodes(component));
		}
	}
	
    /**
     * Backup the Hadoop configuration directory for a given component.
     * This will setup a new temporary Hadoop configuration directory where
     * settings can be changed for testing. This is necessary because the
     * default configuration directory is owned and writable only to root. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param daemonHost String Array of component hostname(s).
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error backing up the configuration 
     * directory.
     */
	public boolean backupConfDir (String component, String[] daemonHost) 
			throws Exception {
	    if ((component == null) || component.isEmpty()) {
	        component = "gateway";
	    }

        if ((!component.equals("gateway")) && (daemonHost == null)) {
            daemonHost = TestSession.cluster.getNodes(component); 
        }           
	    // Current Hadoop conf dir
		String currentComponentConfDir = this.getHadoopConfDir(component);

		// New Hadoop conf dir
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
	    df.setTimeZone(TimeZone.getTimeZone("CST"));  
	    String customConfDir = this.getHadoopProp("TMP_DIR") + 
	            "/hadoop-conf-" + component + "." +    
	    		df.format(new Date());

	    // Follow and dereference symlinks
        String cpCmd[] = {
                "/bin/cp", "-rfL", currentComponentConfDir, customConfDir};

        // Change all the xml config files to have the proper permission so that
        // they can be changed by the tests later.
        String[] chmodCmd = {"/bin/chmod", "644", customConfDir+"/*.xml"};

		String[] cmd = null;
		if (!component.equals("gateway")) {
			TestSession.logger.info("Back up the Hadoop configuration " +
					"directory to " + customConfDir + " on " + "the " + 
					component + " host(s) of " + Arrays.toString(daemonHost));
			/* if conf dir is NFS mounted and shared across component hosts,
			 * do just on a single host
			 */
			boolean isConfDirNFS = true;
			String targetHost = (isConfDirNFS) ? daemonHost[0] :
			    StringUtils.join(daemonHost, "," );
			String[] pdshCmd = { "/home/y/bin/pdsh", "-w", targetHost};
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
    			temp.add(";");
			temp.addAll(Arrays.asList(chmodCmd));
			cmd = temp.toArray(
			        new String[pdshCmd.length+cpCmd.length+chmodCmd.length]);
		}
		else {
			TestSession.logger.info(
			        "Back up the Hadoop configuration directory on " +
					"the gateway:");
			cmd = cpCmd;
		}		
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		// Update the HadoopConfDir Properties
        this.setHadoopConfDir(customConfDir, component);

		// The setHadoopConfDirPath() will set the path to the new configuration
		// directory. It is the responsibility for the test to call the 
		// cluster.reset() to restart the component for the configuration change
		// to take effect, and to reinitialize the configuration object (via
		// initComponentConfFiles(tmpConfDir, component))

		return output[0].equals("0") ? true : false;
	}

    /**
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
			
}
