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
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A class that represents a Hadoop Configuration for a distributed
 * Hadoop cluster under test.
 */
public class FullyDistributedConfiguration extends TestConfiguration
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
	private Hashtable<String, Hashtable<String, Properties>> componentResourcesProps =
			new Hashtable<String, Hashtable<String, Properties>>();

	
	/**
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test.
	 * Hadoop default configuration is not used.
	 */
	public FullyDistributedConfiguration() {
		super(false);
		this.loadResourceForComponent(
				hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR"), "gateway");	
	}

	/**
	 * Loads the Hadoop default configuration if true is passed as a parameter, before the 
	 * distributed test cluster default configuration is initialized into the 
	 * configuration.
	 * 
	 * @param loadDefaults whether or not to load the default configuration parameters
	 * specified by the Hadoop installation, before loading the class configuration defaults.
	 */
	public FullyDistributedConfiguration(boolean loadDefaults) {
		super(loadDefaults);
		this.loadResourceForComponent(
				hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR"), "gateway");
	}
	
	/**
	 * Initializes cluster-specific properties defaults.
	 */
	protected void initDefaultsClusterSpecific() {
		hadoopProps.setProperty("CLUSTER_NAME", TestSession.conf.getProperty("CLUSTER_NAME", ""));
		try {
			hadoopProps.setProperty("GATEWAY", InetAddress.getLocalHost().getHostName());
		}
		catch (Exception e) {
			TestSession.logger.warn("Hostname not found!!!");
		}

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
		hadoopProps.setProperty("HADOOP_INSTALL", HADOOP_ROOT + "/gs/gridre/yroot." +
				hadoopProps.getProperty("CLUSTER_NAME"));
		
		hadoopProps.setProperty("HADOOP_COMMON_HOME", hadoopProps.getProperty("HADOOP_INSTALL") +
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

	/**
	 * Removes the configuration files from disk, that were written to disk
	 * by the .write() of the object.
	 * 
	 * Currently unimplemented for FullyDistributedConfiguration.
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
	
    /**
     * Initialize the Hadoop configuration files for each components. 
     */
	public void loadResourceForAllComponents() {
		loadResourceForAllCopmonents(null, null);
	}
	
    /**
     * Initialize the Hadoop configuration files for each components for the
     * given configuration directory. 
     */
	public void loadResourceForAllComponents(String confDir) {
		loadResourceForAllCopmonents(confDir, null);
	}

    /**
     * Initialize the Hadoop configuration files for each components for the
     * given configuration directory. 
     */
	public void loadResourceForAllCopmonents(String confDir, String[] components) {
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
     */
	public String copyRemoteConfDirToLocal(String confDir, String component) {
		/* Generate the localconfiguration filename */
		DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
		df.setTimeZone(TimeZone.getTimeZone("CST"));  
		String localConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-" +	
				component + "-" + df.format(new Date());	
		String componentHost = TestSession.cluster.getNodes(component)[0];
		String[] cmd = {"/usr/bin/scp", "-r", componentHost + ":" + confDir, localConfDir};
		String[] output = TestSession.exec.runProcBuilder(cmd);
		return localConfDir;
	}


	/**
     * Initialize the Hadoop configuation files for the given configuration
     * directory and the given component. 
     * 
     * @param confDir the Hadoop configuration directory 
     * @param component the cluster component name. 
     */
	public void loadResourceForComponent(String confDir, String component) {

		/*
		 *  For components on remote hosts (i.e. not gateway), we will need to
		 *  scp the files to a local tmp configuration directory (localConfDir)
		 *  so we can parse the files content. 
		 */
		if ((confDir == null) || confDir.isEmpty()) {
			confDir = hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR");
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
		Hashtable<String, Properties> configs = componentResourcesProps.get(component);
		Enumeration<String> enumKey = configs.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
		    Properties prop = configs.get(key);
		    	TestSession.logger.trace("List hadoop config properties for config file " + key);
		    	traceProperties(prop);
		}
	}		

    /**
     * Initialize the Hadoop configuration files. 
     * 
     * @return Hashtable of Properties for each of the configuration files.
     */
	private void loadResourceFiles() {
		loadResourceFiles(null);
	}
	
    /**
     * Initialize the Hadoop configuration files. 
     * 
     * @param confDir the configuration directory path.
     * 
     * @return hastable of Properties for each of the configuration files.
     */
	private Hashtable<String, Properties> loadResourceFiles(String confDir) {

		if ((confDir == null) || confDir.isEmpty()) {
			confDir = hadoopProps.getProperty("HADOOP_DEFAULT_CONF_DIR");
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
     */
	public Properties parseResourceFile(String filename) {
		TestSession.logger.debug("Parse Hadoop configuration file: " +
				filename);
		Properties props = new Properties();
		try {
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

    /**
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
		NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
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
     */
	public void setHadoopConfFileProp (String propName, String propValue,
			String component, String confFilename) {
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
     */
	public void setHadoopConfFileProp (String propName, String propValue,
			String component, String confFilename, String confDir) {

		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDirPath(component);
			if ((confDir == null) || confDir.isEmpty()) {
				/* Custom directory has not yet been set */
				TestSession.logger.warn("Custom configuration directory not set!!!");
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
     */
	public void updateXmlConfFile (String filename, String targetPropName,
			String targetPropValue) {

		TestSession.logger.info("Insert/Replace property '" +
				targetPropName + "'='" + targetPropValue + "'.");
		
		boolean foundPropName = false;
		
		/*
		 * Parse the XML configuration file using a DOM parser
		 */		
		try {
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
		catch (Exception e) {
			
		}

	}
	
	/**
     * Copy a single file from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceFile source configuration file to copy
     * 
     * @return boolean true for success, false for failure.
     */
	public boolean copyFileToConfDir (String component, String sourceFile) {
		String targetFile = null;
		String[] daemonHost = (component.equals("gateway")) ?
				null : TestSession.cluster.getNodes(component);
		return copyFileToConfDir(component, sourceFile, daemonHost, targetFile);	
	}


	/**
     * Copy a single file from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceFile source configuration file to copy
     * @param targetFile target configuration file to copy to
     * 
     * @return boolean true for success, false for failure.
     */
	public boolean copyFileToConfDir (String component, String sourceFile, String targetFile) {
		String[] daemonHost = (component.equals("gateway")) ?
				null : TestSession.cluster.getNodes(component);
		return copyFileToConfDir(component, sourceFile, daemonHost, targetFile);			
	}

    /**
     * Copy a single file from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceFile source configuration file to copy
     * @param daemonHost String Array of component hostname(s).
     * @param target file name.
     * 
     * @return boolean true for success, false for failure.
     */
	public boolean copyFileToConfDir (String component, String sourceFile,
			String[] daemonHost, String targetFile) {
		return copyToConfDir(component, sourceFile, daemonHost, targetFile);			
	}

	/**
     * Copy files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceDir source configuration directory to copy the files from
     * 
     * @return boolean true for success, false for failure.
     */
	public boolean copyFilesToConfDir (String component, String sourceDir) {
		String[] daemonHost = (component.equals("gateway")) ?
				null : TestSession.cluster.getNodes(component);
		return copyFilesToConfDir(component, sourceDir, daemonHost);	
	}

    /**
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
     * @return boolean true for success, false for failure.
     */
	public boolean copyFilesToConfDir (String component, String sourceDir,
			String[] daemonHost) {
		String targetFile = null;
		return copyToConfDir(component, sourceDir + "/*", daemonHost, targetFile);
	}

    /**
     * Copy file or files from a given Hadoop configuration directory to a Hadoop
     * cluster component. This assumes that the cluster under test is already
     * using a custom backup directory that is editable, by previously calling
     * the backupConfDir() methdo. 
     * 
     * @param component cluster component such as gateway, namenode,
     * resourcemanager, etc.
     * @param sourceFiles source configuration file or files to copy
     * @param daemonHost String Array of component hostname(s).
     * @param target file name
     * 
     * @return boolean true for success, false for failure.
     */
	private boolean copyToConfDir (String component, String sourceFile,
			String[] daemonHost, String targetFile) {

		String confDir = this.getHadoopConfDirPath(component);
		String target =
			((targetFile == null) || targetFile.isEmpty()) ?
			confDir : confDir + "/" + targetFile;
		
		String cpCmd[] = { "/usr/bin/scp",
				this.getHadoopProp("GATEWAY") + ":" + sourceFile,
				target};
				
		if ((!component.equals("gateway")) && (daemonHost == null)) {
			daemonHost = TestSession.cluster.getNodes(component); 
		}

		String[] cmd;
		if (!component.equals("gateway")) {
			TestSession.logger.info("Copy file(s) to the Hadoop " +
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

		return output[0].equals("0") ? true : false;
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
     */
	public boolean backupConfDir (String component) {
		if (component.equals("gateway")) {
			return backupConfDir(component, null);			
		}
		else {
			return backupConfDir(
						component,
						TestSession.cluster.getNodes(component));
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
     */
	public boolean backupConfDir (String component, String[] daemonHost) {
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
			daemonHost = TestSession.cluster.getNodes(component); 
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
