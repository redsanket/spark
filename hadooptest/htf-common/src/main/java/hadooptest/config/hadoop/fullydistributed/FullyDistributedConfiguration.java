/*
 * YAHOO!
 */

package hadooptest.config.hadoop.fullydistributed;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
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
 * A class that represents a Hadoop Configuration for a distributed Hadoop
 * cluster under test.
 */
public class FullyDistributedConfiguration extends HadoopConfiguration {
	/**
	 * Calls the superclass constructor, and initializes the default
	 * configuration parameters for a distributed cluster under test. Hadoop
	 * default configuration is not used.
	 * 
	 * @throws Exception
	 *             if there is a fatal error loading a resource from the
	 *             gateway.
	 */
	public FullyDistributedConfiguration(String confDir, String hostname,
			String component) throws Exception {
		super(false, confDir, hostname, component);
	}

	/**
	 * Loads the Hadoop default configuration if true is passed as a parameter,
	 * before the distributed test cluster default configuration is initialized
	 * into the configuration.
	 * 
	 * @param loadDefaults
	 *            whether or not to load the default configuration parameters
	 *            specified by the Hadoop installation, before loading the class
	 *            configuration defaults.
	 * 
	 * @throws Exception
	 *             if there is a fatal error loading a resource from the
	 *             gateway.
	 */
	public FullyDistributedConfiguration(boolean loadDefaults, String confDir,
			String hostname, String component) throws Exception {
		super(loadDefaults, confDir, hostname, component);
	}

	/**
	 * Initializes cluster-specific properties defaults.
	 * 
	 * @throws UnknownHostException
	 *             if we can not get the localhost
	 */
	protected void initDefaultsClusterSpecific() throws UnknownHostException {
		hadoopProps.setProperty("CLUSTER_NAME",
				TestSession.conf.getProperty("CLUSTER_NAME", ""));
		hadoopProps.setProperty("GATEWAY", InetAddress.getLocalHost()
				.getHostName());

		// Setup temp directory
		String defaultTmpDir = "/homes/hadoopqa/tmp/hadooptest";
		hadoopProps.setProperty("TMP_DIR",
				TestSession.conf.getProperty("TMP_DIR", defaultTmpDir));
		String tmpDir = this.getHadoopProp("TMP_DIR") + "/hadooptest-"
				+ TestSession.getFileDateFormat(new Date());
		new File(tmpDir).mkdirs();
		hadoopProps.setProperty("TMP_DIR", tmpDir);

		String HADOOP_ROOT = "/home"; // /grid/0
		hadoopProps.setProperty("JAVA_HOME", HADOOP_ROOT + "/gs/java/jdk");
		hadoopProps
				.setProperty(
						"HADOOP_INSTALL",
						HADOOP_ROOT + "/gs/gridre/yroot."
								+ hadoopProps.getProperty("CLUSTER_NAME"));
		hadoopProps.setProperty("HADOOP_INSTALL_CONF_DIR",
				hadoopProps.getProperty("HADOOP_INSTALL") + "/conf/hadoop");
		hadoopProps.setProperty("HADOOP_COMMON_HOME",
				hadoopProps.getProperty("HADOOP_INSTALL") + "/share/hadoop");
	}

	/**
	 * Get the resource property.
	 * 
	 * @param property
	 *            name String.
	 */
	public String getResourceProp(String propName) {
		return super.get(propName);
	}

	/**
	 * Writes the distributed cluster configuration specified by the object out
	 * to disk.
	 * 
	 * Currently unimplemented for FullyDistributedConfiguration.
	 * 
	 * @throws IOException
	 *             if there is a problem writing the configuration to disk.
	 */
	public void write() throws IOException {

	}

	/**
	 * Removes the configuration files from disk, that were written to disk by
	 * the .write() of the object.
	 * 
	 * Currently unimplemented for FullyDistributedConfiguration.
	 */
	public void cleanup() {

	}

	/**
	 * Copy the remote configuration directory to local so that it can be
	 * processed later.
	 * 
	 * @param confDir
	 *            the Hadoop configuration directory
	 * @param component
	 *            the cluster component name.
	 * 
	 * @return String of the local configuration directory.
	 * 
	 * @throws Exception
	 *             if there is a fatal error running the process to scp the
	 *             remote conf dir.
	 */
	public String copyRemoteConfDirToLocal(String confDir, String component)
			throws Exception {
		component = this.component;
		/* Generate the localconfiguration filename */
		String localConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-"
				+ component + ".local."
				+ TestSession.getFileDateFormat(new Date());
		String componentHost = TestSession.cluster.getNodeNames(component)[0];
		String[] cmd = { "/usr/bin/scp", "-r", componentHost + ":" + confDir,
				localConfDir };
		String[] output = TestSession.exec.runProcBuilder(cmd);
		return localConfDir;
	}

	/**
	 * Get the xml node value given a tag and an element.
	 * 
	 * @param tag
	 *            Tag name in the xml element
	 * @param element
	 *            The xml element.
	 * 
	 * @return String node value.
	 */
	private static String getValue(String tag, Element element) {
		TestSession.logger.trace("XML tag =" + tag);
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		if (node == null) {
			TestSession.logger.info("XML node is null");
			return "";
		} else {
			try {
				String nodeValue = node.getNodeValue();
				if (nodeValue == null) {
					TestSession.logger.info("XML Node value for "
							+ node.getNodeName() + " is null.");
					return "";
				} else {
					return nodeValue;
				}
			} catch (Exception e) {
				TestSession.logger.info("Exception getting XML node value for "
						+ node.getNodeName() + ".");
				return "";
			}
		}
	}

	/**
	 * Set the xml node value given a tag and an element.
	 * 
	 * @param tag
	 *            Tag name in the xml element
	 * @param element
	 *            The xml element.
	 * @param value
	 *            tag name value to insert.
	 * 
	 * @return String node value.
	 */
	private static void setValue(String tag, Element element, String value) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		node.setNodeValue(value);
	}

	/**
	 * Insert into the xml node name and value pair given a tag and an element.
	 * 
	 * @param tag
	 *            Tag name in the xml element
	 * @param element
	 *            The xml element.
	 * @param value
	 *            tag name value to insert.
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
	 * @param propName
	 *            String of property name
	 * @param propValue
	 *            String of property value
	 * @param confFilename
	 *            String of the configuration file name.
	 * 
	 * @throws Exception
	 *             if the configuration file property can not be set.
	 */
	public void setHadoopConfFileProp(String propName, String propValue,
			String confFilename) throws Exception {
		setHadoopConfFileProp(propName, propValue, confFilename, null);
	}

	/**
	 * Set the Hadoop configuration file property for a given property name,
	 * property value, component, file name, and configuration directory path.
	 * 
	 * @param propName
	 *            String of property name
	 * @param propValue
	 *            String of property value
	 * @param confFilename
	 *            String of the configuration file name.
	 * @param confDir
	 *            String of the configuration directory path.
	 * 
	 * @throws Exception
	 *             if the configuration file property can not be set.
	 */
	public void setHadoopConfFileProp(String propName, String propValue,
			String confFilename, String confDir) throws Exception {
		String component = this.component;
		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir();
			if ((confDir == null) || confDir.isEmpty()) {
				/* Custom directory has not yet been set */
				TestSession.logger
						.warn("Custom configuration directory not set!!!");
			}
		}

		// We don't want to call propNameExistsInHadoopConfFile at this point
		// because the HadoopConfFileProp may not have been initialized at.
		// It is only initialized when the component reset is called.

		// We also don't want to set the property for the specified property
		// name and value in the internal Properties object because again it
		// may not exists yet. This needs to be done when the component reset
		// occurs.

		/* Copy the file to local if component is not gateway */
		String localConfDir = (component.equals(HadoopCluster.GATEWAY)) ? confDir
				: this.copyRemoteConfDirToLocal(confDir, component);

		/* Write to file */
		this.updateXmlConfFile(localConfDir + "/" + confFilename, propName,
				propValue);

		/* Copy the directory back to the remote host */
		if (!component.equals(HadoopCluster.GATEWAY)) {
			this.copyFilesToConfDir(localConfDir, component);
		}
	}
	public void removeHadoopConfFileProp(String propName,
			String confFilename, String confDir) throws Exception {
		String component = this.component;
		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir();
			if ((confDir == null) || confDir.isEmpty()) {
				/* Custom directory has not yet been set */
				TestSession.logger
						.warn("Custom configuration directory not set!!!");
			}
		}

		// We don't want to call propNameExistsInHadoopConfFile at this point
		// because the HadoopConfFileProp may not have been initialized at.
		// It is only initialized when the component reset is called.

		// We also don't want to set the property for the specified property
		// name and value in the internal Properties object because again it
		// may not exists yet. This needs to be done when the component reset
		// occurs.

		/* Copy the file to local if component is not gateway */
		String localConfDir = (component.equals(HadoopCluster.GATEWAY)) ? confDir
				: this.copyRemoteConfDirToLocal(confDir, component);

		/* Write to file */
		this.deleteElementFromXmlConfFile(localConfDir + "/" + confFilename, propName);

		/* Copy the directory back to the remote host */
		if (!component.equals(HadoopCluster.GATEWAY)) {
			this.copyFilesToConfDir(localConfDir, component);
		}
	}

	/**
	 * Set the Hadoop configuration file property for a given property name,
	 * property value, component, file name, and configuration directory path.
	 * Overloaded method to consume a map
	 * 
	 * @param propName
	 *            String of property name
	 * @param propValue
	 *            String of property value
	 * @param confFilename
	 *            String of the configuration file name.
	 * @param confDir
	 *            String of the configuration directory path.
	 * 
	 * @throws Exception
	 *             if the configuration file property can not be set.
	 */
	public void setHadoopConfFileProp(Map<String, String> keyValuePairs,
			String confFilename, String confDir) throws Exception {
		String component = this.component;
		if ((confDir == null) || confDir.isEmpty()) {
			confDir = this.getHadoopConfDir();
			if ((confDir == null) || confDir.isEmpty()) {
				/* Custom directory has not yet been set */
				TestSession.logger
						.warn("Custom configuration directory not set!!!");
			}
		}

		// We don't want to call propNameExistsInHadoopConfFile at this point
		// because the HadoopConfFileProp may not have been initialized at.
		// It is only initialized when the component reset is called.

		// We also don't want to set the property for the specified property
		// name and value in the internal Properties object because again it
		// may not exists yet. This needs to be done when the component reset
		// occurs.

		/* Copy the file to local if component is not gateway */
		String localConfDir = (component.equals(HadoopCluster.GATEWAY)) ? confDir
				: this.copyRemoteConfDirToLocal(confDir, component);

		/* Write to file */
		for (String aKey : keyValuePairs.keySet()) {
			this.updateXmlConfFile(localConfDir + "/" + confFilename, aKey,
					keyValuePairs.get(aKey));
		}

		/* Copy the directory back to the remote host */
		if (!component.equals(HadoopCluster.GATEWAY)) {
			this.copyFilesToConfDir(localConfDir, component);
		}
	}

	/**
	 * Insert or replace the property tag in the xml configuration file.
	 * 
	 * @param filename
	 *            the xml configuration file
	 * @param targetPropName
	 *            the target property tag
	 * @param targetPropValue
	 *            the value to replace the property tag with
	 * 
	 * @throws ParseConfigurationException
	 *             if the DocumentBuilderFactory can not get a new
	 *             DocumentBuilder
	 * @throws SAXException
	 *             if the DocumentBuilder can not parse the file
	 * @throws IOException
	 *             if the DocumentBuilder can not parse the file
	 * @throws TransformerException
	 *             if the Transformer can not transform the document
	 * @throws TransformerConfigurationException
	 *             if the TransformerFactory can not provide a new Transformer
	 *             instance
	 */
	public void updateXmlConfFile(String filename, String targetPropName,
			String targetPropValue) throws ParserConfigurationException,
			SAXException, IOException, TransformerException,
			TransformerConfigurationException {

		TestSession.logger.info("Insert/Replace property '" + targetPropName
				+ "'='" + targetPropValue + "' in file '" + filename + "'.");

		boolean foundPropName = false;

		/*
		 * Parse the XML configuration file using a DOM parser
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: "
				+ document.getDocumentElement().getNodeName());

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

				TestSession.logger.trace("Config Property Name: "
						+ getValue("name", element));
				TestSession.logger.trace("Config Property Value: "
						+ getValue("value", element));

				if (propName.equals(targetPropName)) {
					foundPropName = true;
					if (propValue.equals(targetPropValue)) {
						TestSession.logger.info("Propperty value for '"
								+ propName + "' is already set to '"
								+ propValue + "'.");
						return;
					} else {
						setValue("value", element, targetPropValue);

					}
				}
			}
		}

		if (foundPropName == false) {
			insertValue("name", "value", element, targetPropName,
					targetPropValue, document);
		}

		// Write the change to file
		Transformer xformer = TransformerFactory.newInstance().newTransformer();
		String outputFile = filename;
		xformer.transform(new DOMSource(document), new StreamResult(new File(
				outputFile)));

	}
	public void deleteElementFromXmlConfFile(String filename, String targetPropName) throws ParserConfigurationException,
			SAXException, IOException, TransformerException,
			TransformerConfigurationException {

		/*
		 * Parse the XML configuration file using a DOM parser
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: "
				+ document.getDocumentElement().getNodeName());

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

				TestSession.logger.trace("Config Property Name: "
						+ getValue("name", element));
				TestSession.logger.trace("Config Property Value: "
						+ getValue("value", element));

				if (propName.equals(targetPropName)) {
					element.getParentNode().removeChild(element);
					TestSession.logger.info("Removed element: (localName)" + element.getLocalName() + " (nodeName)" + element.getNodeName() + " (nodeValue)" + element.getNodeValue());
					break;
				}
			}
		}

		// Write the change to file
		Transformer xformer = TransformerFactory.newInstance().newTransformer();
		String outputFile = filename;
		xformer.transform(new DOMSource(document), new StreamResult(new File(
				outputFile)));

	}

	/**
	 * Copy a single file from a given Hadoop configuration directory to a
	 * Hadoop cluster gateway. This assumes that the cluster under test is
	 * already using a custom backup directory that is editable, by previously
	 * calling the backupConfDir() method.
	 * 
	 * @param sourceFile
	 *            source configuration file to copy
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error copying the configuration file.
	 */
	public boolean copyFileToConfDir(String sourceFile) throws Exception {
		return copyFileToConfDir(sourceFile, null);
	}

	/**
	 * Copy a single file from a given Hadoop configuration directory to a
	 * Hadoop cluster component. This assumes that the cluster under test is
	 * already using a custom backup directory that is editable, by previously
	 * calling the backupConfDir() method.
	 * 
	 * @param sourceFile
	 *            source configuration file to copy
	 * @param targetFile
	 *            target configuration file to copy to
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error copying the configuration file.
	 */
	public boolean copyFileToConfDir(String sourceFile, String targetFile)
			throws Exception {
		return copyToConfDir(sourceFile, targetFile);
	}

	/**
	 * Copy files from a given Hadoop configuration directory to a Hadoop
	 * cluster component. This assumes that the cluster under test is already
	 * using a custom backup directory that is editable, by previously calling
	 * the backupConfDir() method.
	 * 
	 * @param sourceDir
	 *            source configuration directory to copy the files from
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error copying the configuration file.
	 */
	public boolean copyFilesToConfDir(String sourceDir) throws Exception {
		return copyFilesToConfDir(sourceDir, null);
	}

	/**
	 * Copy files from a given Hadoop configuration directory to a Hadoop
	 * cluster component. This assumes that the cluster under test is already
	 * using a custom backup directory that is editable, by previously calling
	 * the backupConfDir() method.
	 * 
	 * @param sourceDir
	 *            source configuration directory to copy the files from
	 * @param component
	 *            cluster component such as gateway, namenode, resourcemanager,
	 *            etc.
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error copying the configuration file.
	 */
	public boolean copyFilesToConfDir(String sourceDir, String component)
			throws Exception {

		// TODO: Component is not used... this might be an artifact from the
		// previous design!!!

		if (!sourceDir.endsWith("/")) {
			sourceDir = sourceDir.concat("/");
		}
		return copyToConfDir(sourceDir + "*", null);
	}

	/**
	 * Copy file or files from a given Hadoop configuration directory to a
	 * Hadoop cluster component. This assumes that the cluster under test is
	 * already using a custom backup directory that is editable, by previously
	 * calling the backupConfDir() method.
	 * 
	 * @param sourceFiles
	 *            source configuration file or files to copy
	 * @param component
	 *            cluster component such as gateway, namenode, resourcemanager,
	 *            etc.
	 * @param daemonHost
	 *            String Array of component hostname(s).
	 * @param target
	 *            file name
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error copying the configuration file.
	 */
	private boolean copyToConfDir(String sourceFile, String targetFile)
			throws Exception {
		/*
		 * pdcp doesn't always work because it needs to be available on all of
		 * the Hadoop nodes and they are not installed by default. Alternative
		 * is use pdsh with scp copying from the gateway host to one or more of
		 * the target hosts. But this will require that the source directory on
		 * the gateway be in the full path, and not in relative path. If path is
		 * a relative path, it will be concatenated with the default root
		 * directory path of TestSession.conf.getProperty("WORKSPACE")
		 */
		if (!sourceFile.startsWith("/")) {
			sourceFile = TestSession.conf.getProperty("WORKSPACE") + "/"
					+ sourceFile;
		}

		if (!this.component.equals(HadoopCluster.GATEWAY)) {
			sourceFile = this.getHadoopProp("GATEWAY") + ":" + sourceFile;
		}

		/*
		 * target would either be the custom Hadoop configuration directory
		 * where files are to be copied to, or the actual full path filename to
		 * be copied to.
		 */

		String confDir = this.hadoopConfDir;

		String target = ((targetFile == null) || targetFile.isEmpty()) ? confDir
				: confDir + "/" + targetFile;

		String cpBin = (this.component.equals(HadoopCluster.GATEWAY)) ? "/bin/cp"
				: "/usr/bin/scp";
		String cpCmd[] = { cpBin, sourceFile, target };

		String[] cmd;
		if (this.component.equals(HadoopCluster.GATEWAY)) {
			TestSession.logger.info("Copy files(s) to the Hadoop conf dir '"
					+ confDir + "' on the gateway:");
			String[] cpCmd_ = { "/bin/sh", "-c",
					cpBin + " " + sourceFile + " " + target };
			cmd = cpCmd_;
		} else {
			TestSession.logger.info("Copy file(s) to the Hadoop "
					+ "conf dir '" + confDir + "' on " + "the '"
					+ this.component + "' component host of '" + this.hostname
					+ "'.");

			/*
			 * if conf dir is NFS mounted and shared across component hosts, do
			 * just on a single host
			 */
			boolean isConfDirNFS = true;
			String[] pdshCmd = { "pdsh", "-w", this.hostname };
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
			cmd = temp.toArray(new String[pdshCmd.length + cpCmd.length]);
		}
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		return output[0].equals("0") ? true : false;
	}

	public boolean insertBlock(String matchStr, String appendStr,
			String targetFile, String RS, String LS) throws Exception {
		String component = this.component;
		if ((component == null) || component.isEmpty()) {
			component = HadoopCluster.GATEWAY;
		}

		String[] daemonHost = null;
		if ((!component.equals(HadoopCluster.GATEWAY)) && (daemonHost == null)) {
			daemonHost = TestSession.cluster.getNodeNames(component);
		}

		String confDir = this.getHadoopConfDir();
		if (RS == null) {
			RS = "";
		}
		if (LS == null) {
			LS = "\n";
		}

		String perlCmd = "/usr/local/bin/perl -pi -0e 'BEGIN {$match_str=\""
				+ matchStr + "\"; $append_str=\"" + appendStr + "\"}; " + "s|"
				+ "(.*." + matchStr + ".*\n)|" + "${1}" + RS + appendStr + LS
				+ "|' " + targetFile;

		String insertCmd[] = { perlCmd };

		String[] cmd;
		if (component.equals(HadoopCluster.GATEWAY)) {
			TestSession.logger
					.info("Insert block to the Hadoop "
							+ "configuraiton directory " + confDir
							+ " on the gateway:");
			String[] insertCmd_ = { "/bin/sh", "-c", perlCmd };
			cmd = insertCmd_;
		} else {
			TestSession.logger.info("Insert block to the Hadoop "
					+ "configuration directory " + confDir + " on " + "the "
					+ component + " host(s) of " + Arrays.toString(daemonHost));

			/*
			 * if conf dir is NFS mounted and shared across component hosts, do
			 * just on a single host
			 */
			boolean isConfDirNFS = true;
			String targetHost = (isConfDirNFS) ? daemonHost[0] : StringUtils
					.join(daemonHost, ",");
			String[] pdshCmd = { "pdsh", "-w", targetHost };
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(insertCmd));
			cmd = temp.toArray(new String[pdshCmd.length + insertCmd.length]);
		}
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		return output[0].equals("0") ? true : false;
	}

	public boolean replaceBlock(String matchStr, String appendStr,
			String targetFile, String RS, String LS) throws Exception {
		String component = this.component;
		if ((component == null) || component.isEmpty()) {
			component = HadoopCluster.GATEWAY;
		}

		String[] daemonHost = null;
		if ((!component.equals(HadoopCluster.GATEWAY)) && (daemonHost == null)) {
			daemonHost = TestSession.cluster.getNodeNames(component);
		}

		String confDir = this.getHadoopConfDir();
		if (RS == null) {
			RS = "";
		}
		if (LS == null) {
			LS = "\n";
		}

		String perlCmd = "/usr/local/bin/perl -pi -0e 'BEGIN {$match_str=\""
				+ matchStr + "\"; $append_str=\"" + appendStr + "\"}; " + "s|"
				+ "(" + matchStr + ")|" + RS + appendStr + LS + "|' "
				+ targetFile;

		String replaceCmd[] = { perlCmd };

		String[] cmd;
		if (component.equals(HadoopCluster.GATEWAY)) {
			TestSession.logger
					.info("Insert block to the Hadoop "
							+ "configuraiton directory " + confDir
							+ " on the gateway:");
			String[] replaceCmd_ = { "/bin/sh", "-c", perlCmd };
			cmd = replaceCmd_;
		} else {
			TestSession.logger.info("Insert block to the Hadoop "
					+ "configuration directory " + confDir + " on " + "the "
					+ component + " host(s) of " + Arrays.toString(daemonHost));

			/*
			 * if conf dir is NFS mounted and shared across component hosts, do
			 * just on a single host
			 */
			boolean isConfDirNFS = true;
			String targetHost = (isConfDirNFS) ? daemonHost[0] : StringUtils
					.join(daemonHost, ",");
			String[] pdshCmd = { "pdsh", "-w", targetHost };
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(replaceCmd));
			cmd = temp.toArray(new String[pdshCmd.length + replaceCmd.length]);
		}
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		return output[0].equals("0") ? true : false;
	}

	/**
	 * Backup the Hadoop configuration directory for a given component. This
	 * will setup a new temporary Hadoop configuration directory where settings
	 * can be changed for testing. This is necessary because the default
	 * configuration directory is owned and writable only to root.
	 * 
	 * @param component
	 *            cluster component such as gateway, namenode, resourcemanager,
	 *            etc.
	 * @param daemonHost
	 *            String Array of component hostname(s).
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception
	 *             if there is a fatal error backing up the configuration
	 *             directory.
	 */
	public boolean backupConfDir() throws Exception {
		// New Hadoop conf dir
		String customConfDir = this.getHadoopProp("TMP_DIR") + "/hadoop-conf-"
				+ this.component + "-" + this.hostname + "."
				+ TestSession.getFileDateFormat(new Date());

		// Follow and dereference symlinks
		String cpCmd[] = { "/bin/cp", "-rfL", this.hadoopConfDir, customConfDir };

		// Change all the xml config files to have the proper permission so that
		// they can be changed by the tests later.
		String[] chmodCmd = { "/bin/chmod", "644", customConfDir + "/*.xml" };

		String[] cmd = null;
		if (this.component.equals(HadoopCluster.GATEWAY)) {
			TestSession.logger
					.info("Back up the Hadoop configuration directory on "
							+ "the gateway:");
			cmd = cpCmd;
		} else {
			TestSession.logger.info("Back up the Hadoop configuration "
					+ "directory to '" + customConfDir + "' on " + "the '"
					+ this.component + "' host of '" + this.hostname + "'.");
			String[] pdshCmd = { "pdsh", "-w", this.hostname };
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(Arrays.asList(pdshCmd));
			temp.addAll(Arrays.asList(cpCmd));
			temp.add(";");
			temp.addAll(Arrays.asList(chmodCmd));
			cmd = temp.toArray(new String[pdshCmd.length + cpCmd.length
					+ chmodCmd.length]);
		}

		// Run the command
		String output[] = TestSession.exec.runProcBuilder(cmd);
		TestSession.logger.trace(Arrays.toString(output));

		// Update the HadoopConfDir Properties
		this.setHadoopConfDir(customConfDir);

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
	 * @param Properties
	 *            a given Properties
	 */
	private void traceProperties(Properties prop) {
		TestSession.logger.trace("-- listing properties --");
		Enumeration<Object> keys = prop.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = (String) prop.get(key);
			TestSession.logger.trace(key + ": " + value);
		}
	}

}
