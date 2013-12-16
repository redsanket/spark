package hadooptest.cluster.gdm;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import coretest.Util;
import hadooptest.TestSession;

public class GdmUtils
{	
    private static final String TEST_CONFIGURATION = Util.getResourceFullPath(
			"gdm/conf/config.xml");
	private static Configuration conf;
	
	
	static {
		try {
     	    conf = new XMLConfiguration(TEST_CONFIGURATION);
		}
		catch (ConfigurationException e){
			conf = null;
			TestSession.logger.error("Test Configuration not found: " + TEST_CONFIGURATION , e);
		}
	}
	
	public static String getConfiguration(String path){
		return conf.getString(path);
	}
	
	public static String getFilePath(String paramString)
	{
		String fileName = paramString.substring(paramString.lastIndexOf("/")+1);
		URL localURL = GdmUtils.class.getClassLoader().getResource(fileName);
		if (localURL == null) {
			TestSession.logger.debug("File " + paramString + " not found in classpath");
			return paramString;
		}
		TestSession.logger.debug("File " + paramString + " was found in the classpath at path " + localURL.getPath());

		return localURL.getPath();
	}

	public static String readFile(String paramString)
	{
		String str1 = "";
		paramString = getFilePath(paramString);
		try {
			BufferedReader localBufferedReader = new BufferedReader(new FileReader(paramString));
			String str2 = "";
			while ((str2 = localBufferedReader.readLine()) != null) {
				str1 = str1 + str2;
			}
			localBufferedReader.close();
		} catch (FileNotFoundException localFileNotFoundException) {
			TestSession.logger.error(localFileNotFoundException.toString());
			throw new IllegalArgumentException(localFileNotFoundException.toString()); 
		} catch (IOException localIOException) {
			TestSession.logger.error(localIOException.toString());
		}
		TestSession.logger.debug("Read from " + paramString + ": " + str1);
		TestSession.logger.debug("Total Size: " + str1.length());
		return str1;
	}

	/**
	 * Copies the GDM web.xml necessary for headless users from the HTF
	 * resources to the correct location on the GDM node, using the 
	 * htf_gdm_setup package.
	 * 
	 * @throws Exception
	 */
	public static void copyWebXMLForHeadless() throws Exception {
		TestSession.logger.info("Installing htf_gdm_setup package...");
		TestSession.exec.runProcBuilder(new String[] {"/usr/local/bin/yinst", 
				"i", "htf_gdm_setup", 
				"-br", "current", "-live" });
		TestSession.logger.info("Running yinst directive to copy headless web.xml configuration for GDM...");
		TestSession.exec.runProcBuilder(new String[] { "sudo", 
				"/usr/local/bin/yinst", 
				"start", "htf_gdm_setup" });
	} 
	
	/**
	 * Triggers a restart of the GDM console, using the htf_gdm_console_restart
	 * yinst package.
	 * 
	 * @throws Exception
	 */ 
	public static void restartConsole() throws Exception {
		TestSession.logger.info("Installing htf_gdm_console_restart package...");
		TestSession.exec.runProcBuilder(new String[] {"/usr/local/bin/yinst", 
				"i", "htf_gdm_console_restart", 
				"-br", "current", "-live" });
		TestSession.logger.info("Running yinst directive to restart GDM console...");
		TestSession.exec.runProcBuilder(new String[] { "sudo", 
				"/usr/local/bin/yinst", 
				"start", "htf_gdm_console_restart" });
	}
	
	public static Calendar getCalendar() {
		Calendar localCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		return localCalendar;
	}

	public static String getCalendarAsString(Calendar paramCalendar) {
		SimpleDateFormat localSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		localSimpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		return localSimpleDateFormat.format(paramCalendar.getTime());
	}

	private static final String treatSingleDigits(int paramInt) {
		return "0" + String.valueOf(paramInt);
	}

	public static String getCalendarAsString() {
		Calendar localCalendar = Calendar.getInstance();

		SimpleDateFormat localSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		localSimpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		String str = localSimpleDateFormat.format(localCalendar.getTime());
		TestSession.logger.debug(str);
		return str;
	}
	
	
	public static void customizeSpecificationConf( 
			String gdmPrefix, String acqCluster, String replCluster, String specification) throws Exception {

		String specificationXML = 
				Util.getResourceFullPath("gdm/datasetconfigs") + "/" + 
						"VerifyAcqRepRetWorkFlowExecutionSingleDate" + 
						"_specification.xml";

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.parse(specificationXML);

		Node dataSet = doc.getElementsByTagName("DataSet").item(0);
		NamedNodeMap attr = dataSet.getAttributes();
		Node nodeAttr = attr.getNamedItem("description");
		nodeAttr.setTextContent("gdm-dataset-" + gdmPrefix);

		Node parameters1 = doc.getElementsByTagName("attribute").item(0);
		attr = parameters1.getAttributes();
		nodeAttr = attr.getNamedItem("value");
		nodeAttr.setTextContent("gdm-dataset-" + gdmPrefix);
		
		Node parameters2 = doc.getElementsByTagName("attribute").item(3);
		attr = parameters2.getAttributes();
		nodeAttr = attr.getNamedItem("value");
		nodeAttr.setTextContent("gdm-dataset-" + gdmPrefix + "_stats");

		Node sourceNode = doc.getElementsByTagName("Source").item(0);
		attr = sourceNode.getAttributes();
		nodeAttr = attr.getNamedItem("name");
		nodeAttr.setTextContent("gdm-fdi-source-" + gdmPrefix);
		

		Node acqClusterNode = doc.getElementsByTagName("Target").item(0);
		attr = acqClusterNode.getAttributes();
		nodeAttr = attr.getNamedItem("name");
		nodeAttr.setTextContent("gdm-target-" + acqCluster + "-" + gdmPrefix);
		

		Node replClusterNode = doc.getElementsByTagName("Target").item(1);
		attr = replClusterNode.getAttributes();
		nodeAttr = attr.getNamedItem("name");
		nodeAttr.setTextContent("gdm-target-" + replCluster + "-" + gdmPrefix);
		
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(specificationXML));
		transformer.transform(source, result);
	}

	public static void customizeTestConf(String gdmConsoleHost, int gdmConsolePort)
			throws Exception {

		String configXML = Util.getResourceFullPath("gdm/conf/config.xml");

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.parse(configXML);

		Node xmlBaseUrl = doc.getElementsByTagName("base_url").item(0);
		xmlBaseUrl.setTextContent("http://" + gdmConsoleHost + ":" + gdmConsolePort);

		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(configXML));
		transformer.transform(source, result);
	}
}
