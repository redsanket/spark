package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;

import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class YarnTestsBaseClass extends TestSession {
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	public final String KRB5CCNAME = "KRB5CCNAME";
	protected String localCluster = System.getProperty("CLUSTER_NAME");

	public static enum YarnAdminSubCommand {
		REFRESH_QUEUES, REFRESH_NODES, REFRESH_SUPERUSER_GROUPS_CONFIGURATION, REFRESH_USER_TO_GROUPS_MAPPING, REFRESH_ADMIN_ACLS, REFRESH_SERVICE_ACL, GET_GROUPS
	};

	
	/*
	 * Run a Sleep Job, from org.apache.hadoop.mapreduce
	 */
	public void runStdSleepJob(HashMap<String, String> jobParams, String[] args)
			throws Exception {
		TestSession.logger.info("running SleepJob.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new SleepJob(), args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			TestSession.logger.error("SleepJob barfed...");
			throw e;
		}

	}

	/*
	 * Run a RandomWriter Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopRandomWriter(HashMap<String, String> jobParams,
			String randomWriterOutputDirOnHdfs) throws Exception {
		TestSession.logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		for (String key : jobParams.keySet()) {
			conf.set(key, jobParams.get(key));
		}
		int res;
		try {
			res = ToolRunner.run(conf, new RandomWriter(),
					new String[] { randomWriterOutputDirOnHdfs });
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	/*
	 * Run a sort Job, from package org.apache.hadoop.examples;
	 */
	public void runStdHadoopSortJob(String sortInputDataLocation,
			String sortOutputDataLocation) throws Exception {
		TestSession.logger.info("running Sort Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res;

		try {
			res = ToolRunner.run(conf, new Sort<Text, Text>(), new String[] {

			sortInputDataLocation, sortOutputDataLocation });
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}

	public void runStdHadoopStreamingJob(String... args) throws Exception {
		TestSession.logger.info("running Streaming Job.................");
		Configuration conf = TestSession.cluster.getConf();
		int res;

		try {
			StreamJob job = new StreamJob();
			res = ToolRunner.run(conf, job, args);
			Assert.assertEquals(0, res);
		} catch (Exception e) {
			throw e;
		}

	}
	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		if (node == null) {
			return "";
		} else {
			return node.getNodeValue();
		}
	}

	public static String lookupValueInBackCopiedCapacitySchedulerXmlFile(String filename, String propertyName)
			throws ParserConfigurationException, SAXException, IOException,
			TransformerException, TransformerConfigurationException {

		boolean foundPropName = false;
		String valueToReturn = "";
		TestSession.logger.info("Looking up value in file:" + filename);
		/*
		 * Parse the XML configuration file using a DOM parser
		 */
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbFactory.newDocumentBuilder();
		Document document = db.parse(filename);
		document.getDocumentElement().normalize();
		TestSession.logger.trace("Root of xml file: "
				+ document.getDocumentElement().getNodeName());

		Element element = null;
		NodeList nodes = document.getElementsByTagName("property");
		for (int index = 0; index < nodes.getLength(); index++) {
			Node node = nodes.item(index);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				element = (Element) node;

				String propName = getValue("name", element);

				TestSession.logger.trace("Config Property Name: "
						+ getValue("name", element));
				TestSession.logger.trace("Config Property Value: "
						+ getValue("value", element));

				if (propName.equals(propertyName)) {
					foundPropName = true;
					TestSession.logger
							.info("Found what I was looking for aka ["
									+ propertyName + "] returning");
					valueToReturn = getValue("value", element);
				}
			}
		}

		if (foundPropName == false) {
			return "";
		} else {
			return valueToReturn;
		}

	}
	
//	public static Configuration createConfigObjFromBackCopiedConfFilesForCapacityScheduler(String dirWhereConfFilesHaveBeenBackCopied){
//		Configuration backCopiedConf = new Configuration();
//		backCopiedConf.addResource(new Path(dirWhereConfFilesHaveBeenBackCopied + HadooptestConstants.ConfFileNames.CORE_SITE_XML));
//		backCopiedConf.addResource(new Path(dirWhereConfFilesHaveBeenBackCopied + HadooptestConstants.ConfFileNames.HDFS_SITE_XML));
//		backCopiedConf.addResource(new Path(dirWhereConfFilesHaveBeenBackCopied + HadooptestConstants.ConfFileNames.MAPRED_SITE_XML));
//		backCopiedConf.addResource(new Path(dirWhereConfFilesHaveBeenBackCopied + HadooptestConstants.ConfFileNames.YARN_SITE_XML));
//		backCopiedConf.addResource(new Path(dirWhereConfFilesHaveBeenBackCopied + HadooptestConstants.ConfFileNames.CAPACITY_SCHEDULER_XML));
//		
//		
//		return backCopiedConf;
//	}

	@Override
	public void logTaskReportSummary() {
		// Override the job summary, as it is not required
	}

}
