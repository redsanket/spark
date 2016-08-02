package hadooptest.gdm.regression.integration;


import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

import hadooptest.TestSession;
import hadooptest.Util;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class CreateIntegrationDataSet {

    private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_Daily/";
    private Document document = null;
    private String dataPath = null;
    private String hcatType = null;
    private String dataSetName = null;
    private String dataSetPath;
    private List<String> targeList;
    
    public CreateIntegrationDataSet() {
    	String absolutePath = new File("").getAbsolutePath();
    	String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/IntegrationBaseDataSet.xml");
    	TestSession.logger.info("dataSetConfigFile  = " + dataSetConfigFile);
        File file = new File(dataSetConfigFile);
        if (!file.exists()) {
            try {
                throw new FileNotFoundException(file.toString() + " does not exists");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        DocumentBuilder builder = null;
        try {
            //Get the DOM Builder Factory
            DocumentBuilderFactory factory =  DocumentBuilderFactory.newInstance();
            builder = factory.newDocumentBuilder();
            Document doc = builder.parse(dataSetConfigFile);
            this.setDocument(doc);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setDataSetPath(String dataSetPath) {
    	this.dataSetPath = dataSetPath;
    }
    
    public String getDataSetPath() {
    	return this.dataSetPath;
    }

    public void setDataSetName(String dataSetName){
    	this.dataSetName = dataSetName;
    }
    
    public String getDataSetName() {
    	return this.dataSetName;
    }
    
    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public void setTargeList(List<String> targetList) {
        this.targeList = targetList;
    }

    public List<String> getTargeList() {
        return this.targeList;
    }

    public void setHcatType(String hcatType) {
        this.hcatType = hcatType;
    }

    public String getHcatType() {
        return hcatType;
    }

    public void setDocument(Document doc) {
        this.document = doc;
    }

    public Document getDocument() {
        return this.document;
    }

    public NodeList getBaseDataSetNodeList() {
        NodeList nodeList = this.getDocument().getDocumentElement().getChildNodes();
        return nodeList;
    }

    public Node getNode(String tagName, NodeList nodes) {
        for ( int x = 0; x < nodes.getLength(); x++ ) {
            Node node = nodes.item(x);
            String nodeName  = node.getNodeName();
            TestSession.logger.debug("node name -  " + nodeName);
            if (node.getNodeName().equalsIgnoreCase(tagName)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Create a new node and attach it to the specified parenet node
     * @param parentNode parent node to which the new node is added a child
     * @param nodeName name of the new node
     * @param attributeNameAndValue  name of the attribute and value as array example : name , qe6blue
     * @return
     */
    public Element createNewNode(Node parentNode, String nodeName , String... attributeNameAndValue) {
        Element newElement = this.getDocument().createElement(nodeName);
        if (newElement != null) {
            int index = 0;
            while ( index < attributeNameAndValue.length ) {
                String attName = attributeNameAndValue[index++];
                String attValue = attributeNameAndValue[index++];
                newElement.setAttribute(attName,attValue);
            }
        }
        return newElement;
    }

    public Element createNewNode(String nodeName) {
        Element newElement = this.getDocument().createElement(nodeName);
        return newElement;
    }

    public void writeModifiedDocumentToFile() {
        OutputFormat outputFormat = new OutputFormat();
        outputFormat.setIndenting(true);
        String absoultePath = new File("").getAbsolutePath();
        
        Calendar dataSetCal = Calendar.getInstance();
        SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
        feed_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        long dataSetHourlyTimeStamp = Long.parseLong(feed_sdf.format(dataSetCal.getTime()));
        String clusterName = this.getTargeList().get(0);
        String dSName = clusterName + "_Integration_Testing_DS_" + dataSetHourlyTimeStamp + "00";
        this.setDataSetName(dSName);
        String newDataSetXml = absoultePath + "/resources/gdm/datasetconfigs/" + this.getDataSetName() + ".xml";
        File newFile = new File(newDataSetXml);
        if (newFile.exists()) {
            newFile.delete();
        } else {
            try {
                newFile.createNewFile();
                TestSession.logger.info("newDataSetXml  = " + newDataSetXml);
                XMLSerializer serializer = new XMLSerializer(new FileOutputStream(newFile),outputFormat);
                serializer.serialize(this.getDocument());
                try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
                if (newFile.exists()) {
                    TestSession.logger.info("File created");
                    this.setDataSetPath(newDataSetXml);
                } else {
                    TestSession.logger.error("File not created");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public void createDataSet() {
    	 Document doc = this.getDocument();
         TestSession.logger.info(doc.toString());
         Document document = this.getDocument();
         NodeList nodeList = this.getBaseDataSetNodeList();
         Node targetsNode = this.getNode("Targets" , nodeList);
         
    	 for ( String cName : this.getTargeList()) {
             Element newTargetElement  = this.createNewNode(targetsNode, "Target", "latency", "1440", "name", cName, "status", "active");
             Element dateRangeElement = this.createNewNode("DateRange");
             Element startDateElement = this.createNewNode(dateRangeElement , "StartDate" , "type" , "fixed" , "value" , "20120125");
             Element endtDateElement = this.createNewNode(dateRangeElement , "EndDate" , "type" , "fixed" , "value" , "20220131");
             dateRangeElement.appendChild(startDateElement);
             dateRangeElement.appendChild(endtDateElement);
             newTargetElement.appendChild(dateRangeElement);

             Element hcatTypeElement = this.createNewNode("HCatTargetType");
             hcatTypeElement.setTextContent(this.getHcatType());
             newTargetElement.appendChild(hcatTypeElement);

             // create and add paths tag
             Element pathsElement = this.createNewNode("Paths");
             newTargetElement.appendChild(pathsElement);

             // create and add path tags
             Element countPath = this.createNewNode(pathsElement , "Path" , "location" , "/data/daqdev/abf/count/${DataSetName}/%{date}" , "type" , "count");
             Element dataPath = this.createNewNode(pathsElement ,  "Path" ,"location" , "/data/daqdev/abf/data/${DataSetName}/%{date}" , "type" , "data");
             Element schemaPath = this.createNewNode(pathsElement ,  "Path" , "location" , "/data/daqdev/abf/schema/${DataSetName}/%{date}" , "type" , "schema");
             pathsElement.appendChild(countPath);
             pathsElement.appendChild(dataPath);
             pathsElement.appendChild(schemaPath);

             // policies element
             Element policiesElement = this.createNewNode("Policies");
             targetsNode.appendChild(policiesElement);

             // policy element
             Element policyElement = this.createNewNode(policiesElement , "Policy" , "condition", "instanceDate(instancelist, instance) > 5" , "type" , "retention");
             policiesElement.appendChild(policyElement);
             newTargetElement.appendChild(policiesElement);

             Element resourceElement = this.createNewNode(newTargetElement, "Resource" , "capacity" , "10" , "name" , "bandwidth");
             newTargetElement.appendChild(resourceElement);

             targetsNode.appendChild(newTargetElement);
         }
         this.writeModifiedDocumentToFile();
    }

    public static void main(String... args) {
    	CreateIntegrationDataSet obj = new CreateIntegrationDataSet();
        Document doc = obj.getDocument();
        TestSession.logger.debug(doc.toString());
        Document document = obj.getDocument();
        NodeList nodeList = obj.getBaseDataSetNodeList();
        Node targetsNode = obj.getNode("Targets" , nodeList);
        TestSession.logger.info(targetsNode.getNodeName());
        obj.setHcatType("Mixed");
        String []clusterName = {"qe6blue" , "openqe44blue", "openqe55blue" };
        obj.setTargeList(java.util.Arrays.asList(clusterName));

        for ( String cName : obj.getTargeList()) {
            Element newTargetElement  = obj.createNewNode(targetsNode, "Target", "latency", "1440", "name", cName, "status", "active");
            Element dateRangeElement = obj.createNewNode("DateRange");
            Element startDateElement = obj.createNewNode(dateRangeElement , "StartDate" , "type" , "fixed" , "value" , "20120125");
            Element endtDateElement = obj.createNewNode(dateRangeElement , "EndDate" , "type" , "fixed" , "value" , "20220131");
            dateRangeElement.appendChild(startDateElement);
            dateRangeElement.appendChild(endtDateElement);
            newTargetElement.appendChild(dateRangeElement);

            Element hcatTypeElement = obj.createNewNode("HCatTargetType");
            hcatTypeElement.setTextContent(obj.getHcatType());
            newTargetElement.appendChild(hcatTypeElement);

            // create and add paths tag
            Element pathsElement = obj.createNewNode("Paths");
            newTargetElement.appendChild(pathsElement);

            // create and add path tags
            Element countPath = obj.createNewNode(pathsElement , "Path" , "location" , "/data/daqdev/abf/count/${DataSetName}/%{date}" , "type" , "count");
            Element dataPath = obj.createNewNode(pathsElement ,  "Path" ,"location" , "/data/daqdev/abf/data/${DataSetName}/%{date}" , "type" , "data");
            Element schemaPath = obj.createNewNode(pathsElement ,  "Path" , "location" , "/data/daqdev/abf/schema/${DataSetName}/%{date}" , "type" , "schema");
            pathsElement.appendChild(countPath);
            pathsElement.appendChild(dataPath);
            pathsElement.appendChild(schemaPath);

            // policies element
            Element policiesElement = obj.createNewNode("Policies");
            targetsNode.appendChild(policiesElement);

            // policy element
            Element policyElement = obj.createNewNode(policiesElement , "Policy" , "condition", "instanceDate(instancelist, instance) > 1" , "type" , "retention");
            policiesElement.appendChild(policyElement);
            newTargetElement.appendChild(policiesElement);

            Element resourceElement = obj.createNewNode(newTargetElement, "Resource" , "capacity" , "10" , "name" , "bandwidth");
            newTargetElement.appendChild(resourceElement);

            targetsNode.appendChild(newTargetElement);
        }
        obj.writeModifiedDocumentToFile();
    }
}
