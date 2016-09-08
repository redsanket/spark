// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import hadooptest.TestSession;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.junit.Assert;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class DataSetXmlGenerator {
    private Document doc;
    private String name;
    private String description;
    private String catalog;
    private String active;
    private String retentionEnabled;
    private String priority;
    private String frequency;
    private String discoveryFrequency;
    private String discoveryInterface;
    private String hcatTableName;
    private String hcatDbName;
    private String hcatForceExternalTables = "FALSE";
    private String hcatInstanceKey = "instancedate";
    private String hcatRunTargetFilter = "FALSE";
    private String hcatTablePropagationEnabled = "FALSE";
    private String source;
    private DataSetTarget target;
    private Map<String,String> sourcePaths = new HashMap<String,String>();
    private Map<String,String> parameters = new HashMap<String,String>();
    private String owner = "dfsload";
    private String group = "users";
    private String permission = "755";
    
    public DataSetXmlGenerator() {
    }
    
    /**
     * Get the XML for a dataset
     * @return the dataset XML
     */
    public String getXml() {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            this.doc = docBuilder.newDocument();
    
            // root elements
            Element dataset = doc.createElement("DataSet");
            doc.appendChild(dataset);
            
            this.addRequiredAttribute(dataset, "name", this.name);
            this.addRequiredAttribute(dataset, "description", this.description);
            this.addRequiredAttribute(dataset, "catalog", this.catalog);
            this.appendRequiredElement(dataset, "Active", this.active);
            this.appendRequiredElement(dataset, "RetentionEnabled", this.retentionEnabled);
            this.appendRequiredElement(dataset, "Priority", this.priority);
            Element ugi = this.appendRequiredElement(dataset, "UGI");
            this.addRequiredAttribute(ugi, "owner", this.owner);
            this.addRequiredAttribute(ugi, "group", this.group);
            this.addRequiredAttribute(ugi, "permission", this.permission);
            
            this.appendRequiredElement(dataset, "Frequency", this.frequency);
            this.appendRequiredElement(dataset, "DiscoveryFrequency", this.discoveryFrequency);
            this.appendRequiredElement(dataset, "DiscoveryInterface", this.discoveryInterface);
            Element paths = this.appendRequiredElement(dataset, "Paths");
            
            Iterator sourcePathIterator = sourcePaths.entrySet().iterator();
            while (sourcePathIterator.hasNext()) {
                Map.Entry entry = (Map.Entry)sourcePathIterator.next();
                Element path = this.appendRequiredElement(paths, "Path");
                this.addRequiredAttribute(path, "location", (String)entry.getValue());
                this.addRequiredAttribute(path, "type", (String)entry.getKey());
                sourcePathIterator.remove();
            }
            
            Element parametersElement = this.appendRequiredElement(dataset, "Parameters");
            Iterator parameterIterator = parameters.entrySet().iterator();
            while (parameterIterator.hasNext()) {
                Map.Entry entry = (Map.Entry)parameterIterator.next();
                Element attribute = this.appendRequiredElement(parametersElement, "attribute");
                this.addRequiredAttribute(attribute, "name", (String)entry.getKey());
                this.addRequiredAttribute(attribute, "value", (String)entry.getValue());
                parameterIterator.remove();
            }
            
            Element sources = this.appendRequiredElement(dataset, "Sources");
            
            Element source = this.appendRequiredElement(sources, "Source");
            this.addRequiredAttribute(source, "latency", "100");
            this.addRequiredAttribute(source, "name", this.source);
            this.addRequiredAttribute(source, "switchovertype", "Standard");
            
            Element targets = this.appendRequiredElement(dataset, "Targets");
            Element target = this.target.buildElement(this.doc);
            targets.appendChild(target);
            
            NodeList nl = target.getElementsByTagName("HCatTargetType");
            boolean hcatFlag = false;
            for ( int i=0;i<nl.getLength();i++) {
            	String targetType  = nl.item(i).getTextContent().trim();
            	if (targetType.equals("HCatOnly") ||  targetType.equals("Mixed")) {
            		hcatFlag = true;
            		break;
            	}
            }
            
            if (hcatFlag) {
            	Element hcatElement = this.appendRequiredElement(dataset, "HCat");
            	this.appendRequiredElement(hcatElement, "HCatDatabaseName", this.hcatDbName);
            	this.appendRequiredElement(hcatElement, "HCatForceExternalTables", this.hcatForceExternalTables);
            	this.appendRequiredElement(hcatElement, "HCatInstanceKey", this.hcatInstanceKey);
            	this.appendRequiredElement(hcatElement, "HCatRunTargetFilter", this.hcatRunTargetFilter);
            	this.appendRequiredElement(hcatElement, "HCatTableName", this.hcatTableName);
            	this.appendRequiredElement(hcatElement, "HCatTablePropagationEnabled", this.hcatTablePropagationEnabled);
            }
            
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            String output = writer.getBuffer().toString().replaceAll("\n|\r", "");
            TestSession.logger.info("Dataset xml: " + output);
            return output;
        } catch (Exception e) {
            TestSession.logger.error("Unexpected exception", e);
            Assert.fail("Unexpected exception: " + e.getMessage());
            return null;
        }
    }
    
    private void addRequiredAttribute(Element element, String attributeName, String attributeValue) throws Exception {
        if (attributeValue == null) {
            throw new Exception(attributeName + " requires a value!");
        }
        Attr attr = doc.createAttribute(attributeName);
        attr.setValue(attributeValue);
        element.setAttributeNode(attr);
    }
    
    private void appendRequiredElement(Element element, String elementName, String elementValue) throws Exception {
        if (elementValue == null) {
            throw new Exception(elementName + " requires a value!");
        }
        Element subElement = doc.createElement(elementName);
        subElement.appendChild(doc.createTextNode(elementValue));
        element.appendChild(subElement);
    }
    
    private Element appendRequiredElement(Element element, String elementName) throws Exception {
        Element subElement = doc.createElement(elementName);
        element.appendChild(subElement);
        return subElement;
    }
    
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public void setActive(String active) {
        this.active = active;
    }
    
    public void setRetentionEnabled(String retentionEnabled) {
        this.retentionEnabled = retentionEnabled;
    }
    
    public void setPriority(String priority) {
        this.priority = priority;
    }
    
    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }
    
    public void setDiscoveryFrequency(String discoveryFrequency) {
        this.discoveryFrequency = discoveryFrequency;
    }
    
    public void setDiscoveryInterface(String discoveryInterface) {
        this.discoveryInterface = discoveryInterface;
    }
    
    /**
     * Add a path to the dataset source
     * @param pathType
     * @param pathLocation
     */
    public void addSourcePath(String pathType, String pathLocation) {
        this.sourcePaths.put(pathType, pathLocation);
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public void setTarget(DataSetTarget target) {
        this.target = target;
    }
    
    /**
     * Add a parameter to the dataset
     * @param key
     * @param value
     */
    public void addParameter(String key, String value) {
        this.parameters.put(key, value);
    }
    
    public void setOwner(String owner) {
        this.owner = owner;
    }
    
    public void setGroup(String group) {
        this.group = group;
    }
    
    public void setPermission(String permission) {
        this.permission = permission;
    }

		public void setHcatTableName(String hcatTableName) {
			this.hcatTableName = hcatTableName;
		}

		public void setHcatDbName(String hcatDbName) {
			this.hcatDbName = hcatDbName;
		}

		public void setHcatForceExternalTables(String hcatForceExternalTables) {
			this.hcatForceExternalTables = hcatForceExternalTables;
		}

		public void setHcatInstanceKey(String hcatInstanceKey) {
			this.hcatInstanceKey = hcatInstanceKey;
		}

		public void setHcatRunTargetFilter(String hcatRunTargetFilter) {
			this.hcatRunTargetFilter = hcatRunTargetFilter;
		}

		public void setHcatTablePropagationEnabled(String hcatTablePropagationEnabled) {
			this.hcatTablePropagationEnabled = hcatTablePropagationEnabled;
		}
}

