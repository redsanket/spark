// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DataSetTarget {
    private String name;
    private Document doc;
    private boolean addPathFlag = false;
    private String dateRangeStartType;
    private String dateRangeStartValue;
    private String dateRangeEndType;
    private String dateRangeEndValue;
    private String hcatType;
    private Map<String,String> paths = new HashMap<String,String>();
    private String numInstances;
    private String replicationStrategy = "HFTPDistributedCopy";
    
    public DataSetTarget() {
    }
    
    /**
     * Creates an XML element representing a dataset target
     * @param doc
     */
    public Element buildElement(Document doc) throws Exception {
        this.doc = doc;
        Element target = doc.createElement("Target");
        this.addRequiredAttribute(target, "name", this.name);
        this.addRequiredAttribute(target, "latency", "200");
        this.addRequiredAttribute(target, "status", "active");
        
        Element dateRange = this.appendRequiredElement(target, "DateRange");
        Element endDate = this.appendRequiredElement(dateRange, "EndDate");
        this.addRequiredAttribute(endDate, "type", this.dateRangeEndType);
        this.addRequiredAttribute(endDate, "value", this.dateRangeEndValue);
        Element startDate = this.appendRequiredElement(dateRange, "StartDate");
        this.addRequiredAttribute(startDate, "type", this.dateRangeStartType);
        this.addRequiredAttribute(startDate, "value", this.dateRangeStartValue);
        
        this.appendRequiredElement(target, "HCatTargetType", this.hcatType);
        
        if (this.addPathFlag) {
        	Element paths = this.appendRequiredElement(target, "Paths");
          Iterator pathIterator = this.paths.entrySet().iterator();
          while (pathIterator.hasNext()) {
              Map.Entry entry = (Map.Entry)pathIterator.next();
              Element path = this.appendRequiredElement(paths, "Path");
              this.addRequiredAttribute(path, "location", (String)entry.getValue());
              this.addRequiredAttribute(path, "type", (String)entry.getKey());
              pathIterator.remove();
          }	
        }
        
        Element policies = this.appendRequiredElement(target, "Policies");
        Element policy = this.appendRequiredElement(policies, "Policy");
        String condition = "numberOfInstances(instancelist, instance) > " + this.numInstances;
        this.addRequiredAttribute(policy, "condition", condition);
        this.addRequiredAttribute(policy, "type", "retention");
        
        this.appendRequiredElement(target, "ReplicationStrategy", this.replicationStrategy);
        
        Element resource = this.appendRequiredElement(target, "Resource");
        this.addRequiredAttribute(resource, "capacity", "10");
        this.addRequiredAttribute(resource, "name", "bandwidth");
        
        return target;
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
    
    private void addRequiredAttribute(Element element, String attributeName, String attributeValue) throws Exception {
        if (attributeValue == null) {
            throw new Exception(attributeName + " requires a value!");
        }
        Attr attr = doc.createAttribute(attributeName);
        attr.setValue(attributeValue);
        element.setAttributeNode(attr);
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Sets the target start date range
     * @param fixed  true if the date range is fixed, false otherwise
     * @param value  the value to use for the daterange
     */
    public void setDateRangeStart(boolean fixed, String value) {
        if (fixed) {
            this.dateRangeStartType = "fixed";
        } else {
            this.dateRangeStartType = "offset";
        }
        this.dateRangeStartValue = value;
    }
    
    /**
     * Sets the target end date range
     * @param fixed  true if the date range is fixed, false otherwise
     * @param value  the value to use for the daterange
     */
    public void setDateRangeEnd(boolean fixed, String value) {
        if (fixed) {
            this.dateRangeEndType = "fixed";
        } else {
            this.dateRangeEndType = "offset";
        }
        this.dateRangeEndValue = value;
    }
    
    public void setHCatType(String hcatType) {
        this.hcatType = hcatType;
    }
    
    /**
     * Adds a path override to the target
     * @param pathType  
     * @param pathLocation
     */
    public void addPath(String pathType, String pathLocation) {
        this.paths.put(pathType, pathLocation);
        this.addPathFlag = true;
    }
    
    public void setNumInstances(String numInstances) {
        this.numInstances = numInstances;
    }
    
    public void setReplicationStrategy(String replicationStrategy) {
        this.replicationStrategy = replicationStrategy;
    }
}

