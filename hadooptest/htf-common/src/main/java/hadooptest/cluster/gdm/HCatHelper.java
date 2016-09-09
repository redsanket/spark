// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

/*
 * HCat helper class, implemented using HCAT data discovery API.
 */
public class HCatHelper {
    
    private ConsoleHandle consoleHandle = null;
    private HTTPHandle httpHandle = null;
    private Configuration conf = null;
    private static final String HCAT_TABLE_LIST_API = "/api/admin/hcat/table/list";
    private static final String HCAT_TABLE_PARTITION_API = "/api/admin/hcat/partition/list";

    public HCatHelper() {
        this.httpHandle = new HTTPHandle();
        this.consoleHandle = new ConsoleHandle();
        this.init();
    }
    
    private void init() {
        String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
        try {
            this.conf = new XMLConfiguration(configPath);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns true if HCAT table is created on a specified hcat server and dataset name.
     * @param hcatServerName
     * @param datasetName
     * @return
     */
    public boolean isTableExists(String hcatServerName , String datasetName , String dataBaseName) {
        String tableName = datasetName.toLowerCase().trim();
        String url = "http://"+  hcatServerName + ":" + this.consoleHandle.getFacetPortNo("console") + "/hcatalog/v1/ddl/database/" + dataBaseName.trim()  + "/table";
        TestSession.logger.info("Check hcat table created url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        JSONArray array = jsonObject.getJSONArray("tables");
        boolean isTableExist = false;
        for ( Object obj : array) {
            if (obj.toString().equals(tableName)) {
                isTableExist = true;
                TestSession.logger.info("Found table - " + tableName);
                break;
            }
        }
        return isTableExist;
    }

    /**
     * Query the given HCAT server whether partition InstanceId is created for the specified table name 
     * @param dataSourceName
     * @param tableName
     * @param instanceID
     * @return
     */
    public boolean isPartitionIDExists(String databaseName , String dataSourceName , String tableName , String instanceID ) {
        boolean instanceIdCreated = false;
        String url = "http://" +  dataSourceName + ":" + this.consoleHandle.getFacetPortNo("console") + "/hcatalog/v1/ddl/database/"+ databaseName +"/table/" + tableName.toLowerCase() + "/partition";
        TestSession.logger.info("Check for table partition url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        JSONArray jsonArray = jsonObject.getJSONArray("partitions");
        for ( Object obj : jsonArray) {
            TestSession.logger.info(obj);
            String partitionStr = obj.toString();
            JSONObject jsonObject1 =  (JSONObject) JSONSerializer.toJSON(partitionStr);
            JSONArray valuesJsonArray = jsonObject1.getJSONArray("values");
            for ( Object instanceDateObj : valuesJsonArray) {
                String instanceDateStr = instanceDateObj.toString();
                JSONObject instanceDateJSONObject = (JSONObject) JSONSerializer.toJSON(instanceDateStr);
                String columnValue = instanceDateJSONObject.getString("columnValue").trim();
                TestSession.logger.info("columnValue  = "  + columnValue);
                if (columnValue.equals(instanceID))  {
                    instanceIdCreated = true;
                    break;
                }
            }
        }
        return instanceIdCreated;
    }
    
    /**
     * Get Partition of a given table name
     * @param dataSourceName
     * @param tableName
     * @return
     */
    public JSONArray getAllHCatTableParitions(String databaseName , String dataSourceName , String tableName) {
        JSONArray jsonArray = null;
        String url = "http://" +  dataSourceName + ":" + this.consoleHandle.getFacetPortNo("console") + "/hcatalog/v1/ddl/database/"+ databaseName +"/table/" + tableName.toLowerCase() + "/partition";
        TestSession.logger.info("Check for table partition url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        jsonArray = jsonObject.getJSONArray("partitions");
        assertTrue("Failed to get the partition for " + tableName + "  table."  , ( (jsonArray != null) && (jsonArray.size() > 0)) );
        return jsonArray;
    }
    
    public List<String> getHCatTablePartitions(String hostName , String facetName , String clusterName , String dbName, String tableName) {
	List<String> partitions =  new ArrayList<String>();
	String url = "http://" + hostName + ":4080/" + facetName + HCAT_TABLE_PARTITION_API  + "?dataSource=" + clusterName + "&dbName=" + dbName + "&tablePattern=" + tableName;
	TestSession.logger.info("get partition url - " + url);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
	String res = response.getBody().asString();
	TestSession.logger.info("Response - " + res);
	JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
	JSONArray jsonArray = jsonObject.getJSONArray("Partitions");
	for ( Object obj : jsonArray) {
	    TestSession.logger.info(obj);
	    String partitionStr = obj.toString();
	    JSONObject jsonObject1 =  (JSONObject) JSONSerializer.toJSON(partitionStr);
	    JSONArray valuesJsonArray = jsonObject1.getJSONArray("Values");
	    for ( Object instanceDateObj : valuesJsonArray) {
		String instanceDateStr = instanceDateObj.toString();
		JSONObject instanceDateJSONObject = (JSONObject) JSONSerializer.toJSON(instanceDateStr);
		String columnValue = instanceDateJSONObject.getString("Value").trim();
		TestSession.logger.info("columnValue  = "  + columnValue);
		partitions.add(columnValue);
	    }
	}
	return partitions;
    }

    /**
     *  Returns schema columns of the given hcat table
     * @param dataSourceName
     * @param tableName
     * @return
     */
    public JSONArray getHCatTableColumns(String databaseName , String dataSourceName , String tableName) {
        List<String> columns = null;
        String url = "http://"+  dataSourceName + ":" + this.consoleHandle.getFacetPortNo("console") +"/hcatalog/v1/ddl/database/" + databaseName.trim() + "/table/"+ tableName.toLowerCase();
        TestSession.logger.info(" Get Hcat Table Column = url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(response.getBody().asString());
        JSONArray columnsJsonArray = jsonObject.getJSONArray("columns");
        TestSession.logger.info("schema columns size = " + columnsJsonArray.size());
        assertTrue("Failed to get the schema columns of table " + tableName + "  in " + dataSourceName + "  HCat server. " , columnsJsonArray.size() > 0);
        return columnsJsonArray;
    } 
    
    public boolean isPartitionExist(String databaseName , String dataSourceName , String tableName) {
        String url = "http://" +  dataSourceName + ":" + this.consoleHandle.getFacetPortNo("console") + "/hcatalog/v1/ddl/database/"+ databaseName + "/table/" + tableName.toLowerCase() + "/partition";
        TestSession.logger.info("Check for table partition url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        String partition = jsonObject.getString("partitions");
        if (partition.equals("[]")) {
            return false;
        }
        return true;
    }
    
    public boolean doPartitionExist(String hostName , String facetName , String clusterName , String dbName, String tableName) {
	List<String> partitions =  new ArrayList<String>();
	String url = "http://" + hostName + ":4080/" + facetName + HCAT_TABLE_PARTITION_API  + "?dataSource=" + clusterName + "&dbName=" + dbName + "&tablePattern=" + tableName;
	TestSession.logger.info("get partition url - " + url);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
	String res = response.getBody().asString();
	TestSession.logger.info("Response - " + res);
	JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
	JSONArray jsonArray = jsonObject.getJSONArray("Partitions");
	if (jsonArray.size() == 0 ) {
	    return false;
	}
	return true;
    }
    
    /**
     * Get partition of the table as List<String>
     * @param dataSourceName
     * @param tableName
     * @return
     */
    public List<String> getHCatTableParitionAsList(String databaseName , String dataSourceName , String tableName) {
        List<String> partitions =  new ArrayList<String>();
        String url = "http://" +  dataSourceName + ":" + this.consoleHandle.getFacetPortNo("console") + "/hcatalog/v1/ddl/database/"+ databaseName +"/table/" + tableName.toLowerCase() + "/partition";
        TestSession.logger.info("Check for table partition url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        JSONArray jsonArray = jsonObject.getJSONArray("partitions");
        for ( Object obj : jsonArray) {
            TestSession.logger.info(obj);
            String partitionStr = obj.toString();
            JSONObject jsonObject1 =  (JSONObject) JSONSerializer.toJSON(partitionStr);
            JSONArray valuesJsonArray = jsonObject1.getJSONArray("values");
            for ( Object instanceDateObj : valuesJsonArray) {
                String instanceDateStr = instanceDateObj.toString();
                JSONObject instanceDateJSONObject = (JSONObject) JSONSerializer.toJSON(instanceDateStr);
                String columnValue = instanceDateJSONObject.getString("columnValue").trim();
                TestSession.logger.info("columnValue  = "  + columnValue);
                partitions.add(columnValue);
            }
        }
        return partitions;
    }

    /**
     * Get HCat server name, if hcat server name is not found in hive-site.xml file then a null is return
     * @param facetName
     * @param clusterName
     * @return
     */
    public String getHCatServerHostName(String clusterName) {
        String hcatHostName = null;
        try {
            String location = this.getHiveSiteXmlFileLocation(clusterName);
            StringBuffer hiveSiteXmlFileLocation = new StringBuffer("/grid/0/yroot/var/yroots/console");
            if ( location != null) {
                hiveSiteXmlFileLocation.append(location).append("hadoopconfig/conf/hadoop/hive-site.xml");
            }
            TestSession.logger.info("hiveSiteXmlFileLocation  = " + hiveSiteXmlFileLocation);
            File file = new File(hiveSiteXmlFileLocation.toString());
            if (file.exists() == true) {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(file);
                doc.getDocumentElement().normalize();
                NodeList nodeLst = doc.getElementsByTagName("property");
                
                for (int index = 0; index < nodeLst.getLength(); index++) {
                    Node fstNode = nodeLst.item(index);
                    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) fstNode;
                        String value = eElement.getElementsByTagName("value").item(0).getTextContent();
                        if (value.startsWith("thrift://")) {
                            String temp = value.substring("thrift://".length()  , value.length() );
                            hcatHostName = temp.substring(0, temp.indexOf(":"));
                            TestSession.logger.info("hostname = " + hcatHostName);
                            break;
                        }
                    }
                }
            }  else {
                TestSession.logger.info("Failed to hive.xml file location for " + clusterName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hcatHostName;
    }
    
    /**
     * Get the location of hive-site.xml file to get the hcat server hostname
     * @return
     */
    public String getHiveSiteXmlFileLocation(String clusterName) {

        String testURL = this.consoleHandle.getConsoleURL() + "/console/query/hadoop/versions";
        TestSession.logger.info("testURL = " + testURL);        
        com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(testURL);
        String responseString = response.getBody().asString();
        String gridName="", location="";
        boolean found = false;
        JSONObject versionObj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
        Object obj = versionObj.get("HadoopClusterVersions");
        if (obj instanceof JSONArray) {
            JSONArray sizeLimitAlertArray = versionObj.getJSONArray("HadoopClusterVersions");
            Iterator iterator = sizeLimitAlertArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                Iterator<String> keys  = jsonObject.keys();
                while( keys.hasNext() ) {
                    String key = (String)keys.next();
                    if (key.equals("DataStoreName") ) {
                        gridName = jsonObject.getString(key);
                    }
                    String value = jsonObject.getString(key);
                    if (value.startsWith("hcat_common") && (clusterName.equals(gridName)) &&   (! gridName.startsWith("gdm")) ) {
                        location = jsonObject.getString("ConfigDirectory");
                        TestSession.logger.info("location - " + location);
                        found = true;
                        break;
                    }
                }
                if (found == true) {
                    break;
                }
            }
        } 
        return location;
    }
    
    /**
     * Get the owner of the table
     * @param dataSourceName  - name of the grid or target cluster name
     * @param dataSetName - dataset naem
     * @param facetName - facet name either acquisition ,  replication , retention
     * @return
     */
    public String getHCatTableOwner(String dataSourceName , String  dataSetName , String facetName) {
        String tableOwner = null;
        String temp = this.consoleHandle.getRestAPI(ConsoleHandle.HCAT_LIST_REST_API , facetName);
        String url = temp +  dataSourceName + "&dataSet=" + dataSetName;
        TestSession.logger.info("Test URL - " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        JSONArray jsonArray = jsonObject.getJSONArray("Tables");
        
        if (jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject tableObject = (JSONObject) iterator.next();
                if (tableObject.has("Owner")) {
                    tableOwner = tableObject.getString("Owner");
                    break;
                }
            }
        }else {
            fail("Failed to get the table for dataset - " + dataSetName + " Response - " + res);
        }
        return tableOwner;
    }
    
    
    /**
     * Get the table name 
     * @param dataSourceName  - name of the grid or target cluster name
     * @param dataSetName - dataset naem
     * @param facetName - facet name either acquisition ,  replication , retention
     * @return
     */
    public String getHCatTableName(String dataSourceName , String  dataSetName , String facetName) {
        String tableName = null;
        String hostName = null;
        String url = null;
        
        String temp = this.consoleHandle.getRestAPI(ConsoleHandle.HCAT_LIST_REST_API , facetName);
        url = temp +  dataSourceName + "&dataSet=" + dataSetName;
        TestSession.logger.info("Test URL - " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("Response - " + res);
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        JSONArray jsonArray = jsonObject.getJSONArray("Tables");
        
        if (jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject tableObject = (JSONObject) iterator.next();
                if (tableObject.has("TableName")) {
                    tableName = tableObject.getString("TableName");
                    break;
                }
            }
        }else {
            fail("Failed to get the table for dataset - " + dataSetName + " Response - " + res);
        }
        return tableName;
    }
    
    /**
     * Check whether table exists for a given db on the given facet. If table exist check even for location path or data path of the table
     * @param hostName
     * @param facetName
     * @param clusterName
     * @param dbName
     * @param tableName
     * @param dataPath
     * @return
     */
    public boolean checkTableAndDataPathExists(String hostName, String facetName, String clusterName , String dbName , String tableName , String dataPath) {
	boolean isTableExists = false , isDataPathExists = false;
	String testURL = "http://" + hostName + ":4080" + "/" + facetName  + HCAT_TABLE_LIST_API + "?dataSource=" + clusterName + "&dbName=" + dbName  + "&tablePattern="  + tableName;
	TestSession.logger.info("url - " + testURL);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(testURL);
	JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Tables");
	if (jsonArray.size() > 0) {
	    Iterator iterator = jsonArray.iterator();	
	    while (iterator.hasNext()) {
		JSONObject dSObject = (JSONObject) iterator.next();

		// check for table name
		String  tName = dSObject.getString("TableName");
		TestSession.logger.info("tableName  - " + tableName);
		isTableExists = tName.equalsIgnoreCase(tableName);
		
		if (isTableExists) {
		    TestSession.logger.info(dbName + " table exists");
		} else {
		    TestSession.logger.info(dbName + " table does not exists");
		    return false;
		}

		// check for path 
		String location = dSObject.getString("Location");
		TestSession.logger.info("location - " + location);
		isDataPathExists = location.indexOf(dataPath) > 0;
		
		if (isDataPathExists) {
		    TestSession.logger.info(dataPath + " path exists");
		} else {
		    TestSession.logger.info(dataPath + " path does not exists");
		    return false;
		}

		if (isTableExists && isDataPathExists) {
		    return true;
		}
	    }
	}
	return false;
    }
}
