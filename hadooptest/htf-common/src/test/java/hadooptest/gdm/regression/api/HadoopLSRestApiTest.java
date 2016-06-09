// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class HadoopLSRestApiTest extends TestSession {
	
	private String cookie;
	private String url;
	public static final String dataSource = "densea";
	public String dataSet;
	public JSONUtil jsonUtil;
	private ConsoleHandle consoleHandle;
	private List<String>datasetsResultList;
	private List<String>dataSourceList= new ArrayList<String>();
	private List<String>dataTargetList= new ArrayList<String>();
	public static final String hadoopLSCommand = "/console/api/admin/hadoopls";
	public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
	public static final String dataSourcePath = "/console/query/config/datasource";
	public static final String dataPath = "/data/daqdev/data/";
	
	@Before
	public void setUp() throws NumberFormatException, Exception {
		String hostName = TestSession.conf.getProperty("GDM_CONSOLE_NAME") + ":" + Integer.parseInt(TestSession.conf.getProperty("GDM_CONSOLE_PORT"));
		TestSession.logger.info("hostName = " + hostName);
		HTTPHandle httpHandle = new HTTPHandle();
		this.consoleHandle = new ConsoleHandle();
		jsonUtil = new JSONUtil();
		cookie = httpHandle.getBouncerCookie();
		this.url = "http://" + hostName ;
		TestSession.logger.info("url = " + url);
		this.dataSet  =  getDataSet();
		TestSession.logger.info("dataSet = " + this.dataSet);
		TestSession.logger.info("dataSource = " + this.dataSource);
	}
	
	@Test
	public void testHadoopLSWithDataSourceAndPathWithJsonFormat() {
		String testURL = url  + this.hadoopLSCommand + "?dataSource=" + this.dataSource + "&path=" +  this.dataPath + this.dataSet + "&format=json";
		TestSession.logger.info("testurl = " + testURL ) ;
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response  : " + responseString);
		
		JSONObject jsonObject = (JSONObject)JSONSerializer.toJSON(responseString);
		JSONArray filesJSONArray = jsonObject.getJSONArray("Files");
		TestSession.logger.info("files - " + filesJSONArray);
		
		if ( filesJSONArray.size() > 0 ) {
			
			for (int i = 0 ; i <filesJSONArray.size() ; i++) {
				JSONObject jsonObject1 = filesJSONArray.getJSONObject(i);
				
				String path = jsonObject1.getString("Path");
				TestSession.logger.info(" Path = " + path);
				
				// check for dataset is the part of the path
				assertTrue("expected " + this.dataSet + " to be the part  "+ this.dataPath +" , but found " + path , path.contains(this.dataSet) == true);
				
				// check for path protocol
				assertTrue("Expected hdfs as the protocol, but got " + path + " protocol ", path.contains("hdfs") == true);
			}
		}
		assertTrue("There is no data available for the given path = " + this.dataPath + this.dataSet  +  " on " +  this.dataSource  +  " grid" , filesJSONArray.toString().contains("[]") == true);
	}
	
	private String getDataSet() {
		String dataSet = null;
		datasetsResultList = this.getDataSetListing(cookie , this.url + this.dataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (datasetsResultList == null) {
			fail("Failed to get the datasets");
		} else {
			dataSet = datasetsResultList.get(0);
		}
		return dataSet;
	}
	
	private String getDataSource() { 
		String dataSource = null;
		List<String>tempSource = Arrays.asList(this.getDataSetListing(cookie , this.url + this.dataSourcePath).getBody().prettyPrint().replace("/", "").split("datasource"));
		if (tempSource == null) {
			fail("Failed to get the data sources");
		}
		for (String str : tempSource) {
			if (str.contains("target")) {
				String temp[] = str.split(",");
				if (temp[0] != null && temp[0] != "") {
					dataTargetList.add( temp[0]);
				}
			} else {
				String temp[] = str.split(",");
				if (temp[0] != null && temp[0] != "") {
					dataSourceList.add(temp[0]);
				}
			}
		}
		dataSource = dataSourceList.get(0);
		return dataSource;
	}
	
	private com.jayway.restassured.response.Response getDataSetListing(String cookie , String url)  {
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url );
		return response;
	}
	
}
