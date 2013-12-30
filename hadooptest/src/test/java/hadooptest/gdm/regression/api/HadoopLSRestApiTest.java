package hadooptest.gdm.regression.api;



import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.path.json.JsonPath;

import coretest.SerialTests;

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
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}
	
	@Before
	public void setUp() throws NumberFormatException, Exception {
		String hostName = TestSession.conf.getProperty("GDM_CONSOLE_NAME") + ":" + Integer.parseInt(TestSession.conf.getProperty("GDM_CONSOLE_PORT"));
		TestSession.logger.info("hostName = " + hostName);
		HTTPHandle httpHandle = new HTTPHandle();
		consoleHandle = new ConsoleHandle();
		jsonUtil = new JSONUtil();
		cookie = httpHandle.getBouncerCookie();
		this.url = "http://" + hostName ;
		TestSession.logger.info("url = " + url);
		this.dataSet  =  getDataSet();
		TestSession.logger.info("dataSet = " + this.dataSet);
		//this.dataSource = getDataSource();
		TestSession.logger.info("dataSource = " + this.dataSource);
	}
	
	//@Test
	public void testHadoopLSWithDataSourceAndPathWithJsonFormat() {
		String testURL = url  + this.hadoopLSCommand + "?dataSource=" + this.dataSource + "&path=" +  this.dataPath + this.dataSet + "&format=json";
		TestSession.logger.info("testurl = " + testURL ) ;
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
		JsonPath jsonPath = response.jsonPath();
		List<String>paths = jsonPath.getList("Files.Path");
		if (paths == null) {
			fail("failed to get the response = " +  jsonUtil.formatString(response.prettyPrint()));
		}
		TestSession.logger.info("result = " + jsonUtil.formatString(response.prettyPrint()));
		
		// check for path , dataset and protocol as part of response Files.Path value
		for (String path : paths) {
			
			// check for dataset is the part of the path
			assertTrue("expected " + this.dataSet + " to be the part  "+ this.dataPath +" , but found " + path , path.contains(this.dataSet) == true);
			
			// check datapath is part of the response path value
			assertTrue("expected " + this.dataPath +" to be the part " + path + " but found " + path, path.contains(this.dataPath) == true );
			
			// check for path protocol
			assertTrue("Expected hdfs as the protocol, but got " + path + " protocol ", path.contains("hdfs") == true);
		}

		// check whether specified path is a directory, i,e dataset is always the directory in HDFS 
		List<String> directories = jsonPath.getList("Files.Directory");
		for (String directory : directories) {
			assertTrue("Expected directory, but got " + directory , directory.equals("yes"));
		}
		
		// check for directory permission
		List<String> permission = response.jsonPath().getList("Files.Permission");
		for (String per : permission) { 
			assertTrue("Expected directory permission = rwxr-xr-x , but got " + per , per.equals("rwxr-xr-x"));
		}
		// Note : I am not going to test Group and Owner, since doAs feature may change there values
	}
	
	/**
	 * Test Scenario : Verify whether 
	 */
	@Test
	public void testHadoopLSWithWrongDataSource() {
		String testURL = url  + this.hadoopLSCommand + "?dataSource=" + "UNKNOW_DATASOURCE" + "&path=" +  this.dataPath + this.dataSet + "&format=json";
		TestSession.logger.info("testurl = " + testURL ) ;
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
		TestSession.logger.info("Response = " + response.prettyPrint());
		JsonPath jsonPath = response.jsonPath();
		
		// check for action name
		List<String> actionName  =  jsonPath.getList("Response.ActionName");
		assertTrue("Expected hadoopls, but got " + actionName.get(0) , actionName.equals("hadoopls"));
		/*
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected -2 , but got " + responseId , responseId.equals("-2"));
		
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		assertTrue("Expeced DataSource UNKNOW_DATASOURCE does not exist , but got " + responseMessage , responseMessage.equals("DataSource UNKNOW_DATASOURCE does not exist"));		*/
	}
	
	private String getDataSet() {
		String dataSet = null;
		datasetsResultList = consoleHandle.getDataSetListing(cookie , this.url + this.dataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (datasetsResultList == null) {
			fail("Failed to get the datasets");
		} else {
			dataSet = datasetsResultList.get(0);
		}
		return dataSet;
	}
	
	private String getDataSource() { 
		String dataSource = null;
		List<String>tempSource = Arrays.asList(consoleHandle.getDataSetListing(cookie , this.url + this.dataSourcePath).getBody().prettyPrint().replace("/", "").split("datasource"));
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
	
}
