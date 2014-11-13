package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import com.jayway.restassured.path.json.JsonPath;

/**
 * Test Scenario : verify whether non admin user is able to get the classloader information about the 
 * specified facet and is able to invalidate the class loader. 
 */
public class TestNonAdminClassLoaderFunctionality  extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	private HTTPHandle httpHandle = null;
	private JSONUtil jsonUtil;
	private List<String> acqClusterClassLoaded = null;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;
	private static final String CLASS_LOADER_LIST ="/api/admin/classloader/list";
	private static final String InValidate_CLASS_LOADER_LIST ="/api/admin/classloader/invalidate?datastore=";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		httpHandle = new HTTPHandle();
		this.nonAdminUserName = this.consoleHandle.getConf().getString("auth.nonAdminUser");
		this.nonAdminPassWord = this.consoleHandle.getConf().getString("auth.nonAdminPassWord");
		this.httpHandle.logonToBouncer(this.nonAdminUserName, nonAdminPassWord);
		this.cookie = this.httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + this.url);
	}

	@Test
	public void testNonAdminClassLoader() {
		testAcquisitionTargetClassLoaderList();
		testAcquisitionInvalidateClassLoader();
	}

	/**
	 * Test Scenario : Verify whether acquisition cluster class loader is successfully after acquisition workflow.
	 */
	public void testAcquisitionTargetClassLoaderList() {
		acqClusterClassLoaded = getClassLoaderList("acquisition");
		if ( acqClusterClassLoaded == null ) {
			assertTrue("** There is no class to invalidate on acquisition facet **" ,  acqClusterClassLoaded == null);
		} else if (acqClusterClassLoaded.size() > 0) {
			TestSession.logger.info("Found class loaded in acquisition facet");
			assertTrue("** There is no class to invalidate on acquisition facet **" ,  acqClusterClassLoaded.size() > 0);
			
			// just print the cluster names
			for (String cluster : acqClusterClassLoaded) {
				TestSession.logger.info(cluster);
			}
		}
	}
	
	/**
	 * Verify whether non-admin can invalide the classloader.
	 */
	public void testAcquisitionInvalidateClassLoader() { 
		for (String acqCluster : acqClusterClassLoaded ) {
			invalidateClassLoader(acqCluster , "acquisition");
		}
	}
	
	/**
	 * Invalidate the specified classloader for the given facet
	 * @param clusterName
	 * @param facetName
	 */
	private void invalidateClassLoader(String clusterName , String facetName) {
		String testURL  =  this.url.replace("9999", this.consoleHandle.getFacetPortNo(facetName)) + "/" + facetName + InValidate_CLASS_LOADER_LIST + clusterName; 
		TestSession.logger.info("testURL  = " + testURL);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).post(testURL);
		
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		
		JsonPath jsonPath = response.getBody().jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		String responseId = jsonPath.getString("Response.ResponseId");
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains("Unauthorized access");
		assertTrue("Expected ResponseId is -3 , but got " + responseId , responseId.equals("-3"));
		assertTrue("Expected response message as Unauthorized access , but got " + responseMessage  , flag == true);
	}

	/**
	 * Get all the cluster name loaded for the specified for facet
	 * @param facetName - facet name to get the port no
	 * @return
	 */
	private List<String> getClassLoaderList(String facetName) {
		List<String> classLoaderList = null;
		List<String> clusterList = new ArrayList<String>();
		String testURL  =  this.url.replace("9999", this.consoleHandle.getFacetPortNo(facetName)) + "/" + facetName + CLASS_LOADER_LIST ; 
		TestSession.logger.info("testURL  = " + testURL);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(testURL);
		
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		
		JsonPath jsonPath = response.getBody().jsonPath();
		TestSession.logger.info("ClassLoader response = " + jsonPath.prettyPrint());
		classLoaderList = jsonPath.getList("ClassLoaders.Version");
		for ( String cluster : classLoaderList ) {
			List<String> clusterName = Arrays.asList(cluster.split("\\."));
			clusterList.add(clusterName.get(clusterName.size()-1));
		}
		return clusterList;
	}
}
