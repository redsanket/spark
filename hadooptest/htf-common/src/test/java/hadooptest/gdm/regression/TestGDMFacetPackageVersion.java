package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestGDMFacetPackageVersion extends TestSession {

    private ConsoleHandle consoleHandle;
    private String cookie;
    private CommonFunctions commonFunctions;
    private List<String> facets;
    private String envType;

    @BeforeClass
    public static void startTestSession() {
	TestSession.start();
    }

    @Before
    public void setup() throws Exception {
	this.consoleHandle = new ConsoleHandle();
	HTTPHandle httpHandle = new HTTPHandle();
	this.cookie = httpHandle.getBouncerCookie();
	this.commonFunctions = new CommonFunctions();
	this.facets = new ArrayList<String>(Arrays.asList("console", "acquisition", "replication", "retention"));

	// get test environment type
	if (this.consoleHandle.getConsoleURL().indexOf("stg") > -1) {
	    this.setEnvType("staging");
	} else {
	    this.setEnvType("qa");
	}
    }
    
    @Test
    public void testDeployedGDMFacetPackageVersion() throws Exception {
	for ( String facetName : this.facets) {
	    checkGDMVersion(facetName);
	}
    }
    
    private void checkGDMVersion(String facetName) throws Exception {
	Map<String , String> applicationSummaryMap = this.getHealthCheckDetails(facetName);
	assertTrue("Expected Current State to be Active but got " + applicationSummaryMap.get("Current State") ,  applicationSummaryMap.get("Current State").endsWith("Active"));
	
	String distedVersion = this.commonFunctions.executeCommand("dist_tag list gdm_" + facetName + "_" + this.getEnvType() +  "_test_latest | cut -d- -f2 | cut -d' ' -f1");
	String deployedGdmVersion = applicationSummaryMap.get("build.version");
	TestSession.logger.info("INSTALLED_GDM_VERSION=" + deployedGdmVersion);
	TestSession.logger.info(facetName + " is running on  " + deployedGdmVersion  + " version & package disted on gdm_" + facetName + "_" + this.getEnvType() +  "_test_latest  is " + distedVersion);
	if (! distedVersion.equals(deployedGdmVersion) ) {
	    throw new Exception("Current running " + facetName + " version is " +  deployedGdmVersion + "   & package disted on gdm_" + facetName + "_" + this.getEnvType() +  "_test_latest  is " + distedVersion);
	}
    }
    
    /**
     * Request the facet health checkup and returns the key and value.
     * @param facetName
     * @return
     */
    private Map<String , String> getHealthCheckDetails(String facetName) {
	String	consoleHealthCheckUpTestURL = this.consoleHandle.getConsoleURL()+ "/console/api/proxy/health?colo=gq1&facet=" + facetName;
	TestSession.logger.info("consoleHealthCheckUpTestURL = " +consoleHealthCheckUpTestURL );
	com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(consoleHealthCheckUpTestURL);
	assertTrue("Failed to get the response for " + consoleHealthCheckUpTestURL , (response != null) );
	String resString = response.asString();
	TestSession.logger.info("response = " + resString);
	JsonPath jsonPath = new JsonPath(resString);
	Map<String , String>applicationSummary = new HashMap<String, String>();
	List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
	List<String> values = jsonPath.get("ApplicationSummary.Value");
	for(int i = 0;i<keys.size() ; i++){
	    applicationSummary.put(keys.get(i), values.get(i));
	}
	return applicationSummary;
    }

    private String getEnvType() {
        return envType;
    }

    private void setEnvType(String envType) {
        this.envType = envType;
    }
}
