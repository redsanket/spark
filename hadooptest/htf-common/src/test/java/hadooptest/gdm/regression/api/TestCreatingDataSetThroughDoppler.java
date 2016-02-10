// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.CreateDataSet;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.SourcePath;
import hadooptest.cluster.gdm.Target;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class TestCreatingDataSetThroughDoppler extends TestSession {

    private ConsoleHandle consoleHandle;
    private String cookie;
    private HTTPHandle httpHandle = null;
    private List<String> grids;
    private SourcePath source;
    private String sourceGridName;
    private String targetGridName;
    private Target target1;
    private Target target2;
    
    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }

    @Before
    public void setup() throws Exception {
        this.consoleHandle = new ConsoleHandle();
        this.grids = this.consoleHandle.getUniqueGrids();
        this.httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
        
        this.sourceGridName = this.grids.get(0);
        this.targetGridName = this.grids.get(1);
        
        this.source = new SourcePath();
        source.addSourcePath("/data/daqdev/data/%{date}").addSourcePath("/data/daqdev/count/%{date}").addSourcePath("/data/daqdev/schema/%{date}");
        this.target1 = new Target();
        this.target1.targetName(this.targetGridName).addPath("/data/daqdev/data/%{date}").addPath("/data/daqdev/count/%{date}").addPath("/data/daqdev/schema/%{date}")
        .retentionNumber("92").retentionPolicy("DateOfInstance").numMaps("3");
    }
    
    /**
     * Testcase : Verify whether dataset is not saved when description field is missing.
     * @throws Exception
     */
    @Test
    public void testDescriptionFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testDescriptionFieldMissing")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }

    /**
     * Testcase : Verify whether data is not created, when path instance date value is missing ( %{date} )
     * @throws Exception
     */
    @Test
    public void testDataSetPathInstanceDateFormatMissing() throws Exception {
    	SourcePath source = new SourcePath();
        source.addSourcePath("/data/daqdev/data/").addSourcePath("/data/daqdev/count/%{date}").addSourcePath("/data/daqdev/schema/%{date}");
        
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testDataSetPathInstanceDateFormatMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * Testcase : Verify whether data is not created, when project name is missing ( %{date} )
     * @throws Exception
     */
    @Test
    public void testProjectNameFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testProjectNameFieldMissing")
        .description("Testing dataset creation")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    
    /**
     * Testcase : Verify whether dataset is not created, when source clustername is missing
     * @throws Exception
     */
    @Test
    public void testSourceClusterNameFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testSourceClusterNameFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * TestCase : Verify whether dataset is not created, when UGIGroup is missing
     * @throws Exception 
     */
    @Test
    public void testUGIGroupFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testSourceClusterNameFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * TestCase : Verify whether dataset is not created when UGI owner is missing.
     * @throws Exception
     */
    @Test
    public void testUGIOwnerFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testUGIOwnerFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;  
    }
    
    /**
     * TestCase : Verify whether dataset is not created when UGI permission is missing
     * @throws Exception
     */
    @Test
    public void testUGIPermissionFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testUGIPermissionFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * TestCase : Verify whether dataset is not created when comment field is missing.
     * @throws Exception
     */
    @Test
    public void testCommentFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testCommentFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * TestCase : Verify whether dataset is not created when frequency field is missing.
     * @throws Exception
     */
    @Test
    public void testFrequencyFieldMissing() throws Exception {
        CreateDataSet createDataSetObject = new CreateDataSet();
        createDataSetObject.dataSetName("testFrequencyFieldMissing")
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .addSourcePath(source).addTarget(this.target1);
        this.executeMethod(createDataSetObject.toString(), 400 ) ;
    }
    
    /**
     * TestCase : Verify whether dataset is created successfully, when all the required field are set.
     * @throws Exception
     */
    @Test
    public void testCreatingDataSet() throws Exception {
        String dataSetName = "Test_CreateNewDataSetSuccessFully_"  + System.currentTimeMillis();
        SourcePath sourcePath = new SourcePath();
        sourcePath.addSourcePath("/data/SOURCE_ABF/data/ABF_DAILY/%{date}").addSourcePath("/data/SOURCE_ABF/schema/ABF_DAILY/%{date}").addSourcePath("/data/SOURCE_ABF/count/ABF_DAILY/%{date}");
        Target target = new Target();
        target.targetName(this.targetGridName).addPath("/data/daqdev/data/${DataSetName}/%{date}").addPath("/data/daqdev/schema/${DataSetName}/%{date}").addPath("/data/daqdev/count/${DataSetName}/%{date}")
        .retentionNumber("92").retentionPolicy("DateOfInstance").numMaps("3");;
        CreateDataSet createDSetObject = new CreateDataSet();
        
        createDSetObject.dataSetName(dataSetName)
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGridName)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .doneFilePath("/data/daqdev/data/done")
        .frequency("daily")
        .addSourcePath(sourcePath).addTarget(target);
        TestSession.logger.info("createDSetObject = " + createDSetObject.toString());
        this.executeMethod(createDSetObject.toString(), 201 );
        
        this.consoleHandle.sleep(30000);
        List<String> dataSetList = this.consoleHandle.getAllDataSetName();
        assertTrue("Expected dataset to be created with dataset name = " + dataSetName + "   but failed =  " + dataSetList.toString() , dataSetList.contains(dataSetName) == true);
        
        String getDataSetURL = this.consoleHandle.getConsoleURL() + "/console/query/config/dataset/v1/" + dataSetName + "?format=json";
        com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(getDataSetURL);
        assertTrue("Expected that http status code is success, but got " + response.getStatusCode() , response.getStatusCode() == 200);
        
        String responseString = response.getBody().asString(); 
        TestSession.logger.info("responseString = " + responseString);
        JSONObject responseJsonObject =  (JSONObject) JSONSerializer.toJSON(responseString);
        JSONObject dataSetJsonObject = responseJsonObject.getJSONObject("DataSet");
        
        assertTrue("Expected that dataSetName to be " + createDSetObject.getDataSetName() + "  but got  " + dataSetJsonObject.getString("DataSetName") , dataSetJsonObject.getString("DataSetName").equals(createDSetObject.getDataSetName()));
    }
    
    
    /**
     * method that send request and get response and checks for the response code.
     * @param dataSetRequestJsonValue
     */
    private void executeMethod(String dataSetRequestJsonValue , final int HTTP_CODE) {
    	TestSession.logger.info("dataset request: " + dataSetRequestJsonValue);
        String url = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/v1";
        com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("format", "json").param("datasetRequest" ,dataSetRequestJsonValue ).post(url);
        String res = response.getBody().asString();
        System.out.println(res);
        this.consoleHandle.sleep(5000);
        assertTrue("Expected HTTP  code " + HTTP_CODE  + "  but got " + response.getStatusCode() , response.getStatusCode() == HTTP_CODE);      
    }

}
