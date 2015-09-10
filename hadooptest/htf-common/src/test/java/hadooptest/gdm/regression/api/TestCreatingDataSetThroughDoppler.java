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
	private String dataSetName;
	private String cookie;
	public static final int SUCCESS = 200;
	private HTTPHandle httpHandle = null;
	private List<String> grids;
	private CreateDataSet createDataSetObject;
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
		
		this.dataSetName = "Test_CreateNewDataSetThroughJsonInput_"  + System.currentTimeMillis();
		this.createDataSetObject = new CreateDataSet();
		this.source = new SourcePath();
		source.addSourcePath("/data/daqdev/data").addSourcePath("/data/daqdev/count").addSourcePath("/data/daqdev/schema");
		System.out.println(source.toString());
		this.target1 = new Target();
		this.target1.targetName(this.targetGridName).addPath("/data/daqdev/data").addPath("/data/daqdev/count").addPath("/data/daqdev/schema").retentionDays("92").numMaps("3");
		System.out.println(target1.toString());
	}
	
	/**
	 * Testcase : Verify whether dataset is not saved when description field is missing.
	 * @throws Exception
	 */
	@Test
	public void testDescriptionFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}

	/**
	 * Testcase : Verify whether data is not created, when path instance date value is missing ( %{date} )
	 * @throws Exception
	 */
	@Test
	public void testDataSetPathInstanceDateFormatMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * Testcase : Verify whether data is not created, when project name is missing ( %{date} )
	 * @throws Exception
	 */
	@Test
	public void testProjectNameFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	
	/**
	 * Testcase : Verify whether dataset is not created, when source clustername is missing
	 * @throws Exception
	 */
	@Test
	public void testSourceClusterNameFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created, when UGIGroup is missing
	 * @throws Exception 
	 */
	@Test
	public void testUGIGroupFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when UGI owner is missing.
	 * @throws Exception
	 */
	@Test
	public void testUGIOwnerFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;	
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when UGI permission is missing
	 * @throws Exception
	 */
	@Test
	public void testUGIPermissionFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	
	/**
	 * TestCase : Verify whether dataset is not created when consumer contact is missing.
	 * @throws Exception
	 */
	@Test
	public void testConsumerContactFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
		.description("Testing dataset creation")
		.projectName("apollo")
		.sourceCluster(this.sourceGridName)
		.uGIGroup("aporeport")
		.uGIOwner("apollog")
		.uGIPermission("750")
		.ownerContact("apollo-se@yahoo-inc.com")
		.publisherContact("apollo-se@yahoo-inc.com")
		.comments("Testing dataset creation")
		.doneFilePath("/data/daqdev/data/done")
		.frequency("daily")
		.addSourcePath(source).addTarget(this.target1);
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when contact owner field is missing.
	 * @throws Exception
	 */
	@Test
	public void testOwnerContacFieldMissingt() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
		.description("Testing dataset creation")
		.projectName("apollo")
		.sourceCluster(this.sourceGridName)
		.uGIGroup("aporeport")
		.uGIOwner("apollog")
		.uGIPermission("750")
		.consumerContact("apollo-se@yahoo-inc.com")
		.publisherContact("apollo-se@yahoo-inc.com")
		.comments("Testing dataset creation")
		.doneFilePath("/data/daqdev/data/done")
		.frequency("daily")
		.addSourcePath(source).addTarget(this.target1);
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when publisher contact field is missing.
	 * @throws Exception
	 */
	@Test
	public void testPublisherContactFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
		.description("Testing dataset creation")
		.projectName("apollo")
		.sourceCluster(this.sourceGridName)
		.uGIGroup("aporeport")
		.uGIOwner("apollog")
		.uGIPermission("750")
		.consumerContact("apollo-se@yahoo-inc.com")
		.ownerContact("apollo-se@yahoo-inc.com")
		.comments("Testing dataset creation")
		.doneFilePath("/data/daqdev/data/done")
		.frequency("daily")
		.addSourcePath(source).addTarget(this.target1);
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when comment field is missing.
	 * @throws Exception
	 */
	@Test
	public void testCommentFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when doneFilePath field is missing.
	 * @throws Exception
	 */
	@Test
	public void testDoneFilPathFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		.frequency("daily")
		.addSourcePath(source).addTarget(this.target1);
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is not created when frequency field is missing.
	 * @throws Exception
	 */
	@Test
	public void testFrequencyFieldMissing() throws Exception {
		createDataSetObject.dataSetName(dataSetName)
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
		this.executeMethod(createDataSetObject.toString(), 500 ) ;
	}
	
	/**
	 * TestCase : Verify whether dataset is created successfully, when all the required field are set.
	 * @throws Exception
	 */
	@Test
	public void testCreatingDataSet() throws Exception {
		String dsName = "Test_CreateNewDataSetSuccessFully_"  + System.currentTimeMillis();
		SourcePath sourcePath = new SourcePath();
		sourcePath.addSourcePath("/data/daqdev/data/${DataSetName}/%{date}").addSourcePath("/data/daqdev/count/${DataSetName}/%{date}").addSourcePath("/data/daqdev/schema/${DataSetName}/%{date}");
		Target target = new Target();
		target.targetName(this.targetGridName).addPath("/data/daqdev/data/${DataSetName}/%{date}").addPath("/data/daqdev/count/${DataSetName}/%{date}").addPath("/data/daqdev/schema/${DataSetName}/%{date}").retentionDays("92").numMaps("3");;
		CreateDataSet createDSetObject = new CreateDataSet();
		
		createDSetObject.dataSetName(dsName)
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
		this.executeMethod(createDSetObject.toString(), 200 ) ;
		
		this.consoleHandle.sleep(30000);
		List<String> dataSetList = this.consoleHandle.getAllDataSetName();
		assertTrue("Expected dataset to be created with dataset name = " + dsName + "   but failed =  " + dataSetList.toString() , dataSetList.contains(dsName) == true);
	}
	
	/**
	 * method that send request and get response and checks for the response code.
	 * @param dataSetRequestJsonValue
	 */
	private void executeMethod(String dataSetRequestJsonValue , final int HTTP_CODE) {
		String url = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/v1";
		TestSession.logger.info("url = " + url);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("format", "json").param("datasetRequest" ,dataSetRequestJsonValue ).post(url);
		TestSession.logger.info("response code = " + response.getStatusCode() );
		String res = response.getBody().asString();
		System.out.println(res);
		assertTrue("Expected HTTP  code " + HTTP_CODE  + "  but got " + response.getStatusCode() , response.getStatusCode() == HTTP_CODE);		
	}

}
