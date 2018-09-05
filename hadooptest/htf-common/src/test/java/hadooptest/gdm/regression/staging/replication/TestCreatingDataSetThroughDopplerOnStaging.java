// Copyright 2018, Yahoo Inc.
package hadooptest.gdm.regression.staging.replication;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;
import org.apache.commons.httpclient.HttpStatus;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.CreateDataSet;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.SourcePath;
import hadooptest.cluster.gdm.Target;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class TestCreatingDataSetThroughDopplerOnStaging extends TestSession {

	private String dataSetName;
	private ConsoleHandle consoleHandle;
	private String cookie;
	private HTTPHandle httpHandle = null;
	private SourcePath source;
	private String sourceGridName;
	private boolean eligibleForDelete = false;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();

		this.sourceGridName = "AxoniteRed";

		this.source = new SourcePath();
		source.addSourcePath("/data/daqdev/data/%{date}").addSourcePath("/data/daqdev/count/%{date}").addSourcePath("/data/daqdev/schema/%{date}");
	}

	/**
	 * TestCase : Verify whether dataset is created successfully, when all the required field are set.
	 * @throws Exception
	 */
	@Test
	public void testCreatingDataSet() throws Exception {
		this.dataSetName = "Test_CreateNewDataSetSuccessFully_"  + System.currentTimeMillis();
		SourcePath sourcePath = new SourcePath();
		sourcePath.addSourcePath("/data/SOURCE_ABF/data/ABF_DAILY/%{date}").addSourcePath("/data/SOURCE_ABF/schema/ABF_DAILY/%{date}").addSourcePath("/data/SOURCE_ABF/count/ABF_DAILY/%{date}");
		Target target = new Target();
		String targetCluster = "AmazonS3";
		target.targetName(targetCluster).addPath("s3Bucket/${DataSetName}/%{date}").addPath("s3Bucket/${DataSetName}/%{date}").addPath("s3Bucket/${DataSetName}/%{date}")
			.retentionNumber("92").retentionPolicy("DateOfInstance").numMaps("3");;
		CreateDataSet createDSetObject = new CreateDataSet();

		createDSetObject.dataSetName(this.dataSetName)
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
			.s3Manifest("gdm.manifest")
			.s3Ykeykey("gdm.ykeykey")
			.frequency("daily")
			.addSourcePath(sourcePath)
			.addTarget(target)
			.requestJSONVersion("3");
		TestSession.logger.info("createDSetObject = " + createDSetObject.toString());
		this.executeMethod(createDSetObject.toString(), 201 );

		this.consoleHandle.sleep(30000);
		List<String> dataSetList = this.consoleHandle.getAllDataSetName();
		assertTrue("Expected dataset to be created with dataset name = " + this.dataSetName + "   but failed =  " + dataSetList.toString() , dataSetList.contains(this.dataSetName) == true);

		String getDataSetURL = this.consoleHandle.getConsoleURL() + "/console/query/config/dataset/v3/" + this.dataSetName + "?format=json";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(getDataSetURL);
		assertTrue("Expected that http status code is success, but got " + response.getStatusCode() , response.getStatusCode() == 200);

		String responseString = response.getBody().asString();
		TestSession.logger.info("responseString = " + responseString);
		JSONObject responseJsonObject =  (JSONObject) JSONSerializer.toJSON(responseString);
		JSONObject dataSetJsonObject = responseJsonObject.getJSONObject("DataSet");

		assertTrue("Expected that dataSetName to be " + createDSetObject.getDataSetName() + "  but got  " + dataSetJsonObject.getString("DataSetName") , dataSetJsonObject.getString("DataSetName").equals(createDSetObject.getDataSetName()));
		assertTrue("Expected that manifestFile to be " + createDSetObject.getS3Manifest() + "  but got  " + dataSetJsonObject.getString("S3ManifestFile") , dataSetJsonObject.getString("S3ManifestFile").equals(createDSetObject.getS3Manifest()));
		assertTrue("Expected that s3Ykeykey to be " + createDSetObject.getS3Ykeykey() + "  but got  " + dataSetJsonObject.getString("S3Ykeykey") , dataSetJsonObject.getString("S3Ykeykey").equals(createDSetObject.getS3Ykeykey()));
		eligibleForDelete = true;
	}


	/**
	 * method that send request and get response and checks for the response code.
	 * @param dataSetRequestJsonValue
	 */
	private void executeMethod(String dataSetRequestJsonValue , final int HTTP_CODE) {
		TestSession.logger.info("dataset request: " + dataSetRequestJsonValue);
		String url = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/v3";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("format", "json").param("datasetRequest" ,dataSetRequestJsonValue ).post(url);
		String res = response.getBody().asString();
		TestSession.logger.info(res);
		this.consoleHandle.sleep(5000);
		assertTrue("Expected HTTP  code " + HTTP_CODE  + "  but got " + response.getStatusCode() , response.getStatusCode() == HTTP_CODE);
	}

	@After
	public void tearDown() throws Exception {
		if (eligibleForDelete) {
			this.consoleHandle.removeDataSet(this.dataSetName);
		}
	}
}
