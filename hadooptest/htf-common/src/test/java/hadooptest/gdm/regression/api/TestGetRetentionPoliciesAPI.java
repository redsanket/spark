// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.fail;

/**
 * TestCase to test getRetentionPolicies REST API.
 *
 */
public class TestGetRetentionPoliciesAPI  extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	private String testURL;
	private JSONUtil jsonUtil;
	private String newDataSetName =  "TestDataSet_" + System.currentTimeMillis();
	private String dataSetName;
	private String sourceGrid;
	private String target;
	private List<String>dataSetList;
	private List<String>dataSourceList= new ArrayList<String>();
	private String baseDataSetName;
	public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
	public static final String dataSourcePath = "/console/query/config/datasource";
	private static final String INSTANCE1 = "20151201";
	private static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.jsonUtil = new JSONUtil();
		this.dataSetName = "Test_SetGet_RetentionPolicy_" + System.currentTimeMillis();
		this.url = this.consoleHandle.getConsoleURL(); 
		this.testURL = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/getRetentionPolicies";

		dataSetList = given().cookie(this.cookie).get(url + this.dataSetPath ).getBody().jsonPath().getList("DatasetsResult.DatasetName");;
		if (dataSetList == null) {
			fail("Failed to get the datasets");
		}

		if (dataSetList.size() == 0) {
			// create the dataset.

			dataSourceList = this.consoleHandle.getUniqueGrids();
			if (dataSourceList.size() < 2) {
				Assert.fail("Only " + dataSourceList.size() + " of 2 required grids exist");
			}
			this.sourceGrid = dataSourceList.get(0);
			this.target = dataSourceList.get(1);
			createDataset();

			dataSetList =  given().cookie(this.cookie).get(url + this.dataSetPath ).getBody().jsonPath().getList("DatasetsResult.DatasetName");;
			assertTrue("Failed to get the newly created dataset name" , dataSetList.size() > 0);
			TestSession.logger.info("dataSetsResultList  = " + dataSetList.toString());

			this.baseDataSetName = newDataSetName;
		} else if (dataSetList.size() > 0){
			this.baseDataSetName = dataSetList.get(0);
		}
	}

	@Test
	public void testGetRetentionPolicies() {
		testInValidDataStore();
		testInvalidDataset();
		testInvalidTarget();
		//testGetRetentionPolicyForGivenDataSetAndSource(); 
		testGetRetentionPolicyForGivenDataSetAndTarget(); 
		testGetAllSourceAndTargetsRetentionPolicies();
		testGetRetentionPolicyWhenOnlyDataSetIsSpecified();
		testSetAndGetRetentionPolicyIsCorrectForNumberOfInstance();
		testMultipleResourceName();
	}

	/*
	 * Test Scenario : Verify whether user is shown an error message when a invalid DataStore is specified as source
	 */
	public void testInValidDataStore() { 

		if (dataSetList.size() > 0) {
			String dataSetName = this.dataSetList.get(0);
			String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
			TestSession.logger.info("resourceName  = "  + resourceName);

			// datasource name
			String dataSourceName = "UNKNOWN_DATA_SOURCE";

			// construct source json element
			String source = constructDataStoreNamesParameter("source" ,Arrays.asList(dataSourceName));

			// invoke the REST API & get the response
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
					.param("dataStores", source)
					.post(this.testURL);

			String responseString = response.getBody().asString();
			TestSession.logger.info("response = " + responseString);

			// convert string to jsonObject
			JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
			if ( jsonArray.size() > 0 ) {
				Iterator iterator = jsonArray.iterator();   
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String message = jsonObject.getString("message").trim();
					TestSession.logger.info("message = " + message);
					assertTrue(message + " is wrong" , message.contains("doesn't exist"));
				}
			}
		} else {
			TestSession.logger.info("There is no dataset(s) in the current deployment, create a dataset and retry executing the testcase");
			assertTrue("There is no dataset in the current deployment " , dataSetList.size() == 0);
		}
	}

	/**
	 * Test Scenario : Verify whether user is shown an error message when a invalid dataSet is specified as target.
	 * message = "Dataset specification doesn't exist."
	 */
	public void testInvalidDataset() {
		String dataSetName = "UNKNOWN_DATASET";
		String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resourceName  = "  + resourceName);

		String dataSourceName = "UNKNOWN_DATA_SOURCE";
		String source = constructDataStoreNamesParameter("source" ,Arrays.asList(dataSourceName));
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
				.param("dataStores", source)
				.post(this.testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("response = " + responseString);

		// convert string to jsonObject
		JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();   
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String message = jsonObject.getString("message");
				TestSession.logger.info("message = " + message);
				assertTrue(message + " is wrong" , message.contains("Dataset specification doesn't exist."));
			}
		}
	}

	/*
	 * Test Scenario : Verify whether user is shown an error message when invalid target is specified
	 */
	public void testInvalidTarget() {
		if (dataSetList.size() > 0) {
			String dataSetName = this.dataSetList.get(0);
			String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
			TestSession.logger.info("resourceName  = "  + resourceName);

			String dataSourceName = "UNKNOWN_DATA_TARGET";

			String source = constructDataStoreNamesParameter("target" ,Arrays.asList(dataSourceName));
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
					.param("dataStores", source)
					.post(this.testURL);
			String responseString = response.getBody().asString();
			TestSession.logger.info("response = " + responseString);

			// convert string to jsonObject
			JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
			if ( jsonArray.size() > 0 ) {
				Iterator iterator = jsonArray.iterator();   
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String message = jsonObject.getString("message");
					TestSession.logger.info("message = " + message);
					assertTrue(message + " is wrong" , message.contains("DataStore UNKNOWN_DATA_TARGET doesn't exist"));
				}
			}
		} else {
			TestSession.logger.info("There is no dataset(s) in the current deployment, create a dataset and retry executing the testcase");
			assertTrue("There is no dataset in the current deployment " , dataSetList.size() == 0);
		}
	}


	/**
	 * Test Scenario : Verify whether correct retention policy is given for a specified dataset and source,
	 *  Note: Mostly we dn't set the retention on source atleast while doing testing, so there wn't be policy retention definded. 
	 *  
	 */
	public void testGetRetentionPolicyForGivenDataSetAndSource() {
		if (dataSetList.size() > 0) {
			String dataSetName = this.dataSetList.get(0);
			TestSession.logger.info("dataSetName = " + dataSetName);

			String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
			TestSession.logger.info("resourceName  = "  + resourceName);

			List<String> dataSourceNameList = this.consoleHandle.getDataSource(dataSetName, "source", "name");



			//if (dataSourceNameList.size() > 0 ) {
			String dataSourceName = dataSourceNameList.get(0);
			TestSession.logger.info("dataSourceName =  " + dataSourceName);

			String source = constructDataStoreNamesParameter("source" ,Arrays.asList(dataSourceName));
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
					.param("dataStores", source)
					.post(this.testURL);
			String responseString = response.getBody().asString();
			TestSession.logger.info("response = " + responseString);

			// convert string to jsonObject
			JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
			if ( jsonArray.size() > 0 ) {
				Iterator iterator = jsonArray.iterator();   
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String message = jsonObject.getString("message");
					TestSession.logger.info("message = " + message);
					assertTrue(message + " is wrong" , message.contains("Successful"));

					// invoke policies 
					JSONArray policies = jsonObject.getJSONArray("policies");
					for ( int i=0 ; i< policies.size() ; i++) {
						JSONObject policy = policies.getJSONObject(i);

						// check for source value
						String sourceName = policy.getString("source").trim();
						TestSession.logger.info("sourceName = " + sourceName);
						assertTrue(sourceName + " that got from response is not matching with request parameter source name = " + dataSourceName , sourceName.equals(dataSourceName));

						// check for policyType
						String policyType = policy.getString("policyType");
						TestSession.logger.info("policyType = " + policyType);
						assertTrue("Expected there should not be any policy definded (No retention policy defined.) but got " + policyType , policyType.contains("No retention policy defined"));
					}

				}
			}
		} else {
			TestSession.logger.info("There is no dataset(s) in the current deployment, create a dataset and retry executing the testcase");
			assertTrue("There is no dataset in the current deployment " , dataSetList.size() == 0);
		}
		//} 

	}

	/*
	 * Test Scenario : Verify whether correct retention policy is given for a specified dataset and target
	 */

	public void testGetRetentionPolicyForGivenDataSetAndTarget() {
		if (dataSetList.size() > 0) {
			String dataSetName = this.dataSetList.get(0);
			String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
			TestSession.logger.info("resourceName  = "  + resourceName);

			List<String> dataSourceNameList = this.consoleHandle.getDataSource(dataSetName, "target", "name");
			String targetName = dataSourceNameList.get(0);
			TestSession.logger.info("dataSourceName =  " + targetName);

			String source = constructDataStoreNamesParameter("target" ,Arrays.asList(targetName));
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
					.param("dataStores", source)
					.post(this.testURL);
			String responseString = response.getBody().asString();
			TestSession.logger.info("response = " + responseString);

			JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
			if ( jsonArray.size() > 0 ) {
				Iterator iterator = jsonArray.iterator();   
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String message = jsonObject.getString("message");
					TestSession.logger.info("message = " + message);
					assertTrue(message + " is wrong" , message.contains("Successful"));

					// navigate the policies and check target value
					JSONArray policies = jsonObject.getJSONArray("policies");
					Iterator policyIterator = policies.iterator();
					while ( policyIterator.hasNext()){
						JSONObject policy = (JSONObject) policyIterator.next();
						boolean hisTargetElementExists = policy.has("target");
						if (hisTargetElementExists) {
							String resposeTargetName = policy.getString("target").trim();
							String requestTargetName = dataSourceNameList.get(0).trim();
							TestSession.logger.info("resposeTargetName = " + resposeTargetName   + "   requestTargetName = " + requestTargetName);
							assertTrue("Request target name " + requestTargetName  +  "   dn't match with response name = " + resposeTargetName   , resposeTargetName.equals(requestTargetName));
						}
					}
				}
			}
		} else {
			TestSession.logger.info("There is no dataset(s) in the current deployment, create a dataset and retry executing the testcase");
			assertTrue("There is no dataset in the current deployment " , dataSetList.size() == 0);
		}
	}

	/**
	 * Test Scenario : Verify whether specifying all the targets of the dataset gives all the targets retention policies.
	 */
	public void testGetAllSourceAndTargetsRetentionPolicies() {
		String dataSetName = this.dataSetList.get(0).trim();
		String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resourceName  = "  + resourceName);

		List<String> sourceNameList = this.consoleHandle.getDataSource(dataSetName, "source", "name");
		List<String> targetNameList = this.consoleHandle.getDataSource(dataSetName, "target", "name");
		TestSession.logger.info("target name - " + targetNameList + " dataset name =  " + dataSetName  + " source  = " + sourceNameList);

		String dataStore = combineSourceAndTargetParameter("source" , sourceNameList , "target" , targetNameList);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName)
				.param("dataStores", dataStore)
				.post(this.testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("response = " + responseString);

		// Navigate the response and check the results
		JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();   
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String resName = jsonObject.getString("resourceName");
				TestSession.logger.info("Response resource name =  " + resName);
				assertTrue(resName + " in response is not matching with the actual request resource name = " + dataSetName , resName.equals(dataSetName));

				// get the message for succesful or unsuccessful 
				String message = jsonObject.getString("message");
				TestSession.logger.info("message = " + message);
				assertTrue(message + " is wrong" , message.contains("Successful"));


				JSONArray policies = jsonObject.getJSONArray("policies");

				int policySize = policies.size() - 1;
				assertTrue("Request target size and response policies target size is not matching" , targetNameList.size() == policySize);
				int targetCount = 0;
				int sourceCount = 0;

				// navigate all the policies jsonArray
				Iterator policyIterator = policies.iterator();
				while ( policyIterator.hasNext()){
					JSONObject policy = (JSONObject) policyIterator.next();

					boolean isSourceElementExists = policy.has("source");
					if (isSourceElementExists) {
						String responseSourceName = policy.getString("source");
						String requestSourceName = sourceNameList.get(sourceCount).trim();
						TestSession.logger.info("Request Source Name = " + requestSourceName  + "  Response Source Name = " + responseSourceName);
						assertTrue(responseSourceName + " in response dn't match with request source name = " + requestSourceName  , responseSourceName.equals(requestSourceName));
						sourceCount++;
					}

					boolean hisTargetElementExists = policy.has("target");
					if (hisTargetElementExists) {
						String resposeTargetName = policy.getString("target").trim();
						String requestTargetName = targetNameList.get(targetCount).trim();
						TestSession.logger.info("resposeTargetName = " + resposeTargetName   + "   requestTargetName = " + requestTargetName);
						assertTrue("Request target name " + requestTargetName  +  "   dn't match with response name = " + resposeTargetName   , resposeTargetName.equals(requestTargetName));
						targetCount++;
					}
				}
			}
		}
	}

	/**
	 *  Test Scenario : Verify whether just specifying the dataset name gives all the target and source name as response
	 */
	public void testGetRetentionPolicyWhenOnlyDataSetIsSpecified() {
		String dataSetName = this.dataSetList.get(0).trim();
		TestSession.logger.info("dataSetName  = " + dataSetName);
		String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resourceName  = "  + resourceName);

		List<String> sourceNameList = this.consoleHandle.getDataSource(dataSetName, "source", "name");
		List<String> targetNameList = this.consoleHandle.getDataSource(dataSetName, "target", "name");
		TestSession.logger.info("target name - " + targetNameList + " dataset name =  " + dataSetName  + " source  = " + sourceNameList);

		com.jayway.restassured.response.Response response = given().cookie(this.cookie)
				.param("resourceNames", resourceName)
				.post(this.testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("response = " + responseString);

		int sourceCount = 0;
		int targetCount = 0;
		// Navigate the response and check the results
		JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();   
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String resName = jsonObject.getString("resourceName");
				TestSession.logger.info("Response resource name =  " + resName);
				assertTrue(resName + " in response is not matching with the actual request resource name = " + dataSetName , resName.equals(dataSetName));

				String message = jsonObject.getString("message");
				TestSession.logger.info("message = " + message);
				assertTrue(message + " is wrong" , message.equals("Successful") == true);

				/*
				 * Note : since we have not specified either the source or the target for the dataset, as a result all the targets are returned
				 */
				JSONArray policies = jsonObject.getJSONArray("policies");

				int policySize = policies.size() - 1 ;
				assertTrue("Request target size and response policies target size do not match, targetName size = " + targetNameList.size()  + " & policy Size  = " + policySize ,  policySize >= targetNameList.size());
			}
		}
	}


	/**
	 * Test Scenario : Verify whether setting the target policy numberOfInstances to 100 and getting the policy is same.
	 */
	public void testSetAndGetRetentionPolicyIsCorrectForNumberOfInstance() {

		List<String> dataTargetList = this.consoleHandle.getDataSource(this.baseDataSetName , "target" ,"name");
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		TestSession.logger.info("dataSetXml  = " + dataSetXml);

		// replace basedatasetName with the new datasetname
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		TestSession.logger.info("after changing the dataset name    = " + dataSetXml);

		// Create a new dataset
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);

		this.consoleHandle.sleep(60000);

		String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.dataSetName));
		List<String> arguments = new ArrayList<String>();
		for (String target : dataTargetList) {
			arguments.add("numberOfInstances:100:" + target.trim());
			TestSession.logger.info("grid = " + target);
		}
		String args = constructPoliciesArguments(arguments , "updateRetention");
		TestSession.logger.info("args = "+args);
		TestSession.logger.info("test url = " + this.testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resourceName).param("command","update").param("args", args)
				.post(this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/actions");

		TestSession.logger.info("Response code = " + res.getStatusCode());
		assertTrue("Failed to modify or set the retention policy for " + this.dataSetName , res.getStatusCode() == SUCCESS);

		String resString = res.getBody().asString();
		TestSession.logger.info("resString = " + resString);

		this.consoleHandle.sleep(30000);

		// get the retention policy on the same target and chech whether the value is same
		resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.dataSetName));
		TestSession.logger.info("resourceName  = "  + resourceName);

		com.jayway.restassured.response.Response response1 = given().cookie(this.cookie).param("resourceNames", resourceName).post(this.testURL);
		String responseString = response1.getBody().asString();
		TestSession.logger.info("response = " + responseString);

		// Navigate the response and check the results
		JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);

		int daysCount = 100;
		int targetCount = 0;
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();   
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String resName = jsonObject.getString("resourceName");
				TestSession.logger.info("Response resource name =  " + resName);
				assertTrue(resName + " in response is not matching with the actual request resource name = " + this.dataSetName , resName.equals(this.dataSetName));

				// get the message for succesful or unsuccessful 
				String message = jsonObject.getString("message");
				TestSession.logger.info("message = " + message);
				assertTrue(message + " is wrong" , message.equals("Successful") == true);

				// navigate all the policies jsonArray
				JSONArray policies = jsonObject.getJSONArray("policies");
				int policySize = policies.size() - 1;
				assertTrue("Request target size and response policies target size is not matching" , dataTargetList.size() == policySize);

				int sourceCount = 0;
				Iterator policyIterator = policies.iterator();
				while ( policyIterator.hasNext()){
					JSONObject policy = (JSONObject) policyIterator.next();
					boolean hisTargetElementExists = policy.has("target");
					if (hisTargetElementExists) {
						String resposeTargetName = policy.getString("target").trim();
						String requestTargetName = dataTargetList.get(targetCount).trim();
						TestSession.logger.info("resposeTargetName = " + resposeTargetName   + "   requestTargetName = " + requestTargetName);
						assertTrue("Request target name " + requestTargetName  +  "   dn't match with response name = " + resposeTargetName   , resposeTargetName.equals(requestTargetName));

						String policyType = policy.getString("policyType");
						TestSession.logger.info("policyType = " + policyType);
						assertTrue("policyType dn't match, policy type was set to numberOfInstances, but got " + policyType , policyType.equals("numberOfInstances"));

						String responseDays = policy.getString("days");
						TestSession.logger.info("responseDays = " + responseDays);
						assertTrue("Days dn't match setted days is 100 and got " + responseDays , responseDays.equals("100"));
						targetCount++;
					}
				}
			}
		}
	}


	/**
	 * Test Scenario : Verify whether getRetentionPolicies REST API accepts multiple resourceName and gives all 
	 *      retention policy for the specified resourceNames.
	 */
	public void testMultipleResourceName() {
		String resourceName = this.jsonUtil.constructResourceNamesParameter(this.dataSetList);
		TestSession.logger.info("resourceName  = "  + resourceName);

		// invoke the rest api
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resourceName).post(this.testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("response = " + responseString);

		// convert response string to jsonArray
		JSONArray jsonArray =  (JSONArray) JSONSerializer.toJSON(responseString);
		int index = 0;
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();   
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String resName = jsonObject.getString("resourceName").trim();
				TestSession.logger.info("Response resource name =  " + resName);
				String dSetName = this.dataSetList.get(index).trim();
				assertTrue(resName + " in response is not matching with the actual request resource name = " + dSetName , resName.equals(dSetName));
				index++;

				// get target List  and source of the dataset, so that we can verify with the policyType
				List<String> dataSetTargetList = this.consoleHandle.getDataSource(dSetName , "target" ,"name");
				List<String> dataSetSourceList = this.consoleHandle.getDataSource(dSetName , "source" ,"name");

				// get the polices for the dataset
				JSONArray policies = jsonObject.getJSONArray("policies");

				/*
				 *  Compare each dataset policies with the number of target in the dataset. 
				 *  This way we can make sure there is extra policies in the response for each dataset 
				 */

				TestSession.logger.info(dSetName + " dataset has " + dataSetTargetList.size() + " target and " + dataSetSourceList.size()  + "  source." );
				int dataStore = dataSetTargetList.size() +  dataSetSourceList.size();
				TestSession.logger.info("Total dataStore = " + dataStore);
				TestSession.logger.info("Total size of policies for " + resName  +"    is " + policies.size());

				assertTrue("Failed total dataStore for " + dSetName   + "   is " + dataStore + "  and  total size for polcies of " + resName + " is " + policies.size() ,  policies.size() >= dataStore);

				int sourceIndex = 0;
				int targetIndex = 0;
				Iterator policyIterator = policies.iterator();
				while ( policyIterator.hasNext()) {
					JSONObject policy = (JSONObject) policyIterator.next();

					boolean isSourceElementExists = policy.has("source");
					if (isSourceElementExists) {
						String responseSourceName = policy.getString("source");
						if (sourceIndex < dataSetSourceList.size()) {
							String requestSourceName = dataSetSourceList.get(sourceIndex).trim();
							TestSession.logger.info("Request Source Name = " + requestSourceName  + "  Response Source Name = " + responseSourceName);
							assertTrue(responseSourceName + " in response dn't match with request source name = " + requestSourceName  , responseSourceName.equals(requestSourceName));
							sourceIndex++;
						}
					}
					boolean hisTargetElementExists = policy.has("target");
					if (hisTargetElementExists) {
						String resposeTargetName = policy.getString("target").trim();
						if (targetIndex < dataSetTargetList.size()) {
							String requestTargetName = dataSetTargetList.get(targetIndex).trim();
							TestSession.logger.info("resposeTargetName = " + resposeTargetName   + "   requestTargetName = " + requestTargetName);
							assertTrue("Request target name " + requestTargetName  +  "   dn't match with response name = " + resposeTargetName   , resposeTargetName.equals(requestTargetName));
							targetIndex++;
						}
					}
				}
			}
		}

	}


	/**
	 * Construct a policyType for the given grids or targets
	 * @param policiesArguments
	 * @param action
	 * @return
	 */
	private String constructPoliciesArguments(List<String>policiesArguments , String action) {
		JSONObject actionObject = new JSONObject().element("action", action);
		String args = null;
		JSONArray resourceArray = new JSONArray();
		for ( String policies : policiesArguments) {
			List<String> values = Arrays.asList(policies.split(":"));
			JSONObject policy = new JSONObject().element("policyType", values.get(0)).element("days", values.get(1)).element("target", values.get(2));
			resourceArray.add(policy);
		}
		actionObject.put("policies", resourceArray);
		args = actionObject.toString();
		return args;
	}

	/**
	 * Method that construct the dataStores part of the request
	 * @param dataStore - its either target or source
	 * @param names
	 * @return
	 */
	private String constructDataStoreNamesParameter(String dataStore , List<String> names ) {
		TestSession.logger.info("****** constructResourceNamesParameter *** " + names.toString());
		JSONArray resourceArray = new JSONArray();
		for (String resource : names) {
			resourceArray.add(new JSONObject().element(dataStore , resource));
		}
		String resourceString = resourceArray.toString();
		TestSession.logger.info("resourceString = " + resourceString);
		return resourceString;
	}

	/**
	 * Combine Source and Target source List and convert it to a json string
	 * @param source - Always "source" 
	 * @param sourceList - List representing all the source of the dataset
	 * @param target - Always "target"
	 * @param targetList - List representing all the target(s) of the dataset
	 * @return  -  jsonString combining of source and targets
	 * 
	 * example :  Return String 
	 * sourceAndTarget = [{"source":"densea"},{"target":"gdm-target-omegap1-test-trunk"},{"target":"gdm-target-densea-test-trunk"},{"target":"gdm-archival-target"}]
	 * 
	 */
	public String combineSourceAndTargetParameter( String source , List<String>sourceList  ,  String target , List<String>targetList) {
		JSONArray resourceArray = new JSONArray();

		// construct source jsonArray
		for (String s : sourceList) {
			resourceArray.add(new JSONObject().element(source , s));
		}

		// construct target jsonArray
		for (String t : targetList) {
			resourceArray.add(new JSONObject().element(target , t));
		}

		String sourceAndTarget = resourceArray.toString();
		TestSession.logger.info("sourceAndTarget = " + sourceAndTarget);
		return sourceAndTarget;
	}

	private void createDataset() {
		String basePath = "/data/daqdev/" + this.newDataSetName + "/data/%{date}";
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.newDataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET", this.target);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.newDataSetName);
		dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
		dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
		dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
		dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset");
		dataSetXml = dataSetXml.replaceAll("END_DATE", "0");
		dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
		dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", basePath);
		hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.newDataSetName, dataSetXml);
		if (response.getStatusCode() != org.apache.commons.httpclient.HttpStatus.SC_OK) {
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}

		this.consoleHandle.sleep(30000);
	}


}
