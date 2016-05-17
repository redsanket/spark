// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.hcat;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase : Verify whether cross colo HCAT metadata replication uses webhdfs  
 * Bug id : webhdfs https://jira.corp.yahoo.com/browse/GDM-333
 * 
 */
public class TestABFHCATHierachicalPartitionsCrossColoWebhdfs  extends TestSession {

    private ConsoleHandle consoleHandle;
    private List<String> hcatSupportedGrid;
    private String dataSetName1;
    private String dataSetName2;
    private String targetGrid1;
    private String targetGrid2;
    private String newTarget;
    private String cookie;
    private String datasetActivationTime;
    private String CUSTOM_DATA_PATH;
    private String CUSTOM_SCHEMA_PATH;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private WorkFlowHelper workFlowHelper;
    private HCatHelper hcatHelperObject ;
    private static final String HCAT_TYPE = "Mixed";
    private static final int SUCCESS = 200;
    private static final String SOURCE_NAME = "elrond";
    private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
    private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_DAILY/";
    private static final String DATABASE_NAME = "gdm";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        this.hcatHelperObject = new HCatHelper();
        this.consoleHandle = new ConsoleHandle();
        this.workFlowHelper = new WorkFlowHelper();
        HTTPHandle httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
        this.dataSetName1 = "Test_ABF_HCat_Propagation_BaseDataSet_" + System.currentTimeMillis();
        this.dataSetName2 = "Test_ABF_HCat_Propagation_DataSet_" + System.currentTimeMillis();
        
        // get all the hcat supported clusters
        hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

        // check whether we have two hcat cluster one for acquisition and replication
        if (hcatSupportedGrid.size() < 2) {
            throw new Exception("Unable to run " + this.dataSetName1  +" 2 grid datasources are required.");
        }

        this.targetGrid1 = hcatSupportedGrid.get(0).trim();
        TestSession.logger.info("Using grids " + this.targetGrid1  );

        this.targetGrid2 = hcatSupportedGrid.get(1).trim();
        TestSession.logger.info("Using grids " + this.targetGrid2  );
        this.newTarget = this.targetGrid2 + "_" + System.currentTimeMillis();

        // check whether hcat is enabled on target1 cluster, if not enabled , enable it
        boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
        if (targetHCatSupported == false) {
            this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
        }

        // check whether hcat is enabled on target2 cluster, if not enabled , enable it
        targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid2);
        if (targetHCatSupported == false) {
            this.consoleHandle.modifyDataSource(this.targetGrid2, "HCatSupported", "FALSE", "TRUE");
        }
        // create a new datasource and use it as metadata hcat target grid
        createDataSource(this.targetGrid2 , this.newTarget);
    }
    
    /**
     * Create a datasource and change the colo from gq1 to ne1 to test cross colo HCat metadata only replication
     * @param existingDataSourceName
     * @param newDataSourceName
     */
    private void createDataSource(String existingDataSourceName , String newDataSourceName) {
        String xml = this.consoleHandle.getDataSourceXml(existingDataSourceName);
        xml = xml.replaceFirst(existingDataSourceName,newDataSourceName);

        // change the colo name, so that GDM can consider this as cross colo and use WEBHDFS protocol to copy data and metadata
        xml = xml.replaceAll("gq1", "ne1");
        TestSession.logger.info("New DataSource Name = " + xml);;
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
    }

    @Test
    public void testTablePropagation() throws Exception {
        
        // check whether ABF data is available on the specified source
        List<String>instanceDates = getInstanceFiles();
        assertTrue("ABF data is missing so testing can't be done, make sure whether the ABF data path is correct...!" , instanceDates != null);

        // Base dataset that replicates the data on the target1 and creates the hcat table and partition (hierachical-partitions)
        {
            // create the dataset to create a hierachical partitions for a replication workflow
            createBaseHCATTableDataSet();

            // activate the dataset
            this.consoleHandle.checkAndActivateDataSet(this.dataSetName1);
            this.consoleHandle.sleep(40000);
            String datasetActivationTime = GdmUtils.getCalendarAsString();

            // check whether each instance date workflow
            for (String instanceDate : instanceDates ) {
                this.workFlowHelper.checkWorkFlow(this.dataSetName1 , "replication" , datasetActivationTime , instanceDate);
            }

            // get Hcat server name for targetGrid
            String hCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
            assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , hCatServerName != null);
            TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + hCatServerName);

            // check whether hcat table is created for Mixed HCatTargetType on replication facet's HCat server
            boolean isTableCreated = this.hcatHelperObject.isTableExists(hCatServerName, this.dataSetName1 , this.DATABASE_NAME);
            assertTrue("Failed to HCAT create table for " + this.dataSetName1 , isTableCreated == true);

            // get the partition details of the dataset
            JSONArray jsonArray = getPartitionDetails(this.targetGrid1 , this.dataSetName1);

            // check whether instance is part of the files on the HDFS
            for (String instanceDate : instanceDates ) {
                checkInstanceExistsInLocationPath(jsonArray , instanceDate);
            }

            // check whether location matches with the partition keys
            checkLocationMatchesWithPartitionKeys(jsonArray , "hdfs");
        }


        // table propagation with hierachical-partitions
        {
            createTablePropagationDataSet();

            // activate the dataset
            this.consoleHandle.checkAndActivateDataSet(this.dataSetName2);
            this.consoleHandle.sleep(40000);
            datasetActivationTime = GdmUtils.getCalendarAsString();

            // check whether each instance date workflow
            for (String instanceDate : instanceDates ) {
                this.workFlowHelper.checkWorkFlow(this.dataSetName2 , "replication" , datasetActivationTime , instanceDate);
            }

            // get Hcat server name for targetGrid
            String hCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid2);
            assertTrue("Failed to get the HCatServer Name for " + this.targetGrid2 , hCatServerName != null);
            TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + hCatServerName);

            boolean isTableCreated = this.hcatHelperObject.isTableExists(hCatServerName, this.dataSetName1 , this.DATABASE_NAME);
            assertTrue("Failed to HCAT create table for " + this.dataSetName2 , isTableCreated == true);

            // get the partition details of the dataset
            JSONArray jsonArray = getPartitionDetails(this.newTarget , this.dataSetName2);

            // check whether location matches with the partition keys
            // changed to hdfs after webhdfs https://jira.corp.yahoo.com/browse/GDM-333
            checkLocationMatchesWithPartitionKeys(jsonArray , "webhdfs");
        }

    }

    /**
     * create dataset 
     */
    public void createBaseHCATTableDataSet() {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BaseHCATTablePropagationDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName1, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        this.CUSTOM_DATA_PATH = getCustomDataPath();
        this.CUSTOM_SCHEMA_PATH = getCustomSchemaPath();

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName1);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.SOURCE_NAME );
        dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName1);
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replace("CUSTOM_DATA_PATH", this.CUSTOM_DATA_PATH);
        dataSetXml = dataSetXml.replace("CUSTOM_SCHEMA_PATH", this.CUSTOM_SCHEMA_PATH);
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
        dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>FACET</RunAsOwner>", "");
        dataSetXml = dataSetXml.replace("DATABASE", this.DATABASE_NAME);

        TestSession.logger.info(dataSetXml);

        Response response = this.consoleHandle.createDataSet(this.dataSetName1, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(5000);
    }

    /**
     * returns the custom data path for dataset
     * @return - 
     */
    public String getCustomDataPath() {
        String path = null;
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MMMd");
        String dt = sdf.format(cal.getTime()) +  "-" +System.currentTimeMillis();
        path = "/data/daqdev/gdm-hcat-table-propagation-test-"+ dt  + "/instancedate=%{date}/pagivity=page/validity=valid/property=AllOther/PAGE/Valid";
        return path;
    }

    /**
     * returns the schema data path for dataset
     * @return
     */
    public String getCustomSchemaPath() {
        String path = null;
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MMMd");
        String dt = sdf.format(cal.getTime()) +  "-" +System.currentTimeMillis();
        path = "/data/daqdev/gdm-hcat-table-propagation-test-"+ dt  + "/instancedate=%{date}/pagivity=page/validity=valid/property=AllOther";
        return path;
    }

    /**
     * create the dataset
     */
    public void createTablePropagationDataSet() {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName2, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.newTarget );
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName2 );
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName1);
        dataSetXml = dataSetXml.replace("CUSTOM_DATA_PATH", this.CUSTOM_DATA_PATH);
        dataSetXml = dataSetXml.replace("CUSTOM_SCHEMA_PATH", this.CUSTOM_SCHEMA_PATH);
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
        dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>FACET</RunAsOwner>", "");
        dataSetXml = dataSetXml.replace("DATABASE", this.DATABASE_NAME);

        TestSession.logger.info("  **********  dataset 2 ******** "+dataSetXml);

        Response response = this.consoleHandle.createDataSet(this.dataSetName2, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(50000);
    }

    /**
     * First checks whether ABF data exists on the grid for a given path, if exists returns instance date(s) 
     * @return
     */
    public List<String>getInstanceFiles() {
        JSONArray jsonArray = null;
        List<String>instanceDate = new ArrayList<String>();
        String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.SOURCE_NAME + "&path=" + ABF_DATA_PATH + "&format=json";
        TestSession.logger.info("Test url = " + testURL);
        com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
        assertTrue("Failed to get the response  " + res , (res != null ) );

        jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
        TestSession.logger.info("********size = " + jsonArray.size());
        if ( jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject dSObject = (JSONObject) iterator.next();
                String  directory = dSObject.getString("Directory");
                TestSession.logger.info("######directory == " + directory);
                if (directory.equals("yes")) {
                    String path = dSObject.getString("Path");
                    List<String>instanceFile = Arrays.asList(path.split("/"));
                    if (instanceFile != null ) {
                        String dt = instanceFile.get(instanceFile.size() - 1);
                        TestSession.logger.info("^^^^^^ date = " + dt);
                        instanceDate.add(dt);
                    }   
                }
            }
            return instanceDate;
        }
        return null;
    }

    /**
     * Get Partition of the specified dataset name 
     * @param dataSourceName  - cluster name
     * @param dataSetName -
     * @return
     */
    public JSONArray getPartitionDetails(String dataSourceName , String dataSetName) {
        JSONArray jsonArray = null;
        String url = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo("acquisition") + "/acquisition/api/admin/hcat/partition/list?dataSource=" + dataSourceName.trim() + "&dataSet=" + dataSetName.trim() );
        TestSession.logger.info("url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
        String res = response.getBody().asString();
        JSONObject jsonObject =  (JSONObject) JSONSerializer.toJSON(res);
        jsonArray = jsonObject.getJSONArray("Partitions");
        return jsonArray;
    }

    /**
     * checks whether the location value contains instance date
     * @param jsonArray
     * @param instanceDate
     */
    public void checkInstanceExistsInLocationPath(JSONArray jsonArray , String instanceDate) {
        Iterator iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            JSONObject object = (JSONObject) iterator.next();
            String location = object.getString("Location");
            TestSession.logger.info("location = " + location);

            List<String> locationBreakUp = Arrays.asList(location.split("/"));
            TestSession.logger.info("contains = " + locationBreakUp.contains("instancedate=" + instanceDate));
            assertTrue("Expected to contain the " + instanceDate + " date in the location path , but contains " + location , locationBreakUp.contains("instancedate=" + instanceDate) == true);
        }
    }


    /**
     * Checks whether the location string starts with either hdfs for normal or usual replication i,e mixed target
     * and webhdfs for table propagation location value and also checks the location values is equal to the partition value.
     * @param jsonArray
     * @param protocolType
     */
    public void checkLocationMatchesWithPartitionKeys(JSONArray jsonArray  , String protocolType) {
        if (jsonArray.size() > 0) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject object = (JSONObject) iterator.next();
                String location = object.getString("Location");
                TestSession.logger.info("location = " + location);

                assertTrue("Expected location to start with " + protocolType + "  but got " + location , location.startsWith(protocolType));

                List<String> locationBreakUp = Arrays.asList(location.split("/"));
                JSONArray valuesJsonArray = object.getJSONArray("Values");

                for (int reverseIndex = valuesJsonArray.size() - 1,  index = 1  ; reverseIndex > 0 && index < valuesJsonArray.size(); reverseIndex -- , index ++ ) {

                    String temp = locationBreakUp.get(locationBreakUp.size() - reverseIndex);
                    JSONObject valueObject = valuesJsonArray.getJSONObject(index);
                    String value = valueObject.getString("Value");
                    TestSession.logger.info("temp = " + temp +  "  value = " + value);
                    assertTrue("" , temp.equals(value));
                }
            }
        }
    }
}
