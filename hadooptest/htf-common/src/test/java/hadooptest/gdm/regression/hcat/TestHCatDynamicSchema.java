// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.hcat;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.FeedGenerator;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.sf.json.JSONArray;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 * TestCase : Verify whether creating a dynamic schema to an instance registers in HCAT. 
 * Used : Data Discovery HCAT api for testing.
 */
public class TestHCatDynamicSchema  extends TestSession {

    private String cookie;
    private ConsoleHandle consoleHandle;
    private List<String> hcatSupportedGrid;
    private String dataSetName;
    private String feedName;
    private String targetGrid1;
    private String targetGrid2;
    private String dbName = "gdm";
    private String datasetActivationTime;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private WorkFlowHelper workFlowHelper;
    private HCatHelper hcatHelperObj = null;
    private JSONArray fistInstanceAcquisitionSchemaJSONArray;
    private JSONArray secondInstanceAcqusisitionSchemaJSONArray;
    private JSONArray fistInstanceReplicationSchemaJSONArray;
    private JSONArray secondInstanceReplicationSchemaJSONArray;
    private String acquisitionHCatServerName;
    private String acquisitionTableName;
    private String replicationHCatServerName;
    private String replicationTableName;
    private static final String HCatList = "/api/admin/hcat/table/list";
    private static final String HCAT_TYPE = "Mixed";
    private static final int SUCCESS = 200;
    private static final String fdiServerName = "fdi_HcatDynamicSchema_Server";
    private static final String FIRST_INSTANCE = "20120125";
    private static final String SECOND_INSTANCE = "20120126";
    private static final String THIRD_INSTANCE = "20120127";
    private static final String DATABASE_NAME = "gdm";
    private static final String ADDED_COLUMN_NAME = "bogus_name";
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        HTTPHandle httpHandle = new HTTPHandle();
        this.hcatHelperObj = new HCatHelper();
        this.consoleHandle = new ConsoleHandle();
        cookie = httpHandle.getBouncerCookie();
        this.dataSetName = "TestHCatDynamicSchema_" + System.currentTimeMillis();
        this.feedName = this.dataSetName;

        // get all the hcat supported clusters
        this.hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();
        
        // check whether we have two hcat cluster one for acquisition and replication
        if (hcatSupportedGrid.size() < 2) {
            throw new Exception("Unable to run " + this.dataSetName  +" 2 grid datasources are required.");
        }
        this.targetGrid1 = hcatSupportedGrid.get(0).trim();
        this.targetGrid2 = hcatSupportedGrid.get(1).trim();
        TestSession.logger.info("Using grids " + this.targetGrid1 + " , " + this.targetGrid2 );

        // check whether hcat is enabled on target cluster
        boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
        if (!targetHCatSupported) {
            this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
        }
        targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid2);
        if (!targetHCatSupported) {
            this.consoleHandle.modifyDataSource(this.targetGrid2, "HCatSupported", "FALSE", "TRUE");
        }
        this.workFlowHelper = new WorkFlowHelper();
    }

    @Test
    public void testInstanceWithDynamicSchema() throws Exception {

        //generate feed dynamically
        FeedGenerator feedGenerator = new FeedGenerator(this.dataSetName);
        feedGenerator.generateFeed(FIRST_INSTANCE);

        // create dataset.
        createDataSetForAcqRep( );
        
         // FIRST_INSTANCE workflow 
        {
            // acquisition workflow
            {
        
                this.workFlowHelper.checkWorkFlow(this.dataSetName , "acquisition" , this.datasetActivationTime , FIRST_INSTANCE );
                this.acquisitionHCatServerName = this.hcatHelperObj.getHCatServerHostName(this.targetGrid1);
                assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , acquisitionHCatServerName != null);
                TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + this.acquisitionHCatServerName);
                
                boolean isAcqusitionTableCreated = this.hcatHelperObj.isTableExists(this.acquisitionHCatServerName, this.dataSetName , this.DATABASE_NAME);
                assertTrue("Failed to create table for " + this.dataSetName , isAcqusitionTableCreated == true);
                
                // check for FIRST_INSTANCE created for acquisition hcat 
                this.acquisitionTableName = this.dataSetName.toLowerCase().replaceAll("-", "_").trim();
                boolean isAcquisitionFirstInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME , this.acquisitionHCatServerName , this.acquisitionTableName , FIRST_INSTANCE);
                assertTrue("Failed to create " + FIRST_INSTANCE + " partition in " + this.acquisitionTableName , isAcquisitionFirstInstancePartitionCreated == true);
                
                // get the acquisition table schema for the FIRST_INSTANCE
                this.fistInstanceAcquisitionSchemaJSONArray = this.hcatHelperObj.getHCatTableColumns(this.DATABASE_NAME , this.acquisitionHCatServerName , this.acquisitionTableName); 
            }
            
            // replication workflow
            {
            
                // check for replication workflow
                this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , this.datasetActivationTime , FIRST_INSTANCE );
                
                // get the hcat server name
                this.replicationHCatServerName = this.hcatHelperObj.getHCatServerHostName(this.targetGrid2);
                assertTrue("Failed to get the HCatServer Name for " + this.targetGrid2 , replicationHCatServerName != null);
                TestSession.logger.info("Hcat Server for " + this.targetGrid2  + "  is " + this.replicationHCatServerName);
                
                boolean isReplicationTableCreated = this.hcatHelperObj.isTableExists(this.replicationHCatServerName, this.dataSetName , this.DATABASE_NAME);
                assertTrue("Failed to create table for " + this.dataSetName , isReplicationTableCreated == true);
                
                // check for FIRST_INSTANCE created for acquisition hcat 
                this.replicationTableName = this.dataSetName.toLowerCase().replaceAll("-", "_").trim();
                boolean isReplicationFirstInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME, this.replicationHCatServerName , replicationTableName , FIRST_INSTANCE);
                assertTrue("Failed to create " + FIRST_INSTANCE + " partition in " + this.replicationTableName , isReplicationFirstInstancePartitionCreated == true);
                
                this.fistInstanceReplicationSchemaJSONArray = this.hcatHelperObj.getHCatTableColumns(this.DATABASE_NAME , this.replicationHCatServerName , this.replicationTableName); 
            }
        }
        
        //create the second instance
        {
            // create a new instance with schema change (adding a new column) 
            updateSchemaDynamicallyAndCreateInstanceFile(SECOND_INSTANCE);

            // wait for some time so that instance file is created and syn happens
            this.consoleHandle.sleep(50000);
        }

        // check workflow for second instance
        {
            // check for acquisition workflow after schema change for the new instance.
            this.workFlowHelper.checkWorkFlow(this.dataSetName , "acquisition" , this.datasetActivationTime , SECOND_INSTANCE );
            
            // check for SECOND_INSTANCE created for acquisition hcat
            boolean isAcquisitionSecondInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME, this.acquisitionHCatServerName , this.acquisitionTableName , SECOND_INSTANCE);
            assertTrue("Failed to create " + SECOND_INSTANCE + " partition in " + acquisitionTableName , isAcquisitionSecondInstancePartitionCreated == true);
            
            // get the acquisition table schema for SECOND_INSTANCE where schema is changed in the table.
            this.secondInstanceAcqusisitionSchemaJSONArray =  this.hcatHelperObj.getHCatTableColumns(this.DATABASE_NAME ,this.acquisitionHCatServerName , this.acquisitionTableName); 
            
            // if schema is updated succcessfully, then newly added column is added.
            assertTrue("Failed : Acquisition creating of dynamic schema failed i,e HCAT failed to create a new column or partition." ,  this.secondInstanceAcqusisitionSchemaJSONArray.size()  > this.fistInstanceAcquisitionSchemaJSONArray.size());

            // check for replication workflow after schema change for the new instance.
            this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , this.datasetActivationTime , SECOND_INSTANCE );
                
            // check for SECOND_INSTANCE created for acquisition hcat
            boolean isReplicationSecondInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME, this.replicationHCatServerName , this.replicationTableName , SECOND_INSTANCE);
            assertTrue("Failed to create " + SECOND_INSTANCE + " partition in " + this.replicationTableName , isReplicationSecondInstancePartitionCreated == true);

            // get the replication table schema for SECOND_INSTANCE where schema is changed in the table.
            this.secondInstanceReplicationSchemaJSONArray =  this.hcatHelperObj.getHCatTableColumns(this.DATABASE_NAME ,this.replicationHCatServerName , this.replicationTableName); 

            // if schema is updated succcessfully, then newly added column is added.
            assertTrue("Failed : replication creating of dynamic schema failed i,e HCAT failed to create a new column or partition." , this.secondInstanceReplicationSchemaJSONArray.size() > this.fistInstanceReplicationSchemaJSONArray.size() );

        }

        // create the third instance
        {
            // add one more instance file
            feedGenerator.generateFeed(THIRD_INSTANCE);

            // wait for some time so that instance file is created and syn happens
            this.consoleHandle.sleep(50000);
        }

        // check for third instance workflow
        {
            // acquisition workflow
            this.workFlowHelper.checkWorkFlow(this.dataSetName , "acquisition" , this.datasetActivationTime , THIRD_INSTANCE );
            
            // check for THIRD_INSTANCE created for acquisition hcat
            boolean isAcquisitionThirdInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME , this.acquisitionHCatServerName , this.acquisitionTableName , THIRD_INSTANCE);
            assertTrue("Failed to create " + THIRD_INSTANCE + " partition in " + acquisitionTableName , isAcquisitionThirdInstancePartitionCreated == true);
            
            // replication workflow
            this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , this.datasetActivationTime , THIRD_INSTANCE );
            
            // check for SECOND_INSTANCE created for acquisition hcat
            boolean isReplicationThirdInstancePartitionCreated = this.hcatHelperObj.isPartitionIDExists(this.DATABASE_NAME , this.replicationHCatServerName , this.replicationTableName , THIRD_INSTANCE);
            assertTrue("Failed to create " + SECOND_INSTANCE + " partition in " + this.replicationTableName , isReplicationThirdInstancePartitionCreated == true);
            
        }

    }


    /**
     * Create a instance with newly added column to the schema file.
     * @param instance
     * @throws IOException
     */
    public void updateSchemaDynamicallyAndCreateInstanceFile(String instance) throws IOException {

        String tmpFeedPath = "/tmp/" + this.feedName + "/" + instance + "/DONE/";
        File tmpFeedDir = new File(tmpFeedPath);
        FileUtils.deleteDirectory(tmpFeedDir);
        FileUtils.forceMkdir(tmpFeedDir);
        String testSchemaFilePath = new File("").getAbsolutePath() + File.separator + "schema.dat";
        String sourceFeedPath = Util.getResourceFullPath("gdm/feeds/generic") + "/";
        String instanceFilePath = "/tmp/" + this.feedName + "/" + "INSTANCES." + this.feedName;
        File instanceFile = null;

        // create INSTANCE.datasetName file
        { 
            instanceFile = FileUtils.getFile(instanceFilePath);

            if (instanceFile.exists() == false) {
                instanceFile = new File(instanceFilePath);
                FileUtils.writeStringToFile(instanceFile, instance);
            } else if (instanceFile.exists() == true) {
                instanceFile = new File(instanceFilePath);
                Collection< String > instances = FileUtils.readLines(instanceFile);
                instances.add(instance);
                FileUtils.writeLines(instanceFile, instances);
            }
        }

        {
            String updatedSchemaFile = sourceFeedPath + "updatedSchema.dat";
            TestSession.logger.info("updatedSchemaFile  = " + updatedSchemaFile);
            File srcSchemaFile = new File(updatedSchemaFile);
            TestSession.logger.info("srcSchemaFile  = " + srcSchemaFile);
            File dstSchemaFile = new File(tmpFeedPath + "HEADER." + this.feedName + "." + instance);
            TestSession.logger.info("dstSchemaFile  = " + dstSchemaFile);
            FileUtils.copyFile(srcSchemaFile, dstSchemaFile);
        }

        // setup row count - data has 1 row
        {
            File dstRowCountFile = new File(tmpFeedPath + "ROWCOUNT." + this.feedName + "." + SECOND_INSTANCE);
            String rowCountData = "1 /tmp/FDIROOT/" + this.feedName + "/" + SECOND_INSTANCE + "/DONE/" + this.feedName + "_0";
            FileUtils.writeStringToFile(dstRowCountFile, rowCountData);
        }

        // setup status
        {
            File dstStatusFile = new File(tmpFeedPath + "STATUS." + this.feedName + "." + instance);
            String statusData = "/tmp/FDIROOT/" + this.feedName + "/" + instance + "/DONE/DONE." + this.feedName + "." + instance;
            FileUtils.writeStringToFile(dstStatusFile, statusData);
        }

        // setup done file
        {
            File dstDoneFile = new File(tmpFeedPath + "DONE." + this.feedName + "." + instance);
            String doneData = "/tmp/FDIROOT/" + this.feedName + "/" + instance + "/DONE/" + this.feedName + "_0";
            FileUtils.writeStringToFile(dstDoneFile, doneData);
        }

        // setup data from the existing dataset gdm-dataset-ult-schema
        {
            String dataFile = "/grid/0/yroot/var/yroots/fdiserver/tmp/FDIROOT/gdm-dataset-ult-schema-changes/20120126/DONE/gdm-dataset-ult-schema-changes_0";
            File srcDataFile = new File(dataFile);
            assertTrue("Failed : " + dataFile   + "  does not exists.", srcDataFile.exists() == true);
            File dstDataFile = new File(tmpFeedPath + this.feedName + "_0");
            FileUtils.copyFile(srcDataFile, dstDataFile);
        }

        // move to final directory
        {
            File finalDir = new File("/grid/0/yroot/var/yroots/fdiserver/tmp/FDIROOT/" + this.feedName + "/" + instance);
            FileUtils.forceMkdir(finalDir);
            FileUtils.copyDirectoryToDirectory(tmpFeedDir, finalDir);
            if (instanceFile.exists() == true) {
                File fd = new File("/grid/0/yroot/var/yroots/fdiserver/tmp/FDIROOT/" + this.feedName + "/");
                FileUtils.copyFileToDirectory(instanceFile, fd , true);
            } else {
                TestSession.logger.info("**************  file not exist *********");
            }
        }
    }

    /**
     * Create a dataset & activate it.
     * @throws Exception
     */
    public void createDataSetForAcqRep( ) throws Exception {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "HCATDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME","users");
        dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("HCAT_TYPE", this.HCAT_TYPE );
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
        dataSetXml = dataSetXml.replaceAll("TABLE_NAME", this.dataSetName );
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>FACETS</RunAsOwner>", "");

        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.consoleHandle.sleep(10000);

        // activate the dataset
        response = this.consoleHandle.activateDataSet(dataSetName);
        assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(30000);
        this.datasetActivationTime = GdmUtils.getCalendarAsString();
    }

    /**
     * Method that queries HCAT for a given dataset and datasources and gets the response and test or table name,database name etc
     * & get the table column names 
     * @param dataSourceName
     * @param dataSetName
     * @return
     */
    public List<String> testAndGetHCatTableColumnsDataSet(String dataSourceName , String dataSetName) {
        String hcatListURL =  null;
        List<String>schemaCols = null;
        this.consoleHandle.sleep(50000);
        hcatListURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo("acquisition")) + "/" + "acquisition"  + HCatList +"?dataSource=" + dataSourceName + "&dbName="+ this.dbName +
                "&dataSet=" + dataSetName;
        TestSession.logger.info("hcatListURL  = "  + hcatListURL);
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(hcatListURL);

        assertTrue("Failed to get the response for " + hcatListURL , (response != null ) );

        JsonPath jsonPath = response.getBody().jsonPath();
        TestSession.logger.info("HCat List response  = " + jsonPath.prettyPrint());
        String tempDataSetName = this.dataSetName.replace("-", "_").toLowerCase();
        List<String> tableName = jsonPath.getList("Tables.TableName");
        List<String> dbName  = jsonPath.getList("Tables.DBName");
        List<String> tableType = jsonPath.getList("Tables.TableType");
        List<String> partitionColumns = jsonPath.getList("Tables.PartitionColumns");
        String instanceDate = jsonPath.getString("Tables.PartitionColumns.Name");
        List<String>colName = jsonPath.getList("Tables.Columns.Name");

        Object[] objArray =  (Object[]) colName.toArray();
        String x = Arrays.toString(objArray);
        TestSession.logger.info("x = "+ x);
        schemaCols = Arrays.asList(x.split(","));
        for (String s : schemaCols) {
            TestSession.logger.info("s = " + s);
        }

        // assert table information.
        assertTrue("Expected dataset name " + tableName.get(0) +"  but got " + tempDataSetName , tableName.get(0).equals(tempDataSetName));
        assertTrue("Expected TableType name EXTERNAL_TABLE  but got " + tableType.get(0) , tableType.get(0).equals("EXTERNAL_TABLE"));
        assertTrue("Expected gdm as databaseName" + this.dbName +"  but got " + dbName.get(0) , dbName.get(0).equals(this.dbName));
        assertTrue("Expecteced instancedate but got " + instanceDate ,instanceDate.contains("instancedate") );

        return schemaCols;
    }


    /**
     * Create a FDI dataSource file. 
     * @throws Exception
     */
    public static void createFDIServer() throws Exception {
        // check if FDI server exists 
        ConsoleHandle console = new ConsoleHandle();
        String dataSource = console.getDataSourceXml(fdiServerName);
        if (dataSource == null) {
            TestSession.logger.info(fdiServerName + " dataSource does not exist, creating");
            String dataSourceConfigFile = Util.getResourceFullPath("gdm/datasourceconfigs/FDI_DataSource_Template.xml");
            String xmlFileContent = GdmUtils.readFile(dataSourceConfigFile);
            xmlFileContent = xmlFileContent.replace("FDI_SERVER_NAME", fdiServerName);
            xmlFileContent = xmlFileContent.replace("GDM_CONSOLE_NAME", TestSession.conf.getProperty("GDM_CONSOLE_NAME"));
            boolean created = console.createDataSource(xmlFileContent);
            if (created == false) {
                throw new Exception("Failed to create dataSource " + fdiServerName + " from xml: " + xmlFileContent);
            }
            dataSource = console.getDataSourceXml(fdiServerName);
            if (dataSource == null) {
                throw new Exception("Failed to fetch dataSource " + fdiServerName);
            }
        }
        TestSession.logger.info("DataSource " + fdiServerName + " exists");
    }


    /**
     * deactivate the dataset(s)    
     */
    @After
    public void tearDown() {
        TestSession.logger.info("Deactivate "+ this.dataSetName  +"  dataset ");
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
        assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
        assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
    }
    
}
