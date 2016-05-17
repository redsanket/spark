// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.hcat;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestCase : Verify whether partition are recreated after retention workflow is success on the dataset and re-running the acquisition workflow
 * <HCatTargetType>Mixed</HCatTargetType>  & retention is <HCatTargetType>Mixed</HCatTargetType>
 * Steps : 
 *      1) Create a dataset for acquisition workflow only and set Mixed to the target.
 *      2) Run the workflow, once the workflow is successful.
 *      3) Verify whether instance files are copied to the specified target grid.
 *      4) Verify whether "HCAT table is created on the target's HCAT server cluster".
 *      5) Verify whether "HCAT table is created with same as dataset name or user specified table name".
 *      6) Verify whether "HCAT table is created with the specified schema".
 *      7) Verify whether "HCAT table partition is created with the specified instance date range in the dataset ( start and end date)".
 *      8) Create a retention only dataset for retention workflow.
 *      9) Verify whether table is not dropped.
 *      10) Verify whether partitions are dropped. If partition are not dropped retention on the hcat is failed. Stop further testing of this testcase.
 *      11) Re-run the acquisition workflow for success and check whether partition are re-created.
 *   
 */
public class TestHCATPartitionReCreatedForHCatMixed  extends TestSession {

    private ConsoleHandle consoleHandle;
    private String acqDataSetName;
    private String repDataSetName;
    private String retDataSetName;
    private String targetGrid1;
    private String targetGrid2;
    private String datasetActivationTime;
    private List<String> acqTablePartition;
    private List<String> dataSetList = null;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private WorkFlowHelper workFlowHelper;
    private HCatHelper hcatHelperObject = null;
    private static final String ACQ_HCAT_TYPE = "Mixed";
    private static final String RET_HCAT_TYPE = "Mixed";
    private static final String DATABASE_NAME = "gdm";
    private static final int SUCCESS = 200;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        this.hcatHelperObject = new HCatHelper();
        this.consoleHandle = new ConsoleHandle();
        this.workFlowHelper = new WorkFlowHelper();
        this.acqDataSetName = "Test_HCATPartitionReCreated_Acq_DS_" + System.currentTimeMillis();
        this.retDataSetName = "Test_HCATPartitionReCreated_Ret_DS_"+ System.currentTimeMillis();

        dataSetList = new ArrayList<String>();

        this.targetGrid1 = "omegap1";
        TestSession.logger.info("Using grids " + this.targetGrid1  );

        this.targetGrid2 = "densea";
        TestSession.logger.info("Using grids " + this.targetGrid2  );

        // check whether hcat is enabled on target1 cluster
        boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
        if (!targetHCatSupported) {
            this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
        }

        // check whether hcat is enabled on target2 cluster
        targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid2);
        if (!targetHCatSupported) {
            this.consoleHandle.modifyDataSource(this.targetGrid2, "HCatSupported", "FALSE", "TRUE");
        }
    }

    @Test
    public void TestHCATPartitionReCreated() throws Exception {
        // Acquisition
        {
            // create a dataset
            createAquisitionDataSet("DoAsAcquisitionDataSet.xml");
            dataSetList.add(this.acqDataSetName);

            // activate the dataset
            this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
            this.datasetActivationTime = GdmUtils.getCalendarAsString();

            // check for acquisition workflow 
            this.workFlowHelper.checkWorkFlow(this.acqDataSetName , "acquisition" , this.datasetActivationTime  );

            // check for data.publish step in completed workflow step execution.
            this.workFlowHelper.checkStepExistsInWorkFlowExecutionSteps(this.acqDataSetName, this.datasetActivationTime , "completed", "Step Name" , "data.publish");

            // get Hcat server name for acquisition facet (targetGrid1)
            String acquisitionHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
            assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , acquisitionHCatServerName != null);
            TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + acquisitionHCatServerName);

            // check whether hcat table is created
            boolean isAcqusitionTableCreated = this.hcatHelperObject.isTableExists(acquisitionHCatServerName, this.acqDataSetName , this.DATABASE_NAME);
            assertTrue("Failed : Expected that HCAT table is created " + this.acqDataSetName , isAcqusitionTableCreated == true);

            String acqtableName = this.acqDataSetName.toLowerCase().replace("-", "_").trim();

            // get paritions for acqusition table 
            acqTablePartition = this.hcatHelperObject.getHCatTableParitionAsList(this.DATABASE_NAME , acquisitionHCatServerName, acqtableName);
            assertTrue("Failed to create the partition for " + this.acqDataSetName  + "  dataset.",  acqTablePartition.size() != 0);
        }

        // create a retention dataset and run the retention workflow, so that HCAT table partition are deleted
        {
            // create a datasource for each target
            String rentionDataSourceForTarget1 = this.targetGrid1 +"_HCAT_DataSource_" + System.currentTimeMillis();
            String rentionDataSourceForTarget2 = this.targetGrid2 +"_HCAT_DataSource_"  + System.currentTimeMillis();

            // create datasource
            createDataSourceForEachRetentionJob(this.targetGrid1 , rentionDataSourceForTarget1);
            createDataSourceForEachRetentionJob(this.targetGrid2 , rentionDataSourceForTarget2);
            this.consoleHandle.sleep(50000);

            // create the retention dataset
            createDoAsRetentionDataSet("DoAsRetentionDataSet.xml" ,rentionDataSourceForTarget1 , rentionDataSourceForTarget2 );

            dataSetList.add(this.retDataSetName);

            // activate the dataset
            this.consoleHandle.checkAndActivateDataSet(this.retDataSetName);
            this.datasetActivationTime = GdmUtils.getCalendarAsString();

            // check for retention workflow
            this.workFlowHelper.checkWorkFlow(this.retDataSetName , "retention" , this.datasetActivationTime  );

            // check for step
            this.workFlowHelper.checkStepExistsInWorkFlowExecutionSteps(this.repDataSetName, this.datasetActivationTime , "completed", "Step Name" , "retention." + this.targetGrid1.trim()  + ".unpublish");

            // get Hcat server name for targetGrid1
            String targetGrid1_HcatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
            assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , targetGrid1_HcatServerName != null);
            TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + targetGrid1_HcatServerName);

            String tableName = this.acqDataSetName.toLowerCase().replace("-", "_").trim();
            boolean partitionExists = this.hcatHelperObject.isPartitionExist(this.DATABASE_NAME ,targetGrid1_HcatServerName, tableName);
            TestSession.logger.info("partitionExists  = " + partitionExists);
            assertTrue("Failed : To delete the partition after retention workflow.  " + tableName, partitionExists == false);
        }
        
        // Rerun the acquisition workflow
        {
            // 
            this.workFlowHelper.restartCompletedWorkFlow(this.acqDataSetName);
            
            this.datasetActivationTime = GdmUtils.getCalendarAsString();

            // check for acquisition workflow 
            this.workFlowHelper.checkWorkFlow(this.acqDataSetName , "acquisition" , this.datasetActivationTime  );

            // check for data.publish step in completed workflow step execution.
            this.workFlowHelper.checkStepExistsInWorkFlowExecutionSteps(this.acqDataSetName, this.datasetActivationTime , "completed", "Step Name" , "data.publish");

            // get Hcat server name for acquisition facet (targetGrid1)
            String acquisitionHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
            assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , acquisitionHCatServerName != null);
            TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + acquisitionHCatServerName);

            // check whether hcat table is created
            boolean isAcqusitionTableCreated = this.hcatHelperObject.isTableExists(acquisitionHCatServerName, this.acqDataSetName , this.DATABASE_NAME);
            assertTrue("Failed : Expected that HCAT table is created " + this.acqDataSetName , isAcqusitionTableCreated == true);

            String acqtableName = this.acqDataSetName.toLowerCase().replace("-", "_").trim();

            // get paritions for acqusition table 
            acqTablePartition = this.hcatHelperObject.getHCatTableParitionAsList(this.DATABASE_NAME ,acquisitionHCatServerName, acqtableName);
            assertTrue("Failed to create the partition for " + this.acqDataSetName  + "  dataset.",  acqTablePartition.size() != 0);
        }
    }

    /**
     * creates a acquisition dataset to test DataOnly HCatTargetType
     * @param dataSetFileName - name of the acquisition dataset
     */
    private void createAquisitionDataSet( String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.acqDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.ACQ_HCAT_TYPE);
        dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>acquisition</RunAsOwner>", "");
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName));
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);

        Response response = this.consoleHandle.createDataSet(this.acqDataSetName, dataSetXml);
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
     * Method that creates a retention dataset
     * @param dataSetFileName - name of the retention dataset
     */
    private void createDoAsRetentionDataSet(String dataSetFileName , String newTargetName1  , String newTargetName2) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.retDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

        dataSetXml = dataSetXml.replaceAll("TARGET1", newTargetName1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", newTargetName2 );
        dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>replication</RunAsOwner>", "");
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.retDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.RET_HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
        dataSetXml = dataSetXml.replace("<RunAsOwner>retention</RunAsOwner>", "");

        String tableName = this.acqDataSetName.toLowerCase().replace("-", "_").trim();
        dataSetXml = dataSetXml.replace("TABLE_NAME", tableName);
        dataSetXml = dataSetXml.replace("NUMBER_OF_INSTANCE_VALUES", "0");

        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH",  getCustomPath("data", this.acqDataSetName));
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName)  );
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName));

        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", "");
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", "");
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", "");
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        
    
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", "");
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", "");
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", "");

        Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
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
     * Create DataSource for each target, in order to avoid target collision
     * @param DataSourceName existing target datasource
     * @param newDataSourceName - new datasource name
     */
    public void createDataSourceForEachRetentionJob(String dataSourceName , String newDataSourceName) {     
        String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
        xml = xml.replaceFirst(dataSourceName,newDataSourceName);
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
    }

    /**
     * Method to create a custom path for the dataset.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet) {
        return  "/data/daqdev/hcat/"+ pathType +"/"+ dataSet + "/%{date}";
    }

}
