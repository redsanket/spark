package gdm.regression;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestAvroTablePropagationWithObsoleteTargetTableSchema extends TestSession{
    
    private static String baseDataSetName = "HCat_Test_Template";
    private static String sourceCluster;
    private static String targetCluster;
    private static final int SUCCESS = 200;
    private String dsActivationTime; 
    private String dataSetName; 
    private ConsoleHandle consoleHandle;
    private WorkFlowHelper workFlowHelperObj = null;
    private String tableName;
    private String partition;
    
    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }
    
    @Before
    public void setup() throws Exception{
        String suffix = String.valueOf(System.currentTimeMillis());
        dataSetName = "AvroReplicationWithObsoleteTargetTable_" + suffix;
        consoleHandle = new ConsoleHandle();
        workFlowHelperObj = new WorkFlowHelper();
        List<String> allGrids = this.consoleHandle.getHCatEnabledGrid();
        if (allGrids.size() < 2) {
            throw new Exception("Unable to run test: 2 hcat enabled grid datasources are required.");
        }
        sourceCluster=allGrids.get(0);
        targetCluster=allGrids.get(1);
        tableName = "HTF_Test_Avro" + suffix;
        //create table
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        partition = dateFormat.format(date);
        //create avro table on source with data and default value for schema
        HCatDataHandle.createAvroTable(sourceCluster, tableName,partition);
        //create avro table on target with obsolete schema
        HCatDataHandle.createObsoleteAvroTable(targetCluster, tableName);
        TestSession.logger.info("Making sure that target has obsolete schema");
        boolean status = HCatDataHandle.isAvroSchemaCorrect(sourceCluster, tableName, targetCluster);
        assertFalse("At this point the source and target tables should have different schemas",status);
    }
    
    public void createDataSet(){
    	String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/HCat_Test_Template.xml");
    	String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("dummy_tablename" , tableName);
        dataSetXml = dataSetXml.replaceAll("dummy_path" , tableName);
        dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceCluster);
        String pattern = "<HCatTablePropagationEnabled>FALSE</HCatTablePropagationEnabled>";
        String replacePattern = "<HCatTablePropagationEnabled>TRUE</HCatTablePropagationEnabled>";
        dataSetXml = dataSetXml.replaceAll(pattern, replacePattern);
        dataSetXml = dataSetXml.replaceAll("TARGET_NAME", this.targetCluster);
        dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
        TestSession.logger.info("dataSetXml  = " + dataSetXml);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(5000);   
    }
    
    @Test 
    public void testAvroTablePropagationWithObsoleteTargetSchema() throws Exception{
        //create dataset
        createDataSet();
        
        //activate dataset
        consoleHandle.checkAndActivateDataSet(this.dataSetName);
        dsActivationTime = GdmUtils.getCalendarAsString();
        
        //check for workflow
        workFlowHelperObj.checkWorkFlow(this.dataSetName , "replication", this.dsActivationTime);
        
        //check if data got replicated
        boolean status = HCatDataHandle.doesPartitionExist(targetCluster, tableName, partition);
        assertTrue("The "+ tableName +" didn't get replicated from " + sourceCluster +
                " to " + targetCluster + ".",status);
        
        //check if the avro schema is set on target
        status = HCatDataHandle.isAvroSchemaSet(targetCluster, tableName);
        assertTrue("The "+ tableName +" doesn't have avro schema set on target cluster " + targetCluster + ".",status);
        
        //check if the target table has correct avro schema
        status = HCatDataHandle.isAvroSchemaCorrect(sourceCluster, tableName,targetCluster);
        assertTrue("The "+ tableName +" doesn't have the expected avro schema on target cluster " + targetCluster + ".",status);
    }

}
