package gdm.regression;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;

public class TestAvroTablePropagationWithObsoleteTargetTableSchema extends TestSession{
    
    private static String baseDataSetName = "avro_replication";
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
        //create avro table on target without data and default value for schema
        //this value will be updated after replication since GDM will create a directory 
        //under table directory and copy the schema from source there, everytime. 
        HCatDataHandle.createAvroTableWithoutData(targetCluster, tableName);
    }
    
    public void createDataSet(){
        StringBuilder dataSetBuilder = new StringBuilder(this.consoleHandle.getDataSetXml(this.baseDataSetName));
          
        //replace dummy table name with correct table name
        String pattern = "<HCatTableName>dummy_tablename</HCatTableName>";
        String replaceWith = "<HCatTableName>"+ tableName +"</HCatTableName>";
        int indexOf = dataSetBuilder.indexOf(pattern);
        dataSetBuilder.replace(indexOf, indexOf + pattern.length(), replaceWith);
        
        //replace dummy path with correct path
        pattern = "location=\"/data/daqdev/data/dummy_path/instancedate=%{date}\" type=\"data\"/>";
        replaceWith = "location=\"/data/daqdev/data/"+ tableName +"/instancedate=%{date}\" type=\"data\"/>";
        indexOf = dataSetBuilder.indexOf(pattern);
        dataSetBuilder.replace(indexOf, indexOf + pattern.length(), replaceWith);        
        
        //update source with current source
        int offset = dataSetBuilder.indexOf("<Source ");
        int indexOne = dataSetBuilder.indexOf("name=", offset) + "name=".length() +1;
        int indexTwo = dataSetBuilder.indexOf("\"",indexOne);
        dataSetBuilder.replace(indexOne, indexTwo, sourceCluster);
        
        //update target with current target
        offset = dataSetBuilder.indexOf("<Target ");
        indexOne = dataSetBuilder.indexOf("name=", offset) + "name=".length() +1;
        indexTwo = dataSetBuilder.indexOf("\"",indexOne);
        dataSetBuilder.replace(indexOne, indexTwo, targetCluster);
        
        String dataSetXml = dataSetBuilder.toString();
        // replace basedatasetName with the new datasetname
        dataSetXml = dataSetXml.replaceAll(baseDataSetName, this.dataSetName);
        
        TestSession.logger.info("dataSetXml  = " + dataSetXml);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(5000);
        
    }
    
    @Test 
    public void testHcatPropSrcHCatDiscDataAndHCat() throws Exception{
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
        
        //check if the target table has correct avro schema
        status = HCatDataHandle.isAvroSchemaSet(targetCluster, tableName);
        assertTrue("The "+ tableName +" doesn't have the expected avro schema on target cluster " + targetCluster + ".",status);
    }

}
