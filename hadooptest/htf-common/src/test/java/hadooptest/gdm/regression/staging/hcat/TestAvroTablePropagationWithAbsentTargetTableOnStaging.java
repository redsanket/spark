package hadooptest.gdm.regression.staging.hcat;

import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HCatDataHandle;


/**
 * Test Case : Test Avrotable with 10 partitions
 *
 */
public class TestAvroTablePropagationWithAbsentTargetTableOnStaging extends TestSession {
	
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
	    private List<String> partitionList;
	    
	    @BeforeClass
	    public static void startTestSession() {
	        TestSession.start();
	    }
	    
	    @Before
	    public void setup() throws Exception{
	        String suffix = String.valueOf(System.currentTimeMillis());
	        dataSetName = "AvroReplicationWithAbsentTargetTable_" + suffix;
	        consoleHandle = new ConsoleHandle();
	        workFlowHelperObj = new WorkFlowHelper();
	        List<String> allGrids = this.consoleHandle.getHCatEnabledGrid();
	        if (allGrids.size() < 2) {
	            throw new Exception("Unable to run test: 2 hcat enabled grid datasources are required.");
	        }
	        sourceCluster=allGrids.get(0);
	        targetCluster=allGrids.get(1);
	        tableName = "HTF_Test_Avro" + suffix;	        
	        Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

			partitionList = new ArrayList<String>();
			cal.add(Calendar.DAY_OF_MONTH, - Integer.parseInt("1"));

	        //create table and partitions
			for ( int i=1;i<=10;i++ ) {
				cal.add(Calendar.DAY_OF_MONTH, -i);
				String part = sdf.format(cal.getTime());
				partitionList.add(part);
			}
			
			for ( String partition : partitionList) {
				System.out.println("partition  - " + partition);
		        HCatDataHandle.createAvroTable(sourceCluster, tableName,partition);
			}
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

	    	// replace basedatasetName with the new datasetname
	    	dataSetXml = dataSetXml.replaceAll(baseDataSetName, this.dataSetName);

	    	TestSession.logger.debug("dataSetXml  = " + dataSetXml);
	    	Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
	    	assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
	    	this.consoleHandle.sleep(5000);
	    }
	    
	    @Test 
	    public void testAvroTablePropagationWithAbsenttargetTable() throws Exception{
	        //create dataset
	        createDataSet();
	        
	        //activate dataset
	        consoleHandle.checkAndActivateDataSet(this.dataSetName);
	        dsActivationTime = GdmUtils.getCalendarAsString();
	        
	        //check for workflow
	        workFlowHelperObj.checkWorkFlow(this.dataSetName , "replication", this.dsActivationTime);
	        
	        //check if data got replicated
	        for ( String partition : partitionList) {
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
}
