package hadooptest.gdm.regression;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import hadooptest.Util;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;


public class TestHCatNoPropagatingSourceHCatDiscoveryDataAndHCat extends TestSession {
    
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
    public void setup() throws Exception {
        String suffix = String.valueOf(System.currentTimeMillis());
        dataSetName = "TestHCatNoPropSrcHCatDiscMix_" + suffix;
        consoleHandle = new ConsoleHandle();
        workFlowHelperObj = new WorkFlowHelper();
        List<String> allGrids = this.consoleHandle.getHCatEnabledGrid();
        if (allGrids.size() < 2) {
            throw new Exception("Unable to run test: 2 hcat enabled grid datasources are required.");
        }
        sourceCluster=allGrids.get(0);
        targetCluster=allGrids.get(1);
        tableName = "HTF_Test_" + suffix;
        
        TestSession.logger.info("sourceCluster = " + sourceCluster   + "  tableName =   " + tableName);
        //create table 
        HCatDataHandle.createTable(sourceCluster, tableName);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        partition = dateFormat.format(date);
        
        //since the source isn't being propagated 
        //the target needs to have this table.
        TestSession.logger.info("targetCluster = " + targetCluster   + "  tableName =   " + tableName);
        HCatDataHandle.createTableOnly(targetCluster,tableName);
    }
    
    @Test 
    public void testHcatNoPropSrcHCatDiscDataAndHCat() throws Exception{
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
    }
    
    /**
     * Create a dataset using HCat_Test_Template as reference.
     */
    public void createDataSet() {
    	String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/HCat_Test_Template.xml");
    	String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        
        //set replication to be mixed
        //template is mined type by default
        
        //set propagation type to be no propagation
        String pattern = "<HCatTablePropagationEnabled>TRUE</HCatTablePropagationEnabled>";
        String replaceWith = "<HCatTablePropagationEnabled>FALSE</HCatTablePropagationEnabled>";
        dataSetXml = dataSetXml.replaceAll(pattern, replaceWith);
        
        //replace dummy table name with correct table name
        dataSetXml = dataSetXml.replaceAll("dummy_tablename",tableName );
        
        //replace dummy path with correct path
        dataSetXml = dataSetXml.replaceAll("dummy_path", tableName);
        
        //update source with current source
        dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceCluster);
        dataSetXml = dataSetXml.replaceAll("TARGET_NAME", this.targetCluster);
        dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
        TestSession.logger.info("dataSetXml  = " + dataSetXml);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(5000);
    }
}
