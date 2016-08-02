package hadooptest.gdm.regression;

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
import hadooptest.gdm.regression.HadoopFileSystemHelper;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestHCatNoDirectoryCreatedForMetadataOnlyRepl extends TestSession {
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
    private String dataCommitPath;
    
    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }
    
    @Before
    public void setup() throws Exception{
        String suffix = String.valueOf(System.currentTimeMillis());
        dataSetName = "TestHCatNoDirCreatedForMetadataOnlyRepl_" + suffix;
        consoleHandle = new ConsoleHandle();
        workFlowHelperObj = new WorkFlowHelper();
        List<String> allGrids = this.consoleHandle.getHCatEnabledGrid();
        if (allGrids.size() < 2) {
            throw new Exception("Unable to run test: 2 hcat enabled grid datasources are required.");
        }
        sourceCluster=allGrids.get(0);
        targetCluster=allGrids.get(1);
        tableName = "HTF_Test_" + suffix;
        dataCommitPath = "/data/daqdev/data/"+ tableName;
        //create table
        HCatDataHandle.createTable(sourceCluster, tableName);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        partition = dateFormat.format(date);
        
    }
    
    public void createDataSet(){
    	String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/HCat_Test_Template.xml");
    	String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);

    	//set replication to be metadata only
    	String pattern = "<HCatTargetType>Mixed</HCatTargetType>";
    	String replaceWith = "<HCatTargetType>HCatOnly</HCatTargetType>";
    	dataSetXml = dataSetXml.replaceAll(pattern,replaceWith);

    	//replace dummy table name with correct table name
    	pattern = "<HCatTableName>dummy_tablename</HCatTableName>";
    	replaceWith = "<HCatTableName>"+ tableName +"</HCatTableName>";

    	dataSetXml = dataSetXml.replaceAll("dummy_tablename" , tableName);
    	dataSetXml = dataSetXml.replaceAll("dummy_path" , tableName);
    	dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceCluster);
    	dataSetXml = dataSetXml.replaceAll("TARGET_NAME", this.targetCluster);

    	pattern = "<HCatTablePropagationEnabled>FALSE</HCatTablePropagationEnabled>";
    	String replacePattern = "<HCatTablePropagationEnabled>TRUE</HCatTablePropagationEnabled>";
    	dataSetXml = dataSetXml.replaceAll(pattern, replacePattern);
    	dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
    	TestSession.logger.info("dataSetXml  = " + dataSetXml);
    	hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
    	assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
    	this.consoleHandle.sleep(5000);
    }
    
    @Test 
    public void testHcatPropSrcHCatDiscMeta() throws Exception{
        //create dataset
        createDataSet();
        
        //activate dataset
        consoleHandle.checkAndActivateDataSet(this.dataSetName);
        dsActivationTime = GdmUtils.getCalendarAsString();
        
        //check for workflow
        workFlowHelperObj.checkWorkFlow(this.dataSetName , "replication", this.dsActivationTime);
        
        HadoopFileSystemHelper sourceFSHandler = new HadoopFileSystemHelper(sourceCluster);
        assertTrue("Path: "+ dataCommitPath +" doesn't exist on source cluster " + sourceCluster ,
                sourceFSHandler.exists(dataCommitPath));
        HadoopFileSystemHelper targetFSHandler = new HadoopFileSystemHelper(targetCluster);
        assertFalse("Path: "+ dataCommitPath + "created on target cluster" + targetCluster, 
                targetFSHandler.exists(dataCommitPath) );
    }
}
