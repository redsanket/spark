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
        StringBuilder dataSetBuilder = new StringBuilder(this.consoleHandle.getDataSetXml(baseDataSetName));
        
        //set replication to be metadata only
        String pattern = "<HCatTargetType>Mixed</HCatTargetType>";
        String replaceWith = "<HCatTargetType>HCatOnly</HCatTargetType>";
        int indexOf = dataSetBuilder.indexOf(pattern);
        dataSetBuilder.replace(indexOf, indexOf + pattern.length(), replaceWith);
        
        //replace dummy table name with correct table name
        pattern = "<HCatTableName>dummy_tablename</HCatTableName>";
        replaceWith = "<HCatTableName>"+ tableName +"</HCatTableName>";
        indexOf = dataSetBuilder.indexOf(pattern);
        dataSetBuilder.replace(indexOf, indexOf + pattern.length(), replaceWith);
        
        //replace dummy path with correct path
        pattern = "location=\"/data/daqdev/data/dummy_path/instancedate=%{date}\" type=\"data\"/>";
        replaceWith = "location=\"" + dataCommitPath +"/instancedate=%{date}\" type=\"data\"/>";
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
        dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
        TestSession.logger.info("dataSetXml  = " + dataSetXml);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
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
