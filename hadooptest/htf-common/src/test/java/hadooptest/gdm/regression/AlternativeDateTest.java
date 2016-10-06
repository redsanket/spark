// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlternativeDateTest {
    private static final int MIN_GRIDS = 2;
    private static final String INSTANCE1 = "201601172359";
    private static final String INSTANCE2 = "201602031248";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String dataSetName = "AltDate_" + System.currentTimeMillis();
    private String sourceGrid;
    private String targetGrid;
    private List<String> invalidInstances = new ArrayList<String>();
    private boolean eligibleForDelete = false;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < MIN_GRIDS) {
            Assert.fail("Only " + grids.size() + " of 2 required grids exist");
        }
        this.sourceGrid = grids.get(0);
        this.targetGrid = grids.get(1);
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
        
        if (eligibleForDelete) {
            this.consoleHandle.removeDataSet(this.dataSetName);
        }
    }   
    
    /**
     * Runs replication and retention workflows for a dataset with non-%{date} paths and validates resulting target file status.
     * 
     * source grid paths:
     *     data: /projects/altdate/datasetname/data/%{dd}/xx%{yyyy}/%{MM}yy/%{hh}%{mm}
     *     schema: /projects/altdate/datasetname/schema/%{dd}/xx%{yyyy}/%{MM}yy/%{hh}%{mm}
     *     
     * target grid paths:
     *     data: /projects/altdate/datasetname/data/%{yyyy}/%{MM}/%{dd}/%{hh}/%{mm}
     *     schema: /projects/altdate/datasetname/schema/%{yyyy}_%{MM}_%{dd}_%{hh}_%{mm}
     *   
     * @throws Exception  on failure
     */
    @Test
    public void runTest() throws Exception {
        createTopLevelDirOnTarget();
        createValidSourceInstances();
        createInvalidSourceInstances();
        createDataSet();
        validateReplicationWorkflows();
        validateTargetFiles(true);
        enableRetention();
        validateRetentionWorkflow();
        validateTargetFiles(false);
        
        eligibleForDelete = true;
    }
    
    private void validateRetentionWorkflow() {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "retention", INSTANCE1));
        Assert.assertFalse("Expected no workflow for instance " + INSTANCE2, workFlowHelper.workflowExists(this.dataSetName, "retention", INSTANCE2));
    }
    
    // change the retenion policy to 1 instance
    private void enableRetention() {
        this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName, "1");
    }
    
    private void validateReplicationWorkflows() {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow " + INSTANCE1 + " to pass", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        Assert.assertTrue("Expected workflow " + INSTANCE2 + " to pass", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE2));
        
        for (String instance : this.invalidInstances) {
            Assert.assertFalse("Expected no workflow for instance " + instance, workFlowHelper.workflowExists(this.dataSetName, "replication", instance));
        }
    }
    
    private void validateTargetFiles(boolean preRetention) throws Exception {
        List<String> validPaths = new ArrayList<String>();
        List<String> invalidPaths = new ArrayList<String>();
        
        if (preRetention) {
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/59/mydata");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_01_17_23_59/schema.txt");
        } else {
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/59/mydata");
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_01_17_23_59/schema.txt");
        }
    
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/02/03/12/48/mydata");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_02_03_12_48/schema.txt");
        
        HadoopFileSystemHelper fs = new HadoopFileSystemHelper(this.targetGrid);
        for (String path : validPaths) {
            Assert.assertTrue("Expected path " + path + " to exist", fs.exists(path));
        }
        for (String path : invalidPaths) {
            Assert.assertFalse("Expected path " + path + " to not exist", fs.exists(path));
        }
        
        validPaths.clear();
        invalidPaths.clear();
        
        // there should be only one file in each of these directories (unless deleted)
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/*");
        if (preRetention) {
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/59/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_01_17_23_59/*");
        } else {
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/*");
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/*");
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/*");
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/01/17/23/59/*");
            invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_01_17_23_59/*");
        }
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/02/*");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/02/03/*");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/02/03/12/*");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/02/03/12/48/*");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/2016_02_03_12_48/*");
        
        for (String path : validPaths) {
            Assert.assertEquals("Expected one file in " + path + " to exist", 1, fs.numFiles(path));
        }
        for (String path : invalidPaths) {
            Assert.assertEquals("Expected 0 files in " + path + " to exist", 0, fs.numFiles(path));
        }
        
        validPaths.clear();
        
        if (preRetention) {
            // there should be only two files in each of these directories before retention runs
            validPaths.add("/projects/altdate/" + this.dataSetName + "/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/data/2016/*");
            validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/*");
            for (String path : validPaths) {
                Assert.assertEquals("Expected two files in " + path + " to exist", 2, fs.numFiles(path));
            }
        }
    }
    
    private void createDataSet() {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DatePathReplDataset.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("DATASET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("PARAMS", "");
        dataSetXml = dataSetXml.replaceAll("SOURCE_GRID", this.sourceGrid);
        dataSetXml = dataSetXml.replaceAll("TARGET_GRID", this.targetGrid);
        dataSetXml = dataSetXml.replaceAll("SOURCE_PATHS", this.getSourcePathsTag());
        dataSetXml = dataSetXml.replaceAll("TARGET_PATHS", this.getTargetPathsTag());
        dataSetXml = dataSetXml.replaceAll("RETENTION_ENABLED", "TRUE");
        dataSetXml = dataSetXml.replaceAll("REPL_STRATEGY", "HFTPDistributedCopy");
        dataSetXml = dataSetXml.replaceAll("EXCLUDE", "");
        dataSetXml = dataSetXml.replaceAll("START_DATE", "20150101");
        
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private String getSourcePathsTag() {
        return "<Paths>\n" + 
                "<Path location=\"/projects/altdate/" + this.dataSetName + "/data/%{dd}/xx%{yyyy}/%{MM}yy/%{hh}%{mm}\" type=\"data\"/>\n" + 
                "<Path location=\"/projects/altdate/" + this.dataSetName + "/schema/%{dd}/xx%{yyyy}/%{MM}yy/%{hh}%{mm}\" type=\"schema\"/>\n" + 
                "</Paths>\n";
    }
    
    private String getTargetPathsTag() {
        return "<Paths>\n" + 
                "<Path location=\"/projects/altdate/" + this.dataSetName + "/data/%{yyyy}/%{MM}/%{dd}/%{hh}/%{mm}\" type=\"data\"/>\n" + 
                "<Path location=\"/projects/altdate/" + this.dataSetName + "/schema/%{yyyy}_%{MM}_%{dd}_%{hh}_%{mm}\" type=\"schema\"/>\n" + 
                "</Paths>\n";
    }
    
    private void createInvalidSourceInstances() throws Exception {
        List<String> invalidPaths = new ArrayList<String>();
        
        // 201601172259 - invalid directory match (xy vs xx)
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/2259/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xy2016/01yy/2259/mydata");
        this.invalidInstances.add("201601172259");
        
        // 201601172159 - invalid directory match (xxx vs xx)
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/2159/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xxx2016/01yy/2159/mydata");
        this.invalidInstances.add("201601172159");
        
        // 201601172059 - invalid directory match (x vs xx)
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/2059/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/x2016/01yy/2059/mydata");
        this.invalidInstances.add("201601172059");
        
        // 201601171959 - shortened year
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/1959/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xx201/01yy/1959/mydata");
        this.invalidInstances.add("201601171959");
        
        // 201601171859 - lengthened year
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/1859/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xx20161/01yy/1859/mydata");
        this.invalidInstances.add("201601171859");
        
        // 201601171759 - year contains letters
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/1759/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xx20a6/01yy/1759/mydata");
        this.invalidInstances.add("201601171759");
        
        // 201401172359 - before date range
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2014/01yy/2359/schema.txt");
        invalidPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xx2014/01yy/2359/mydata");
        this.invalidInstances.add("201401172359");
                
        HadoopFileSystemHelper fs = new HadoopFileSystemHelper(this.sourceGrid);
        for (String path : invalidPaths) {
            fs.createFile(path);
        }
    }
    
    private void createValidSourceInstances() throws Exception {
        List<String> validPaths = new ArrayList<String>();
        
        // 201601172359
        validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/17/xx2016/01yy/2359/schema.txt");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/17/xx2016/01yy/2359/mydata");
        
        // 201602031248
        validPaths.add("/projects/altdate/" + this.dataSetName + "/schema/03/xx2016/02yy/1248/schema.txt");
        validPaths.add("/projects/altdate/" + this.dataSetName + "/data/03/xx2016/02yy/1248/mydata");
        
        HadoopFileSystemHelper fs = new HadoopFileSystemHelper(this.sourceGrid);
        for (String path : validPaths) {
            fs.createFile(path);
        }
    }
    
    private void createTopLevelDirOnTarget() throws Exception {
        HadoopFileSystemHelper fs = new HadoopFileSystemHelper(this.targetGrid);
        fs.createDirectory("/projects/altdate/");
    }
}


