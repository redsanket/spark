// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.util.List;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DoneFileTest {
    private static final String INSTANCE1 = "20151201";
    private static final String INSTANCE2 = "20151202";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private long startTime = System.currentTimeMillis();
    private String sourceGrid;
    private String targetGrid;
    private boolean eligibleForDelete = false;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 2) {
            Assert.fail("Only " + grids.size() + " of 2 required grids exist");
        }
        this.sourceGrid = grids.get(0);
        this.targetGrid = grids.get(1);
    }
    
    @Test
    public void runTest() throws Exception {
        createTopLevelDirOnTarget();
        
        for (int pathType = 0; pathType <= 1; pathType++) {
            String dataSetName = this.getDataSetName(pathType);
            createValidSourceInstances(dataSetName, pathType);
            createInvalidSourceInstances(dataSetName, pathType);
            createDataset(dataSetName, pathType);
        }
        
        for (int pathType = 0; pathType <= 1; pathType++) {
            String dataSetName = this.getDataSetName(pathType);
            validateReplicationWorkflow(dataSetName);
            validateTargetFiles(dataSetName, pathType);
        }
        
        eligibleForDelete = true;
    }
    
    @After
    public void tearDown() throws Exception {
	if (eligibleForDelete) {
	    for (int pathType = 0; pathType <= 1; pathType++) {
		String dataSetName = this.getDataSetName(pathType);
		this.consoleHandle.deActivateAndRemoveDataSet(dataSetName);
	    }    
	}
    }
    
    private String getDataSetName(int pathType) {
        return "DoneFileTest_" + pathType + "_" + startTime;
    }
    
    private void validateTargetFiles(String dataSetName, int pathType) {
        if (pathType == 0) {
            Assert.assertTrue(this.consoleHandle.filesExist(this.targetGrid, "/projects/doneFileTest/" + dataSetName + "/" + INSTANCE1 + "/data/"));
            Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/doneFileTest/" + dataSetName + "/" + INSTANCE2 + "/data/"));
        } else {
            Assert.assertTrue(this.consoleHandle.filesExist(this.targetGrid, "/projects/doneFileTest/" + dataSetName + "/" + INSTANCE1));
            Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/doneFileTest/" + dataSetName + "/" + INSTANCE2));
        }
    }
    
    private void validateReplicationWorkflow(String dataSetName) {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
    }
    
    private void createDataset(String dataSetName, int pathType) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DatePathReplDataset.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("DATASET_NAME", dataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE_GRID", this.sourceGrid);
        dataSetXml = dataSetXml.replaceAll("TARGET_GRID", this.targetGrid);
        dataSetXml = dataSetXml.replaceAll("SOURCE_PATHS", this.getSourcePathsTag(dataSetName, pathType));
        dataSetXml = dataSetXml.replaceAll("TARGET_PATHS", "");
        dataSetXml = dataSetXml.replaceAll("REPL_STRATEGY", "HFTPDistributedCopy");
        dataSetXml = dataSetXml.replaceAll("EXCLUDE", "");
        dataSetXml = dataSetXml.replaceAll("RETENTION_ENABLED", "TRUE");
        dataSetXml = dataSetXml.replaceAll("START_DATE", "20000101");
        
        // test success.file.path and success.file.name
        if (pathType == 0) {
            dataSetXml = dataSetXml.replaceAll("PARAMS", "<attribute name=\"success.file.path\" value=\"/projects/doneFileTest/" + dataSetName + "/%{date}/DONE\"/>");
        } else {
            dataSetXml = dataSetXml.replaceAll("PARAMS", "<attribute name=\"success.file.name\" value=\"subDir/DONE\"/>");
        }
            
        Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private String getSourcePathsTag(String dataSetName, int pathType) {
        if (pathType == 0) {
            return "<Paths>\n" + 
                    "<Path location=\"/projects/doneFileTest/" + dataSetName + "/%{date}/data\" type=\"data\"/>\n" + 
                    "</Paths>\n";
        } else {
            return "<Paths>\n" + 
                    "<Path location=\"/projects/doneFileTest/" + dataSetName + "/%{date}\" type=\"data\"/>\n" + 
                    "</Paths>\n";
        }
    }
    
    private void createTopLevelDirOnTarget() throws Exception {
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.targetGrid , "/projects", "doneFileTest", "bogus" + startTime);
        createInstanceOnGridObj.execute();
    }
    
    private void createValidSourceInstances(String dataSetName, int pathType) throws Exception {
        if (pathType == 0) {
            // path     /projects/doneFileTest/datasetName/%{date}/data/
            // donefile /projects/doneFileTest/datasetName/%{date}/DONE
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/" + dataSetName, INSTANCE1, "data");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/", dataSetName, INSTANCE1);
            createInstanceOnGridObj.setInstanceFileName("DONE");
            createInstanceOnGridObj.execute();
        } else {
            // path     /projects/doneFileTest/datasetName/%{date}/
            // donefile /projects/doneFileTest/datasetName/%{date}/subDir/DONE
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/", dataSetName, INSTANCE1);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/", dataSetName, INSTANCE1 + "/subDir");
            createInstanceOnGridObj.setInstanceFileName("DONE");
            createInstanceOnGridObj.execute();
        }
    }
    
    private void createInvalidSourceInstances(String dataSetName, int pathType) throws Exception {
        // no done file
        if (pathType == 0) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/" + dataSetName, INSTANCE2, "data");
            createInstanceOnGridObj.execute();
        } else {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/doneFileTest/", dataSetName, INSTANCE2);
            createInstanceOnGridObj.execute();
        }
    }   
}

