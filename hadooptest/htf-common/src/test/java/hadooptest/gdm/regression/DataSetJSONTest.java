// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.CreateDataSet;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.SourcePath;
import hadooptest.cluster.gdm.Target;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSetJSONTest {
    private static final long DAY_IN_MS = 1000 * 60 * 60 * 24;
    private static final int RETENTION_INSTANCE_DAYS = 7;
    private final long startTime = System.currentTimeMillis();
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String sourceGrid;
    private String targetGrid;
    private String instanceToDelete = getPastInstanceDate(RETENTION_INSTANCE_DAYS + 1);
    private String instanceToKeep = getPastInstanceDate(RETENTION_INSTANCE_DAYS - 1);
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> datastores = this.consoleHandle.getUniqueGrids();
        if (datastores.size() < 2) {
            Assert.fail("Only " + datastores.size() + " of 2 required grids exist");
        }
        this.sourceGrid = datastores.get(0);
        this.targetGrid = datastores.get(1);
    }
    
    /**
     * Creates datasets for all three retention policy types using JSON.  Validates files get deleted on the target
     * properly for the DateOfInstance and NumberOfInstances policies
     *  
     * @throws Exception
     */
    @Test
    public void runTest() throws Exception {
        for (int retentionPolicy=0; retentionPolicy<=2; retentionPolicy++) {
            createTargetInstanceFiles(retentionPolicy);
            createDataset(retentionPolicy);
        }
        
        // not validating last retention policy (creation time of instance), as it would take at least a day to run
        for (int retentionPolicy=0; retentionPolicy<=1; retentionPolicy++) {
            validateRetentionWorkflow(retentionPolicy);
            validateTargetFiles(retentionPolicy);
        }
    }
    
    private String getDataSetName(int retentionPolicy) {
        return "DataSetJSONTest_" + retentionPolicy + "_" + this.startTime;
    }
    
    private void validateTargetFiles(int retentionPolicy) {
        validateInstanceFiles(retentionPolicy, this.targetGrid, instanceToDelete, false);  // deleted
        validateInstanceFiles(retentionPolicy, this.targetGrid, instanceToKeep, true);
    }
    
    private void validateInstanceFiles(int retentionPolicy, String grid, String instance, boolean exists) {
        Assert.assertEquals(exists, this.consoleHandle.filesExist(grid, "/projects/DataSetJSONTest/" + this.getDataSetName(retentionPolicy) + "/" + instance));
    }
    
    private void validateRetentionWorkflow(int retentionPolicy) {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("1 Expected workflow to pass", workFlowHelper.workflowPassed(this.getDataSetName(retentionPolicy), "retention", instanceToDelete));
    }
    
    private void createDataset(int retentionPolicy) throws Exception {
        String dataSetName = this.getDataSetName(retentionPolicy);
        SourcePath sourcePath = new SourcePath();
        sourcePath.addSourcePath("/projects/DataSetJSONTest/" + dataSetName + "/%{date}");
        
        Target target = new Target();
        if (retentionPolicy == 0) {
            target.targetName(this.targetGrid).retentionNumber("7");
            target.targetName(this.targetGrid).retentionPolicy("DateOfInstance");
        } else if (retentionPolicy == 1) {
            target.targetName(this.targetGrid).retentionNumber("1");
            target.targetName(this.targetGrid).retentionPolicy("NumberOfInstances");
        } else {
            target.targetName(this.targetGrid).retentionNumber("1");
            target.targetName(this.targetGrid).retentionPolicy("CreationTimeOfInstance");
        }
        
        CreateDataSet datasetCreator = new CreateDataSet();     
        datasetCreator.dataSetName(this.getDataSetName(retentionPolicy))
        .description("Testing dataset creation")
        .projectName("apollo")
        .sourceCluster(this.sourceGrid)
        .uGIGroup("aporeport")
        .uGIOwner("apollog")
        .uGIPermission("750")
        .consumerContact("apollo-se@yahoo-inc.com")
        .ownerContact("apollo-se@yahoo-inc.com")
        .publisherContact("apollo-se@yahoo-inc.com")
        .comments("Testing dataset creation")
        .frequency("daily")
        .addSourcePath(sourcePath)
        .addTarget(target)
        .retentionEnabled(true);
        datasetCreator.submit();
    }
    
    private void createTargetInstanceFiles(int retentionPolicy) throws Exception {
        String dataSetName = this.getDataSetName(retentionPolicy);
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.targetGrid , "/projects", "DataSetJSONTest/" + dataSetName, instanceToDelete);
        createInstanceOnGridObj.execute();
        createInstanceOnGridObj = new CreateInstanceOnGrid(this.targetGrid , "/projects", "DataSetJSONTest/" + dataSetName, instanceToKeep);
        createInstanceOnGridObj.execute();
    }
    
    private String getPastInstanceDate(int daysOld) {
        Date instanceDate = new Date(System.currentTimeMillis() - (daysOld * DAY_IN_MS));
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return dateFormat.format(instanceDate);
    }
}

