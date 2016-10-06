// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.util.List;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatePathTest extends TestSession {
    private static final String INSTANCE1 = "20151201";
    private static final String INSTANCE2 = "20151202";
    private static final String INSTANCE3 = "20151203";
    private static final String INSTANCE4 = "20151204";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String sourceGrid;
    private String targetGrid;
    private long startTime = System.currentTimeMillis();
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
        
        // create datasets and instances to allow workflows to launch in parallel
        for (int pathType=0; pathType<=2; pathType++) {  // 3 different path formats being tested
            for (int replStrategy=0; replStrategy<=1; replStrategy++) {  // test hftp and distcp
                for (int pathOverride=0; pathOverride<=1; pathOverride++) {  // test target path overrides existing and not
                    for (int exclude=0; exclude<=1; exclude++) {  // test exclude paths set and not
                        String dataSetName = this.getDataSetName(pathType, replStrategy, pathOverride, exclude);
                        createDataset(dataSetName, pathType, replStrategy, pathOverride, exclude);  
                        createValidSourceInstances(dataSetName, pathType);
                        createBogusSourceInstances(dataSetName, pathType);
                    }
                }
            }
        }
        
        // validate replication workflows and kick off retention discovery
        for (int pathType=0; pathType<=2; pathType++) {
            for (int replStrategy=0; replStrategy<=1; replStrategy++) {
                for (int pathOverride=0; pathOverride<=1; pathOverride++) {
                    for (int exclude=0; exclude<=1; exclude++) {
                        String dataSetName = this.getDataSetName(pathType, replStrategy, pathOverride, exclude);
                        validateReplicationWorkflows(dataSetName);
                        enableRetention(dataSetName);
                    }
                }
            }
        }
        
        // validate retention workflows and files copied
        for (int pathType=0; pathType<=2; pathType++) {
            for (int replStrategy=0; replStrategy<=1; replStrategy++) {
                for (int pathOverride=0; pathOverride<=1; pathOverride++) {
                    for (int exclude=0; exclude<=1; exclude++) {
                        String dataSetName = this.getDataSetName(pathType, replStrategy, pathOverride, exclude);
                        validateRetentionWorkflows(dataSetName);
                        validateTargetFiles(dataSetName, pathType, pathOverride, exclude);
                    }
                }
            }
        }
        
        eligibleForDelete = true;
    }
    
    // validates files exist or not as expected on target
    private void validateTargetFiles(String dataSetName, int pathType, int pathOverride, int exclude) {
        validateTargetFiles(dataSetName, pathType, pathOverride, exclude, INSTANCE1, false);  // copied, deleted
        validateTargetFiles(dataSetName, pathType, pathOverride, exclude, INSTANCE2, true);   // copied
        validateTargetFiles(dataSetName, pathType, pathOverride, exclude, INSTANCE3, true);   // copied
        validateTargetFiles(dataSetName, pathType, pathOverride, exclude, INSTANCE4, false);  // invalid, not discovered
    }
    
    private void validateTargetFiles(String dataSetName, int pathType, int pathOverride, int exclude, String instance, boolean exists) {
        boolean excludePathExists = exists;
        if (exclude == 1) {
            excludePathExists = false;
        }
        if (pathType == 0) {
            if (pathOverride == 0) {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/load_time=" + instance), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/load_time=" + instance), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/load_time=" + instance), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/load_time=" + instance));
            } else {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/load_time=" + instance), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/load_time=" + instance), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/load_time=" + instance), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/load_time=" + instance), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/load_time=" + instance));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/load_time=" + instance));
            }
        } else if (pathType == 1) {
            if (pathOverride == 0) {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/data/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/count/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/invalid/"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/raw/"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/schema/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/status/"), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/status/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/status/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/status/"));
            } else {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/data/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/count/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/invalid/"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/raw/"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/schema/"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed2/status/"), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/" + instance + "/feed1/status/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed2/status/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/data/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/count/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/invalid/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/raw/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/schema/"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/" + instance + "/feed1/status/"));
            }
        } else if (pathType == 2) {
            if (pathOverride == 0) {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/" + instance + "0000"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/" + instance + "0000"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/" + instance + "0000"), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/" + instance + "0001"));
                
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/" + instance + "0001"));
            } else {
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/" + instance + "0000"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/" + instance + "0000"), excludePathExists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/" + instance + "0000"), exists);
                Assert.assertEquals(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/" + instance + "0000"), exists);
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/data/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/count/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/invalid/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/raw/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/schema/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/override/status/" + instance + "0001"));
                
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/" + instance + "0000"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/data/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/count/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/invalid/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/raw/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/schema/" + instance + "0001"));
                Assert.assertFalse(this.consoleHandle.filesExist(this.targetGrid, "/projects/foo/" + dataSetName + "/status/" + instance + "0001"));
            }
        } else {
            Assert.fail("Unknown pathType: " + pathType);
        }
    }
    
    // touch a dataset to re-discover for retention
    private void enableRetention(String dataSetName) {
        this.consoleHandle.setRetentionPolicyToAllDataSets(dataSetName, "3");
        this.consoleHandle.setRetentionPolicyToAllDataSets(dataSetName, "2");
    }
    
    private void createTopLevelDirOnTarget() throws Exception {
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.targetGrid, "/projects", "foo", "bogus" + startTime);
        createInstanceOnGridObj.execute();
    }
    
    private void validateRetentionWorkflows(String dataSetName) {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(dataSetName, "retention", INSTANCE1));
    }
    
    private void validateReplicationWorkflows(String dataSetName) {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
        Assert.assertTrue("Expected workflow to pass", workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE3));
    }
    
    private String getDataSetName(int pathType, int replStrategy, int pathOverride, int exclude) {
        String dataSetName = "DatePath_" + pathType + replStrategy + pathOverride + exclude + "_" + this.startTime;
        return dataSetName;
    }
    
    private void createDataset(String dataSetName, int pathType, int replStrategy, int pathOverride, int exclude) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DatePathReplDataset.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("DATASET_NAME", dataSetName);
        dataSetXml = dataSetXml.replaceAll("PARAMS", "");
        dataSetXml = dataSetXml.replaceAll("SOURCE_GRID", this.sourceGrid);
        dataSetXml = dataSetXml.replaceAll("TARGET_GRID", this.targetGrid);
        dataSetXml = dataSetXml.replaceAll("SOURCE_PATHS", this.getSourcePathsTag(dataSetName, pathType));
        dataSetXml = dataSetXml.replaceAll("TARGET_PATHS", this.getTargetPathsTag(dataSetName, pathType, pathOverride));
        dataSetXml = dataSetXml.replaceAll("RETENTION_ENABLED", "TRUE");
        dataSetXml = dataSetXml.replaceAll("START_DATE", "20000101");
        if (replStrategy == 0) {
            dataSetXml = dataSetXml.replaceAll("REPL_STRATEGY", "HFTPDistributedCopy");
        } else if (replStrategy == 1) {
            dataSetXml = dataSetXml.replaceAll("REPL_STRATEGY", "DistCp");
        } else {
            Assert.fail("Unknown replStrategy: " + replStrategy);
        }
        
        if (exclude == 0) {
            dataSetXml = dataSetXml.replaceAll("EXCLUDE", "");
        } else {
            dataSetXml = dataSetXml.replaceAll("EXCLUDE", "raw,invalid");
        }
        
        Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private String getSourcePathsTag(String dataSetName, int pathType) {
        if (pathType == 0) {
            return "<Paths>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/data/load_time=%{date}\" type=\"data\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/count/load_time=%{date}\" type=\"count\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/invalid/load_time=%{date}\" type=\"invalid\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/raw/load_time=%{date}\" type=\"raw\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/schema/load_time=%{date}\" type=\"schema\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/status/load_time=%{date}\" type=\"status\"/>\n" + 
                   "</Paths>\n";
        } else if (pathType == 1) {
            return "<Paths>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/data\" type=\"data\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/count\" type=\"count\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/invalid\" type=\"invalid\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/raw\" type=\"raw\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/schema\" type=\"schema\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/%{date}/feed2/status\" type=\"status\"/>\n" + 
                   "</Paths>\n";
        } else if (pathType == 2) {
            return "<Paths>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/data/%{date}0000\" type=\"data\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/count/%{date}0000\" type=\"count\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/invalid/%{date}0000\" type=\"invalid\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/raw/%{date}0000\" type=\"raw\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/schema/%{date}0000\" type=\"schema\"/>\n" + 
                   "<Path location=\"/projects/foo/" + dataSetName + "/status/%{date}0000\" type=\"status\"/>\n" + 
                   "</Paths>\n";
            
        } else {
            Assert.fail("Unknown pathType: " + pathType);
            return null;
        }
    }
    
    private String getTargetPathsTag(String dataSetName, int pathType, int pathOverride) {
        if (pathOverride == 0) {
            return "";
        } else if (pathOverride == 1) {
            if (pathType == 0) {
                return "<Paths>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/data/load_time=%{date}\" type=\"data\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/count/load_time=%{date}\" type=\"count\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/invalid/load_time=%{date}\" type=\"invalid\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/raw/load_time=%{date}\" type=\"raw\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/schema/load_time=%{date}\" type=\"schema\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/status/load_time=%{date}\" type=\"status\"/>\n" + 
                       "</Paths>\n";
            } else if (pathType == 1) {
                return "<Paths>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/data\" type=\"data\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/count\" type=\"count\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/invalid\" type=\"invalid\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/raw\" type=\"raw\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/schema\" type=\"schema\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/%{date}/feed2/status\" type=\"status\"/>\n" + 
                       "</Paths>\n";
            } else if (pathType == 2) {
                return "<Paths>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/data/%{date}0000\" type=\"data\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/count/%{date}0000\" type=\"count\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/invalid/%{date}0000\" type=\"invalid\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/raw/%{date}0000\" type=\"raw\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/schema/%{date}0000\" type=\"schema\"/>\n" + 
                       "<Path location=\"/projects/foo/" + dataSetName + "/override/status/%{date}0000\" type=\"status\"/>\n" + 
                       "</Paths>\n";
                
            } else {
                Assert.fail("Unknown pathType: " + pathType);
                return null;
            }
        } else {
            Assert.fail("Unknown pathOverride: " + pathOverride);
            return "";
        }
    }
    
    private void createValidSourceInstances(String dataSetName, int pathType) throws Exception {
        this.createValidSourceInstances(dataSetName, pathType, INSTANCE1);
        this.createValidSourceInstances(dataSetName, pathType, INSTANCE2);
        this.createValidSourceInstances(dataSetName, pathType, INSTANCE3);
    }
    
    private void createValidSourceInstances(String dataSetName, int pathType, String instanceId) throws Exception {
        if (pathType == 0) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "count/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "invalid/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "raw/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "schema/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "status/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "data/", "load_time=" + instanceId);
            createInstanceOnGridObj.execute();
        } else if (pathType == 1) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "count");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "invalid");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "raw");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "schema");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "status");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , instanceId + "/feed2", "data");
            createInstanceOnGridObj.execute();
        } else if (pathType == 2) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "count/", instanceId + "0000");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "invalid/", instanceId + "0000");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "raw/", instanceId + "0000");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "schema/", instanceId + "0000");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "status/", instanceId + "0000");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "data/", instanceId + "0000");
            createInstanceOnGridObj.execute();
        } else {
            Assert.fail("Unknown pathType: " + pathType);
        }
    }
    
    private void createBogusSourceInstances(String dataSetName, int pathType) throws Exception {
        if (pathType == 0) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "count/", "load_time=" + INSTANCE4);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "invalid/", "load_time=" + INSTANCE4);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "raw/", "load_time=" + INSTANCE4);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "schema/", "load_time=" + INSTANCE4);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "status/", "load_time=" + INSTANCE4);
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "data/", INSTANCE4);
            createInstanceOnGridObj.execute();
        } else if (pathType == 1) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "count");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "invalid");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "raw");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "schema");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "status");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , INSTANCE4 + "/feed1", "data");
            createInstanceOnGridObj.execute();
        } else if (pathType == 2) {
            CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "count/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "invalid/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "raw/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "schema/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "status/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
            createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/projects/foo/" + dataSetName + "/" , "data/", INSTANCE4 + "0001");
            createInstanceOnGridObj.execute();
        } else {
            Assert.fail("Unknown pathType: " + pathType);
        }
    }
    
    @After
    public void tearDown() throws Exception {
        for (int pathType=0; pathType<=2; pathType++) {
            for (int replStrategy=0; replStrategy<=1; replStrategy++) {
                for (int pathOverride=0; pathOverride<=1; pathOverride++) {
                    for (int exclude=0; exclude<=1; exclude++) {
                        String dataSetName = this.getDataSetName(pathType, replStrategy, pathOverride, exclude);
                        Response response = this.consoleHandle.deactivateDataSet(dataSetName);
                        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
                        
                        if (this.eligibleForDelete ) {
                            this.consoleHandle.removeDataSet(dataSetName);
                        }
                    }
                }
            }
        }
    }
}


