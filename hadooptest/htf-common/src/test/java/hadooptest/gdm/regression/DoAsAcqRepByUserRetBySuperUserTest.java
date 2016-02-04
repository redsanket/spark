// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.Util;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DoAsDataSetInstanceCreateAndWorflowChecker;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;

@Category(SerialTests.class)
public class DoAsAcqRepByUserRetBySuperUserTest extends TestSession {

    private ConsoleHandle console;
    private String cookie;
    private String url ; 
    private String acqRepdataSetName;
    private String retDataSetName;
    private String acqRepUserName;
    private String acqRepGroupName;
    private String acqRepRunAsOwner;
    private String target1;
    private String target2;
    private String retentionRunAsOwner;
    private static final int SUCCESS = 200;
    public HashMap<String, String> acqDataSetInfo ;
    public HashMap<String, String> retDataSetInfo;
    public DoAsDataSetInstanceCreateAndWorflowChecker doAsDataSetInstanceCreateAndWorflowCheckerObj;
    private String dataSetConfigBase;
    public static String acquisitionDataSetName ;
    public static String replicationDataSetName ;
    public static final String HadoopLS = "/console/api/admin/hadoopls";

    @Before
    public void setUp() throws NumberFormatException, Exception {
        this.console = new ConsoleHandle();
        acqDataSetInfo = new HashMap<String , String>();
        retDataSetInfo = new HashMap<String , String>();

        this.url =  "http://" + TestSession.conf.getProperty("GDM_CONSOLE_NAME") + ":" + Integer.parseInt(TestSession.conf.getProperty("GDM_CONSOLE_PORT"));

        // Get all the configuration value from conf.xml file
        this.acqRepGroupName = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.acqRepGroupName");
        if (this.acqRepGroupName == null) {
            Assert.fail("Failed to find testconfig.DoAsAcqRepByUserRetBySuperUserTest.acqRepGroupName");
        }
        this.acqRepUserName = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.acqRepUserName");
        this.acqRepRunAsOwner = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.acqRepRunAsOwner");
        this.target1 = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.TARGET1");
        this.target2 = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.TARGET2");
        this.retentionRunAsOwner = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.retRunAsOwner");
        this.dataSetConfigBase = Util.getResourceFullPath("gdm/datasetconfigs") + "/";

        HTTPHandle httpHandle = new HTTPHandle();
        cookie = httpHandle.getBouncerCookie();

        doAsDataSetInstanceCreateAndWorflowCheckerObj = new DoAsDataSetInstanceCreateAndWorflowChecker();
    }
    
    @Test
    public void testWorkFlow() {
        testDoAsAcqAndRetWorkFlow();
        testDoAsRetWorkFlow();
    }

    /**
     * Method that creates dataset for Acquisition and Replication and check for workflow and files on HDFS 
     */
    private void testDoAsAcqAndRetWorkFlow() {

        // set dataset name for acquisition and replication workflow
        this.acqRepdataSetName = "AcqRepBy_" + this.acqRepUserName + "_User_" + this.acqRepGroupName +"_Group_" + System.currentTimeMillis();
        acquisitionDataSetName = this.acqRepdataSetName;

        TestSession.logger.info("********************  DoAsAcqRepByUserRetBySuperUserTest  ************************");
        TestSession.logger.info("  groupName = "+this.acqRepGroupName  + "  userName = "+this.acqRepUserName  +"  runAsOwner = "+this.acqRepRunAsOwner  +"  target1 = "+this.target1 +"   target2 = "+this.target2  +"   retRunAsOwneer = " + this.retentionRunAsOwner) ;

        // set groupName for UGI tag attribute
        StringBuilder acqRepUgiTag = new StringBuilder();
        if (this.acqRepGroupName != null ) {
            acqRepUgiTag.append("  group=\""+ this.acqRepGroupName +"\"");
        }
        if (this.acqRepUserName != null) {
            acqRepUgiTag.append("  owner=\""+ this.acqRepUserName +"\"");
        }
        TestSession.logger.info("acqRepUgiTag = " + acqRepUgiTag);

        // set all the required values for acq and rep dataset
        acqDataSetInfo.put("RUN_AS_OWNER",this.acqRepRunAsOwner );
        acqDataSetInfo.put("TARGET1", this.target1);
        acqDataSetInfo.put("TARGET2", this.target2);
        if(acqRepUgiTag.length() > 0 ){
            acqDataSetInfo.put("GROUP_OWNER_VALUE",acqRepUgiTag.toString());
        }

        String dataSetXml = doAsDataSetInstanceCreateAndWorflowCheckerObj.createAcqRepDataSetXml(this.console, this.acqRepdataSetName, this.dataSetConfigBase , "DoAsAcqRepDataSet.xml", acqDataSetInfo);
        TestSession.logger.info("dataSetXml = " + dataSetXml);

        // Create a new dataset
        hadooptest.cluster.gdm.Response response = this.console.createDataSet(this.acqRepdataSetName, dataSetXml);
        assertTrue("Failed to create a new acquisition dataset. " + response.getStatusCode() , response.getStatusCode()==SUCCESS);

        // activate the dataset & check for acquisition and replication workflow
        try {
            this.console.checkAndActivateDataSet(this.acqRepdataSetName);
            String datasetActivationTime = GdmUtils.getCalendarAsString();
            doAsDataSetInstanceCreateAndWorflowCheckerObj.testAcqRepWorkFlow(this.console, this.acqRepdataSetName, datasetActivationTime);
        } catch (Exception e) { 
            e.printStackTrace();
        }

        // check for hdfs information.
        doAsDataSetInstanceCreateAndWorflowCheckerObj.compareAcqReplHDFSFileInfo(acquisitionDataSetName , this.url , this.cookie);
    }

    /**
     * Method that creates dataset for retention workflow and checks for files on HDFS, after retention no files should exists on hdfs 
     */
    private void testDoAsRetWorkFlow() {

        boolean result = false;
        String datasetActivationTime = null ;

        // set dataset name for retention workflow
        this.retDataSetName = "RetBySuperUser_" + System.currentTimeMillis();
        replicationDataSetName = this.retDataSetName;
        TestSession.logger.info("  acqRepdataSetName  = " + acquisitionDataSetName   + "   retDataSetName = " + this.retDataSetName);

        // set all the required values for ret dataset
        retDataSetInfo.put("RUN_AS_OWNER",this.retentionRunAsOwner );
        retDataSetInfo.put("TARGET1", this.target1);
        retDataSetInfo.put("TARGET2", this.target2);

        String dataSetXml = doAsDataSetInstanceCreateAndWorflowCheckerObj.createRetDataSetXml(this.console, this.retDataSetName, this.dataSetConfigBase , acquisitionDataSetName ,  "DoAsRetentionDataSet.xml", retDataSetInfo);
        TestSession.logger.info("dataSetXML  = " + dataSetXml);

        // Create a new retention dataset
        hadooptest.cluster.gdm.Response response = this.console.createDataSet(this.retDataSetName, dataSetXml);
        assertTrue("Failed to create a new retention dataset. " + response.getStatusCode() , response.getStatusCode()==SUCCESS);

        try {
            // activate the dataset
            this.console.checkAndActivateDataSet(this.retDataSetName);
            datasetActivationTime = GdmUtils.getCalendarAsString();

            String retentionWorkFlowExecutionResult = doAsDataSetInstanceCreateAndWorflowCheckerObj.testRetWorkFlow(this.console, this.retDataSetName, datasetActivationTime);
            result = retentionWorkFlowExecutionResult.equals("FAILED");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (result) {
            this.console.getFailureInformation(this.url , this.cookie , this.retDataSetName, datasetActivationTime);
        }
        assertTrue("Retention workflow for " + this.retDataSetName + " dataset failed. User =  " + this.acqRepUserName + "   group = " + this.acqRepGroupName  ,  result == false );

        // check for files exists on HDFS  on Acquisition facet
        doAsDataSetInstanceCreateAndWorflowCheckerObj.checkForFilesOnClusterAfterRetentionWorkflow(this.target1, this.url, this.cookie, "/data/daqdev/data/" , acquisitionDataSetName );

        // check for files exists on HDFS fon replication facet
        doAsDataSetInstanceCreateAndWorflowCheckerObj.checkForFilesOnClusterAfterRetentionWorkflow(this.target2, this.url, this.cookie, "/data/daqdev/data/" , replicationDataSetName );
    }

    @After
    public  void tearDown() {
        doAsDataSetInstanceCreateAndWorflowCheckerObj.deactivateDoAsDataSet(this.console, acquisitionDataSetName);
        doAsDataSetInstanceCreateAndWorflowCheckerObj.deactivateDoAsDataSet(this.console, replicationDataSetName);
    }
}
