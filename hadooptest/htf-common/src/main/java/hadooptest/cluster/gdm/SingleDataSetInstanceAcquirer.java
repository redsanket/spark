// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.TestSession;

public class SingleDataSetInstanceAcquirer {
    private ConsoleHandle console;
    private String feedSubmisionTime;
    private String targetGrid;
    private String date;
    private String dataPath;
    private String schemaPath;
    private String countPath;
    private String dataSetName;
    private String feed;
    private static final String fdiServerName = "fdi_autotest_server";
    private boolean hcatEnabled;
    
    /**
     * Acquires a single dataset instance to a single grid 
     *
     * @param targetGrid    the grid to acquire to
     * @param date               the dataSet instance instance (daily)
     * @param dataPath       desired data path - (/data/gdmqe/auto2/data/${DataSetName}/%{date})
     * @param schemaPath   desired schema path
     * @param countPath    desired count path
     */
    public SingleDataSetInstanceAcquirer(String targetGrid, String date, String dataPath, String schemaPath, String countPath, String feed, boolean hcatEnabled) {
        this.console = new ConsoleHandle();
        this.targetGrid = targetGrid;
        this.date = date;
        this.dataPath = dataPath;
        this.schemaPath = schemaPath;
        this.countPath = countPath;
        this.dataSetName = ("SingleAcquisition_" + System.currentTimeMillis());
        this.feed = feed;
        this.hcatEnabled = hcatEnabled;
    }
    
    /**
     * Starts the aquisition, but does not poll for finish.
     */
    public void startAcquisition() throws Exception {
    
        createFDIServer();
    
        TestSession.logger.info("Creating dataSet " + dataSetName + " to acquire a single dataSet instance");
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/FDI_1_Target.xml");
        String dataSetXml = this.console.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("GRID_TARGET", targetGrid);
        dataSetXml = dataSetXml.replaceAll("START_DATE", date);
        dataSetXml = dataSetXml.replaceAll("END_DATE", date);
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", dataPath);
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", schemaPath);
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", countPath);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feed);
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", fdiServerName);
        
        if (hcatEnabled == true) {
            dataSetXml = dataSetXml.replaceAll("HCAT_USAGE", "<HCatUsage>TRUE</HCatUsage>");
        } else {
            dataSetXml = dataSetXml.replaceAll("HCAT_USAGE", "<HCatUsage>FALSE</HCatUsage>");
        }
        
        TestSession.logger.info("startAcquisition: dataSetName - " + dataSetName);
        TestSession.logger.debug("startAcquisition: dataSetXml - " + dataSetXml);
        
        Response response = this.console.createDataSet(dataSetName, dataSetXml);        

        if (response.getStatusCode() != 200) {
            throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
        
        this.console.checkAndActivateDataSet(dataSetName);
        feedSubmisionTime = GdmUtils.getCalendarAsString();     
    }
    
    private void createFDIServer() throws Exception {
        // check if FDI server exists 
        String dataSource = this.console.getDataSourceXml(fdiServerName);
        if (dataSource == null) {
            TestSession.logger.info(fdiServerName + " dataSource does not exist, creating");
            String dataSourceConfigFile = Util.getResourceFullPath("gdm/datasourceconfigs/FDI_DataSource_Template.xml");
            String xmlFileContent = GdmUtils.readFile(dataSourceConfigFile);
            xmlFileContent = xmlFileContent.replace("FDI_SERVER_NAME", fdiServerName);
            xmlFileContent = xmlFileContent.replace("GDM_CONSOLE_NAME", TestSession.conf.getProperty("GDM_CONSOLE_NAME"));
            boolean created = this.console.createDataSource(xmlFileContent);
            if (created == false) {
                throw new Exception("Failed to create dataSource " + fdiServerName + " from xml: " + xmlFileContent);
            }
            dataSource = this.console.getDataSourceXml(fdiServerName);
            if (dataSource == null) {
                throw new Exception("Failed to fetch dataSource " + fdiServerName);
            }
        }
        TestSession.logger.info("DataSource " + fdiServerName + " exists");
    }
    
    /**
     * Finishes the aquisition and disables the dataSet.
     */
    public void finishAcquisition() throws Exception {
        long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
        String workflowStatus = this.console.waitForWorkflowExecution(dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);

        if(!(workflowStatus.equals("COMPLETED"))) {
            throw new Exception("Workflow not complete.");
        }

        Response response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), dataSetName);
        if(!((response.getElementAtPath("/completedWorkflows/[0]/FacetName")).equals("acquisition"))) {
            throw new Exception("It's not an acquisition job");
        }
        
        if(!((response.getElementAtPath("/completedWorkflows/[0]/Attempt")).equals("1"))) {
            TestSession.logger.info("Mismatched attempt in response " + response);
            throw new Exception("Attempt != 1");
        }
        
        if(!((response.getElementAtPath("/completedWorkflows/[0]/CurrentStep")).equals("data.commit"))) {
            throw new Exception("data.commit not done");
        }
        
        response = this.console.deactivateDataSet(dataSetName);

        if (response.getStatusCode() != 200) {
            throw new Exception("response " + response + " had status code " + response.getStatusCode());
        }

        if(!((response.getElementAtPath("/Response/ActionName")).equals("terminate"))) {
            throw new Exception("ActionName.");
        }

        if(!((response.getElementAtPath("/Response/ResponseId")).equals("0"))) {
            throw new Exception("ResponseId");
        }
        
        if(!((response.getElementAtPath("/Response/ResponseMessage/[0]")).equals("Operation on " + dataSetName + " was successful."))) {
            throw new Exception("ResponseMessage.");
        }
    }

    /**
     *  Runs the complete acquisition job from start to finish
     */
    public void acquireSingleDataSetInstance() throws Exception {
        startAcquisition();
        finishAcquisition();
    }
}
