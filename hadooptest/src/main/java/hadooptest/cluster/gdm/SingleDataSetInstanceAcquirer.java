package hadooptest.cluster.gdm;

import coretest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.TestSession;

public class SingleDataSetInstanceAcquirer {
    private ConsoleHandle console;
    private String dataSetConfigBase = Util.getResourceFullPath("gdm/datasetconfigs") + "/";
    private String feedSubmisionTime;
    private String targetGrid;
    private String date;
    private String dataPath;
    private String schemaPath;
    private String countPath;
    private String dataSetName;
    private String feed;
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
        TestSession.logger.info("Creating dataSet " + dataSetName + " to acquire a single dataSet instance");
        String dataSetXml = this.console.createDataSetXmlFromConfig(dataSetName, dataSetConfigBase + "FDI_1_Target.xml");
        dataSetXml = dataSetXml.replaceAll("GRID_TARGET", targetGrid);
        dataSetXml = dataSetXml.replaceAll("START_DATE", date);
        dataSetXml = dataSetXml.replaceAll("END_DATE", date);
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", dataPath);
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", schemaPath);
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", countPath);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feed);
        
        if (hcatEnabled == true) {
            dataSetXml = dataSetXml.replaceAll("HCAT_USAGE", "<HCatUsage>TRUE</HCatUsage>");
        } else {
            dataSetXml = dataSetXml.replaceAll("HCAT_USAGE", "<HCatUsage>FALSE</HCatUsage>");
        }
        
        Response response = this.console.createDataSet(dataSetName, dataSetXml);        

        if(!(Integer.toString(200).equals(response.getStatusCode()))) {
        	throw new Exception("Response status code does not equal 200.");
        }
        
        this.console.checkAndActivateDataSet(dataSetName);
        feedSubmisionTime = GdmUtils.getCalendarAsString();     
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
        
        if(!((response.getElementAtPath("/completedWorkflows/[0]/Attempt")).equals(Integer.valueOf(1)))) {
        	throw new Exception("Attempt != 1");
        }
        
        if(!((response.getElementAtPath("/completedWorkflows/[0]/CurrentStep")).equals("data.commit"))) {
        	throw new Exception("data.commit not done");
        }
        
        response = this.console.deactivateDataSet(dataSetName);

        if(!(Integer.toString(200).equals(response.getStatusCode()))) {
        	throw new Exception("Response status code does not equal 200.");
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
