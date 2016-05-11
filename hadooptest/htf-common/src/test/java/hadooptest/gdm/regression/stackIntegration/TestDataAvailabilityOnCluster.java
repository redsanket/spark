// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.stackIntegration;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.mail.MessagingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;


import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.integration.IntegrateHBase;
import hadooptest.gdm.regression.integration.IntegrateHive;
import hadooptest.gdm.regression.integration.IntegrateTez;
import hadooptest.gdm.regression.integration.IntegrationJobSteps;
import hadooptest.gdm.regression.integration.JobState;
import hadooptest.gdm.regression.integration.metrics.NameNodeThreadInfo;
import hadooptest.gdm.regression.integration.metrics.NameNodeThreadJob;
import hadooptest.gdm.regression.integration.report.SendIntegrationReportMail;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestDataAvailabilityOnCluster {

    private final static String PATH = "/data/daqdev/abf/data/";
    private final static String TEST_SCENARIO = "Test Whether data available for current Hour - "; 
    private final static String DB_COLUMN_NAME = "dataAvailable";
    private final static String DATA_UNAVAILABLE = "unavailable";
    private final static String DATA_AVAILABLE = "available";
    private final static String POLLING = "polling";
    private final int DATA_AVAILABILITY_INTERVAL_CHECKER = 60;
    private static final String PROCOTOL = "hdfs://";
    private String currentStatus;
    private String nameNode;
    private String currentHour;
    private String dataSetName;
    public FileSystem hdfsFileSystem ;
    private CommonFunctions commonFunctionObject;
    private Configuration configuration;

    public TestDataAvailabilityOnCluster(String nameNodeName) throws IOException {
        this.commonFunctionObject = new CommonFunctions();
        this.setNameNode(nameNodeName);
        this.setCurrentHour(this.commonFunctionObject.getCurrentHourPath());
    }
    
    public String getDataSetName() {
        return dataSetName;
    }

    public void setDataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    public String getNameNode() {
        return nameNode;
    }

    public void setNameNode(String nameNode) {
        this.nameNode = nameNode;
    }
    
    public String getCurrentHour() {
        return currentHour;
    }

    public void setCurrentHour(String currentHour) {
        this.currentHour = currentHour;
    }

    /**
     * Returns the remote cluster configuration object.
     * @param aUser  - user
     * @param nameNode - name of the cluster namenode. 
     * @return
     * @throws IOException 
     */
    public Configuration getConfForRemoteFS() throws IOException {
        Configuration conf = new Configuration(true);
        String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.getNameNode();
        TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
        conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
        TestSession.logger.info(conf);
        return conf;
    }

    public boolean pollForDataAvaiability() throws IOException {
        
        this.setDataSetName(this.commonFunctionObject.getDataSetName());
        
        this.configuration = this.getConfForRemoteFS();
        boolean flag =  false;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar todayCal = Calendar.getInstance();
        Calendar LastdayCal = Calendar.getInstance();
        Calendar currentCal = Calendar.getInstance();
        long toDay = Long.parseLong(sdf.format(todayCal.getTime()));

        // set the duration for how long the data has to generate.
        LastdayCal.add(Calendar.DAY_OF_WEEK_IN_MONTH , 1);
        long lastDay = Long.parseLong(sdf.format(LastdayCal.getTime()));
        TestSession.logger.info(" Current date - " + sdf.format(todayCal.getTime()));
        TestSession.logger.info(" Next date - " + sdf.format(LastdayCal.getTime()));

        Calendar initialCal = Calendar.getInstance();
        Calendar futureCal = Calendar.getInstance();

        long initialMin = Long.parseLong(sdf.format(initialCal.getTime()));
        initialCal.add(Calendar.MINUTE, 1);
        long futureMin =  Long.parseLong(sdf.format(initialCal.getTime()));
        TestSession.logger.info(" intialMin   = " +  initialMin   + "  futureMin  =  "  + futureMin);
        SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
        feed_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat slaSDF = new SimpleDateFormat("yyyyMMddHHmm");
        slaSDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar salStartCal = Calendar.getInstance();
        Calendar salEndCal = Calendar.getInstance();
        long slaStart =  Long.parseLong(sdf.format(initialCal.getTime()));
        initialCal.add(Calendar.MINUTE, 1);
        long slaEnd = Long.parseLong(sdf.format(initialCal.getTime()));
        long pollEnd =  Long.parseLong(sdf.format(initialCal.getTime()));
        Calendar pollCalendar = Calendar.getInstance();

        while (toDay <= lastDay) {
            Date d = new Date();
            long initTime = Long.parseLong(sdf.format(d));
            long slaStartTime = Long.parseLong(slaSDF.format(d));
            long pollStart = Long.parseLong(slaSDF.format(d));

            if (initTime >= futureMin ) {
                initialCal = Calendar.getInstance();
                initialMin = Long.parseLong(feed_sdf.format(initialCal.getTime()));
                initialCal.add(Calendar.HOUR, 1);
                futureMin =  Long.parseLong(feed_sdf.format(initialCal.getTime()));
                TestSession.logger.info("------- hr has started..! -------------");
            }

            if (checkSuccessPathExists() == true) {
                flag = true;
                break;
            } else {
                TestSession.logger.info("Waiting for the data on the cluster.");
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            d = new Date();
            initTime = Long.parseLong(feed_sdf.format(d));
            d = null;
            toDay = Long.parseLong(feed_sdf.format(currentCal.getTime()));
            TestSession.logger.info("Next data polling will start @ " + futureMin + "  and  current  time = " + initTime);
        }
        return flag;
    }

    public boolean checkSuccessPathExists() {
        boolean flag = false , exceptionArised = false;
        try {
            FileSystem hdfsFileSystem = FileSystem.get(this.configuration);
            String hdfsPath = PATH + this.getCurrentHour();
            TestSession.logger.info("hdfsPath  = " + hdfsPath);
            Path path  = new Path(hdfsPath);
            if (hdfsFileSystem.exists(path)) {
                flag = true;
            }
        } catch (IOException e) {
            exceptionArised = true;
            this.commonFunctionObject.updateDB(this.getDataSetName(), "hadoopResult" , "FAIL");
            this.commonFunctionObject.updateDB(this.getDataSetName(), "hadoopCurrentState" , "COMPLETED");
            this.commonFunctionObject.updateDB(this.getDataSetName(), "gdmResult" , "FAIL");
            this.commonFunctionObject.updateDB(this.getDataSetName(), "gdmCurrentState" , "COMPLETED");
            e.printStackTrace();
        }finally {
            if (flag == true) {
                this.commonFunctionObject.updateDB(this.getDataSetName(), "hadoopResult" , "PASS");
                this.commonFunctionObject.updateDB(this.getDataSetName(), "hadoopCurrentState" , "COMPLETED");
                this.commonFunctionObject.updateDB(this.getDataSetName(), "gdmResult" , "PASS");
                this.commonFunctionObject.updateDB(this.getDataSetName(), "gdmCurrentState" , "COMPLETED");
            } else if (flag == false && exceptionArised == false) {
                this.commonFunctionObject.updateDB(this.getDataSetName(), "hadoopCurrentState" , "RUNNING");
                this.commonFunctionObject.updateDB(this.getDataSetName(), "gdmCurrentState" , "RUNNING");
            }
        }
        return flag;
    }
}
