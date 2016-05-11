// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm.report;

import hadooptest.TestSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import static org.junit.Assert.assertTrue;

/**
 * Class that generates report dynamically while the test is executing.
 * 
 */
public class GDMTestExecutionReport extends RunListener {

    private String filePath;
    private String resultFolderPath;
    private String reportFilePath;
    private String testResult;
    private String failureExeceptionString;
    private String stackTrace;
    private static int slNo = 1;
    private static String methodName;
    private static int count;

    public GDMTestExecutionReport() throws IOException {
        this.filePath = new File("").getAbsoluteFile() + File.separator;
        this.resultFolderPath = filePath + "resources" + File.separator + "gdm" + File.separator + "results";
        this.reportFilePath = resultFolderPath + File.separator + "Result.html";
        if (this.slNo == 1) {
            startGDMTestReport();
        }
    }

    @Override
    public void testRunStarted(Description description) throws Exception {

    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        /*if (this.slNo >=  result.getRunCount() ) {
            stopGDMTestReport();
        }*/
    }

    @Override
    public void testStarted(Description description) throws Exception {
        this.testResult = "passed";
        TestSession.logger.info("*************************************************Started: " + description.getMethodName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        TestSession.logger.info("*************************************************Finished: " + description.getMethodName());
        StringBuilder rowTag = new StringBuilder();
        String className = description.getClassName();
        String mName = description.getMethodName();
        // used static count since  testStarted and testFinished is invoked multiple times and getMethodName() is also invoked so many times. 
        if (count == 0) {
            this.methodName = mName;
            rowTag.append("<br>TestCase Name : \t").append(className);
            rowTag.append("<br>TestCase Method Name : \t").append(this.methodName);
            rowTag.append("<br>Result : \t").append(this.testResult);
            if (this.testResult.equals("failed")) {
                rowTag.append("<br><font color=\"red\" > Failed Reason : \t").append(this.failureExeceptionString);
                rowTag.append("<br>StackTrace : \t").append(this.stackTrace).append("</font>");
            }
            rowTag.append("<br><hr>");
            FileUtils.writeStringToFile(new File(this.reportFilePath), rowTag.toString() , "UTF8" ,  true);
            this.slNo ++;
            rowTag = null;
            this.testResult = "";
            count++;
        } else if (count == 1) {
            count = 0;
        }
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        this.testResult = "failed";
        this.failureExeceptionString = failure.getException().toString();
        this.stackTrace = failure.getTrace();
        TestSession.logger.info("Failed: " + failure.getDescription().getMethodName());
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
        TestSession.logger.info("Failed: " + failure.getDescription().getMethodName());
    }

    @Override
    public void testIgnored(Description description) throws Exception {
        this.testResult = "igorned";
        TestSession.logger.info("Ignored: " + description.getMethodName());
    }

    /*
     * Close the html report file
     */
    private void stopGDMTestReport() throws Exception {
        String htmlCloseTag = "</tbody></table></center><br><br></body></html>";
        try {
            FileUtils.writeStringToFile(new File(this.reportFilePath), htmlCloseTag , "UTF8" ,  true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a result folder , Result.html file and add the html tag when test execution starts 
     */
    private void startGDMTestReport() {
        TestSession.logger.info("**********************************************************************************");
        TestSession.logger.info("Result folder - " + this.resultFolderPath);
        File resultFolder = new File(this.resultFolderPath);
        boolean isResultFolderExist = resultFolder.exists(); 
        TestSession.logger.info("Result folder - " + isResultFolderExist);
        if ( isResultFolderExist == false ) {
            TestSession.logger.info("**********************************************************************************");
            TestSession.logger.info(this.resultFolderPath +"  path does not exist, creating a new one");
            boolean isDirCreated = resultFolder.mkdir();
            assertTrue("Failed : To create " + resultFolderPath  , isDirCreated == true);
            TestSession.logger.info("isDirCreated = "  + isDirCreated  +  "  path = " + resultFolderPath);
            File reportFile = new File(reportFilePath);
            try {
                boolean  isReportFileCreated = reportFile.createNewFile();
                assertTrue("Failed : To create " + reportFilePath , isReportFileCreated);
                String htmlTag = "<html><head> <link rel=\"stylesheet\" href=\"http://yui.yahooapis.com/pure/0.6.0/pure-min.css\"> <meta http-equiv=\"refresh\" content=\"10\" > </head> " +
                        "<body><center><h2> GDM Test Result </h2>" + "<table class=\"pure-table pure-table-bordered\">" +
                        "<tr>  <th>TestCase Name </th> <th>Method Name </th> <th>Test Result </th> <th>Failure reason </th> <th> Comments </th> </tr> </thead> <tbody> \n" ;
                FileUtils.writeStringToFile(new File(this.reportFilePath), htmlTag , "UTF8" , true );
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (isResultFolderExist == true) {
            TestSession.logger.info("**********************************************************************************");
            TestSession.logger.info(this.resultFolderPath + " already created.");
            TestSession.logger.info("**********************************************************************************");
            try {
                String htmlTag = "<html><head> <link rel=\"stylesheet\" href=\"http://yui.yahooapis.com/pure/0.6.0/pure-min.css\"> <meta http-equiv=\"refresh\" content=\"10\" > </head> " +
                        "<body><center><h2> GDM Test Result </h2>" + "<table class=\"pure-table pure-table-bordered\">" +
                        "<tr>  <th>TestCase Name </th> <th>Method Name </th> <th>Test Result </th> <th>Failure reason </th> <th> Comments </th> </tr> </thead> <tbody> \n" ;
                FileUtils.writeStringToFile(new File(this.reportFilePath), htmlTag , "UTF8" , true );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
