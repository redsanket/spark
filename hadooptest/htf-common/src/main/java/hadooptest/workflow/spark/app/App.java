/*
 * YAHOO!
 */

package hadooptest.workflow.spark.app;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import hadooptest.Util;

/**
 * A class which should represent the base capability of any app
 * submitted to a cluster.
 */
public abstract class App extends Thread {

    /** The ID of the app. */
    protected String ID = "0";

    /** Error string if error occurred. */
    protected String ERROR = null;
    
    /** The user the app will run under */
    protected String USER = TestSession.conf.getProperty("USER", System.getProperty("user.name")); // The user for the app.
    
    /** The queue the app will run under */
    protected String QUEUE = "default";
    
    /** The process handle for the app when it is run from a system call */
    protected Process process = null;
    
    /** Whether the app should take time to wait for the app ID in the output before progressing */
    protected boolean appInitSetID = true;
    
    /**
     * Submit the app to the cluster through the Hadoop CLI.
     * 
     * @throws Exception if there is a fatal error running the app process.
     */
    protected abstract void submit() 
            throws Exception;
    
    /**
     * Submit the app to the cluster, but don't wait to assign an ID to this App.
     * Should only be intended for cases where you want to saturate a cluster
     * with Apps, and don't care about the status or result of the App.
     * 
     * Submit the app to the cluster through the Hadoop CLI.
     * 
     * @throws Exception if there is a fatal error running the app process.
     */
    protected abstract void submitNoID()
            throws Exception;
    
    /**
     * Get the process handle for a app submitted from a system call.
     * 
     * @return Process the handle to the app process.
     */
    public Process getProcess() {
        return this.process;
    }
    
    /**
     * Get the app ID.
     * 
     * @return String the app ID.
     */
    public String getID() {
        return this.ID;
    }
    
    /**
     * Implements Thread.run().
     * 
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     * 
     * @throws RuntimeException if there is a fatal runtime error when running the app thread.
     */
    public void run() {
        try {
            if (appInitSetID) {
                this.submit();
            }
            else {
                this.submitNoID();
            }
        }
        catch (IOException ioe) {
            TestSession.logger.error("IOException in App.run() triggered RuntimeException.", ioe);
            throw new RuntimeException(ioe.getMessage());
        }
        catch (InterruptedException ie) {
            TestSession.logger.error("InterruptedException in App.run() triggered RuntimeException.", ie);
            throw new RuntimeException(ie.getMessage());
        }
        catch (Exception e) 
        {
            TestSession.logger.error("Exception in App.run() triggered RuntimeException.", e);
            throw new RuntimeException(e.getMessage());
        }
    }
    
    /**
     * Get the status of the Application State through the Hadoop API.
     * 
     * @return YarnApplicaitonState the state of the App.
     * 
     * @throws IOException if there is a fatal error getting the app state.
     */
    public YarnApplicationState getYarnState() throws IOException, YarnException {

        YarnApplicationState state = this.getHadoopApp().getYarnApplicationState();
        TestSession.logger.debug("Application Status: " + state.toString());

        return state;
    }

    /**
     * Get the status of the App through the Hadoop API.
     * 
     * @return AppState the state of the App.
     * 
     * @throws IOException if there is a fatal error getting the app state.
     */
    public FinalApplicationStatus getYarnAppFinalStatus() throws IOException, YarnException {

        FinalApplicationStatus state = this.getHadoopApp().getFinalApplicationStatus();
        TestSession.logger.debug("Final Status: " + state.toString());

        return state;
    }
    
    
    /**
     * Get the name of the App through the Hadoop API.
     * 
     * @return String the name of the app.
     * 
     * @throws IOException if there is a fatal error getting the app name.
     */
    public String getAppName() throws IOException, YarnException {
        String name = null;

        name = this.getHadoopApp().getName();
        TestSession.logger.debug("App Name: " + name);
        return name;
    }
    

    /**
     * Get the Yarn API ApplicationReport that is represented by this application.
     * 
     * @return ApplicationReport the Hadoop API app represented by this app.
     * 
     * @throws IOException if there is a failure getting the Hadoop
     *         YarnClient. 
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     */
    public ApplicationReport getHadoopApp() throws IOException, YarnException {
        YarnClientImpl appClient = this.getHadoopAPIYarnClient();
        if (appClient == null) {
            TestSession.logger.error("Error getting yarn client, returned null!");
        }
        ApplicationReport report = appClient.getApplicationReport(ConverterUtils.toApplicationId(this.ID));
        if (report == null) {
            TestSession.logger.error("Error getting application report , returned null!");
        }
        return report;
    }
    
    /**
     * Sets a user for the app other than the default.
     * 
     * @param user The user to override the default user with.
     */
    public void setUser(String user) {
        this.USER = user;
    }
    
    /**
     * Sets a queue for the app other than the default.
     * 
     * @param queue The queue to override the default queue with.
     */
    public void setQueue(String queue) {
        this.QUEUE = queue;
    }
    
    /**
     * Set whether the app should wait for the ID in the output before proceeding.
     * 
     * If false, an ID will not be set and many functions of App will not work 
     * properly.  This should only be used when submitting many apps to a cluster
     * and the resulting state of the app is irrelevant.
     * 
     * @param setID whether we should wait for the app to initialize the ID.
     */
    public void setAppInitSetID(boolean setID) {
        this.appInitSetID = setID;
    }

    /**
     * Kills the app.  Uses yarn CLI to kill the app.
     * 
     * @return boolean Whether the app was successfully killed.
     * 
     * @throws Exception if there is a fatal error killing the app.
     */
    public boolean killCLI() throws Exception {
       return killCLI(this.USER);
    }

    /**
     * Kills the app as the user specified.  Uses yarn CLI to kill the app.
     * 
     * @param user name of the user to run the command as.
     * @return boolean Whether the app was successfully killed.
     * 
     * @throws Exception if there is a fatal error killing the app.
     */
    public boolean killCLI(String user) throws Exception {
        Process yarnProc = null;

        String[] yarnCmd = {
                TestSession.cluster.getConf().getHadoopProp("YARN_BIN"), 
                "--config", TestSession.cluster.getConf().getHadoopConfDir(),
                "application", "-kill", this.ID };

        TestSession.logger.debug(yarnCmd);

        String yarnPatternStr = "(.*)(Killed application " + this.ID + ")(.*)";
        Pattern yarnPattern = Pattern.compile(yarnPatternStr);
        
        try {
            HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
            Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
                EMPTY_ENV_HASH_MAP);
            environmentVariablesWrappingTheCommand.put("JAVA_HOME", HadooptestConstants.Location.JDK64);
            environmentVariablesWrappingTheCommand.put("HADOOP_CONF_DIR", TestSession.cluster.getConf().getHadoopConfDir());

            yarnProc = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
               yarnCmd, user, environmentVariablesWrappingTheCommand);

            BufferedReader reader=new BufferedReader(new InputStreamReader(yarnProc.getInputStream())); 
            String line=reader.readLine(); 
            while(line!=null) 
            { 
                TestSession.logger.debug(line);
                
                Matcher yarnMatcher = yarnPattern.matcher(line);
                
                if (yarnMatcher.find()) {
                    TestSession.logger.info("APP " + this.ID + " WAS KILLED");
                    return true;
                }
                
                line=reader.readLine();
            } 
        }
        catch (Exception e) {
            if (yarnProc != null) {
                yarnProc.destroy();
            }
            
            TestSession.logger.error("Exception " + e.getMessage(), e);
            throw e;
        }
        
        TestSession.logger.error("APP " + this.ID + " WAS NOT KILLED");
        return false;
    }

    /**
     * Get the yarn logs for this application.  Uses yarn CLI.
     * Assumes the user who started the job is calling it.
     * Application must be completed for this to work.
     * 
     * @return 
     * 
     * @throws Exception if there is a fatal error.
     */
    public boolean grepLogsCLI(String patternString) throws Exception {

        Process yarnProc = null;

        String[] yarnCmd = {
                TestSession.cluster.getConf().getHadoopProp("YARN_BIN"), 
                "--config", TestSession.cluster.getConf().getHadoopConfDir(),
                "logs", "-applicationId", this.ID };

        TestSession.logger.debug(yarnCmd);

        Pattern appPattern = Pattern.compile(patternString);
        
        try {
            HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
            Map<String, String> environmentVariablesWrappingTheCommand = new HashMap<String, String>(
                EMPTY_ENV_HASH_MAP);
            environmentVariablesWrappingTheCommand.put("JAVA_HOME", HadooptestConstants.Location.JDK64);
            environmentVariablesWrappingTheCommand.put("HADOOP_PREFIX", "");

            yarnProc = TestSession.exec.runProcBuilderSecurityGetProcWithEnv(
               yarnCmd, this.USER, environmentVariablesWrappingTheCommand);

            BufferedReader reader=new BufferedReader(new InputStreamReader(yarnProc.getInputStream())); 
            String line=reader.readLine(); 
            while(line!=null) 
            { 
                TestSession.logger.debug(line);

                Matcher appMatcher = appPattern.matcher(line);

                if (appMatcher.find()) {
                    TestSession.logger.info("found in line: " + line);
                    reader.close();
                    return true;
                }
                
                line=reader.readLine();
            } 
        }
        catch (Exception e) {
            if (yarnProc != null) {
                yarnProc.destroy();
            }
            
            TestSession.logger.error("Exception " + e.getMessage(), e);
            throw e;
        }
        
        TestSession.logger.error("Pattern: " + patternString + " for APP " + this.ID + " not found in logs");
        return false;
    }
    

    /**
     * Verifies that the app ID matches the expected format.
     * 
     * @return boolean Whether the app ID matches the expected format.
     */
    public boolean verifyID() {
        if (this.ID == "0") {
            TestSession.logger.error("APP ID DID NOT MATCH FORMAT AND WAS ZERO");
            return false;
        }

        String appPatternStr = "application_(.*)$";
        Pattern appPattern = Pattern.compile(appPatternStr);
        
        Matcher appMatcher = appPattern.matcher(this.ID);
        
        if (appMatcher.find()) {
            TestSession.logger.info("APPLICATION ID MATCHED EXPECTED FORMAT");
            TestSession.logger.info("APPLICATION ID: " + this.ID);
            return true;
        }
        else {
            TestSession.logger.error("APPLICATION ID DID NOT MATCH FORMAT");
            return false;
        }
    }

    /**
     * Sleep while waiting for an error. 
     * 
     * @param seconds the number of seconds to wait for a app ID.
     * @return boolean whether an ID was found or not within the specified
     *                     time interval.
     *
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     */
    public boolean waitForERROR(int seconds) throws InterruptedException {

        // Give the app time to associate with a app ID
        for (int i = 0; i <= seconds; i++) {
            if (this.ERROR == null) {
                Util.sleep(1);
            }
            else {
                return true;
            }
        }
        
        return false;
    }
    
    
    /**
     * Sleep while waiting for a app ID.
     * 
     * @param seconds the number of seconds to wait for a app ID.
     * @return boolean whether an ID was found or not within the specified
     *                     time interval.
     *
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     */
    public boolean waitForID(int seconds) throws InterruptedException {

        // Give the app time to associate with a app ID
        for (int i = 0; i <= seconds; i++) {
            if (this.ID.equals("0")) {
                Util.sleep(1);
            }
            else {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Waits indefinitely for the app to succeed, and returns true for success.
     * Uses the Hadoop API to check status of the app.
     * 
     * @return boolean whether the app succeeded
     * 
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     * @throws IOException if there is a fatal error waiting for the app state.
     */
    public boolean waitForSuccess() 
            throws InterruptedException, IOException, YarnException {
        return this.waitForSuccess(0);
    }
    
    /**
     * Waits for the specified number of minutes for the app to 
     * succeed, and returns true for success.
     * Uses the Hadoop API to check status of the app.
     * 
     * @param minutes The number of minutes to wait for the success state.
     * 
     * @return boolean true if the app was successful, false if it was not or the waitFor timed out.
     * 
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     * @throws IOException if there is a fatal error waiting for the app state.
     */
    public boolean waitForSuccess(int minutes) 
            throws InterruptedException, IOException, YarnException {

        YarnApplicationState currentState; 
        
        // Give the sleep app time to complete
        for (int i = 0; i <= (minutes * 6); i++) {

            currentState = this.getYarnState();
            if (currentState.equals(YarnApplicationState.FINISHED)) {
                TestSession.logger.info("APP " + this.ID + " FINISHED");

                // once the application is marked as finished by yarn we need to check the final status
                // as reported by the application itself
                if (checkFinalStatus(FinalApplicationStatus.SUCCEEDED)) {
                    return true;
                } else {
                    TestSession.logger.error("APP " + this.ID + " finished but didn't SUCCEED.");
                    return false;
                }
            }
            else if (currentState.equals(YarnApplicationState.NEW)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL IN NEW STATE");
            }
            else if (currentState.equals(YarnApplicationState.SUBMITTED)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL IN SUBMITTED STATE");
            }
            else if (currentState.equals(YarnApplicationState.RUNNING)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL RUNNING");
            }
            else if (currentState.equals(YarnApplicationState.FAILED)) {
                TestSession.logger.info("APP " + this.ID + " FAILED");
                return false;
            }
            else if (currentState.equals(YarnApplicationState.KILLED)) {
                TestSession.logger.info("APP " + this.ID + " WAS KILLED");
                return false;
            }

            Util.sleep(10);
        }

        TestSession.logger.error("APP " + this.ID + " didn't SUCCEED within the timeout window.");

        return false;
    }

    /**
     * Waits indefinitely for the app to fail, and returns true for fail.
     * Uses the Hadoop API to check status of the app.
     * 
     * @return boolean whether the app succeeded
     * 
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     * @throws IOException if there is a fatal error waiting for the app state.
     */
    public boolean waitForFailure() 
            throws InterruptedException, IOException, YarnException {
        return this.waitForFailure(0);
    }

    /**
     * Waits for the specified number of minutes for the app to 
     * Fail, and returns true for success.
     * Uses the Hadoop API to check status of the app.
     * 
     * @param minutes The number of minutes to wait for the success state.
     * 
     * @return boolean true if the app was successful, false if it was not or the waitFor timed out.
     * 
     * @throws InterruptedException if there is a failure sleeping the current Thread. 
     * @throws IOException if there is a fatal error waiting for the app state.
     */
    public boolean waitForFailure(int minutes) 
            throws InterruptedException, IOException, YarnException {

        YarnApplicationState currentState; 
        
        // Give the sleep app time to complete
        for (int i = 0; i <= (minutes * 6); i++) {

            currentState = this.getYarnState();
            if (currentState.equals(YarnApplicationState.FINISHED)) {
                TestSession.logger.info("APP " + this.ID + " FINISHED");

                // once the application is marked as finished by yarn we need to check the final status
                // as reported by the application itself
                if (checkFinalStatus(FinalApplicationStatus.FAILED)) {
                    return true;
                } else {
                    TestSession.logger.error("APP " + this.ID + " finished but didn't SUCCEED.");
                    return false;
                }
            }
            else if (currentState.equals(YarnApplicationState.NEW)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL IN NEW STATE");
            }
            else if (currentState.equals(YarnApplicationState.SUBMITTED)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL IN SUBMITTED STATE");
            }
            else if (currentState.equals(YarnApplicationState.RUNNING)) {
                TestSession.logger.info("APP " + this.ID + " IS STILL RUNNING");
            }
            else if (currentState.equals(YarnApplicationState.FAILED)) {
                TestSession.logger.info("APP " + this.ID + " FAILED");
                return true;
            }
            else if (currentState.equals(YarnApplicationState.KILLED)) {
                TestSession.logger.info("APP " + this.ID + " WAS KILLED");
                return false;
            }

            Util.sleep(10);
        }

        TestSession.logger.error("APP " + this.ID + " didn't SUCCEED within the timeout window.");

        return false;
    }

    public boolean checkFinalStatus(FinalApplicationStatus expectedStatus) 
            throws InterruptedException, IOException, YarnException {

        FinalApplicationStatus status = this.getYarnAppFinalStatus();
        
        if (status.equals(expectedStatus)) {
            TestSession.logger.info("APP " + this.ID + " final status is: " + expectedStatus);
            return true;
        } else {
            TestSession.logger.info("APP " + this.ID + " final status doesn't match, expected: " + expectedStatus +  " was: " + status);
        }
        return false;
    }
    

    /**
     * Gets the YarnClient from the Hadoop API.
     * 
     * @return YarnClient a Hadoop API YarnClient.
     * 
     * @throws IOException if there is a problem getting the Hadoop YarnClient.
     */
    public YarnClientImpl getHadoopAPIYarnClient() throws IOException {
        return TestSession.cluster.getYarnClient();
    }
    
    
    /**
     * Blocking call that waits until the current app is running, failed, or killed, before
     * proceeding.
     * 
     * @throws InterruptedException if there is a problem sleeping the current Thread.
     * @throws IOException if there is a fatal error getting the app state.
     */
    public void blockUntilRunning() 
            throws InterruptedException, IOException, YarnException {
        TestSession.logger.info("Blocking until the app is running, failed, or killed.");
        
        do {
            Util.sleep(1);
        }
        while (this.getYarnState() != YarnApplicationState.RUNNING 
                && this.getYarnState() != YarnApplicationState.FAILED 
                && this.getYarnState() != YarnApplicationState.KILLED);
    }
    
}

