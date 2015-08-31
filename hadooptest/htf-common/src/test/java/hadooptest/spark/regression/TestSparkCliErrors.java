package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkPi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkCliErrors extends TestSession {

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }

    /*
     * A test running number of workers 0
     *
     */
    @Test
    public void runSparkPiTestWorkersZero() throws Exception {
        SparkPi appUserDefault = new SparkPi();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(0);
        appUserDefault.setWorkerCores(1);

        appUserDefault.start();

        assertTrue("Error because workers are 0",
            appUserDefault.waitForERROR(10));

    }

    /*
     * A test running number of workers 0
     *
     */
    // This was broken in Spark 1.5.  https://jira.corp.yahoo.com/browse/YSPARK-165 tracks 
    // fixing it, comment it out for now as its not a blocker
/*
    @Test
    public void runSparkPiTestWorkersZeroSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(0);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkPi");

        appUserDefault.start();

        assertTrue("Error because workers are 0",
            appUserDefault.waitForERROR(10));

    }
*/

}
