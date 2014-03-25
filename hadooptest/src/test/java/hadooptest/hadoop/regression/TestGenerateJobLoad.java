package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import hadooptest.cluster.ClusterState;
import hadooptest.cluster.hadoop.HadoopCluster.HADOOP_EXEC_MODE;
import hadooptest.cluster.hadoop.HadoopCluster.HADOOP_JOB_TYPE;
import hadooptest.workflow.hadoop.job.JobClient;
import hadooptest.workflow.hadoop.job.SleepJob;
import hadooptest.workflow.hadoop.job.WordCountAPIJob;
import hadooptest.workflow.hadoop.job.WordCountJob;
import hadooptest.workflow.hadoop.job.TeraGenJob;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/*
 * Generate the Hadoop job load (e.g. for testing Hadoop Rolling Upgrade) 
 * based on one or more of the following parameters: 
 * job types, batch size, job submission interval, runtime duration, 
 * termination file, and capacity threshold.
 * 
 */
@Category(SerialTests.class)
public class TestGenerateJobLoad extends TestSession {
    
    int jobIndex = 0;
    String dataRootDir = "/tmp/genjobload." + 
            new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());

	@BeforeClass
	public static void setupTestClassConfig() throws Exception {
		TestSession.cluster.setupSingleQueueCapacity();
	}
	
	@Before
	public void checkClusterStatus() throws Exception {
	    TestSession.logger.info("Check cluster state is up:");
	    try {
	        ClusterState clusterState = cluster.getState();
	        if (clusterState == ClusterState.UP) {
	            logger.info("Cluster is fully up and ready to go.");                
	        } else {                
	            logger.warn("Cluster is not fully up: cluster state='" +
	                    clusterState.toString() + "'.'");
	        }
	    }
	    catch (Exception e) {
	        logger.error("Failed to get the cluster state.", e);
	    }
	}
	
    /* Generate traffic by submitting jobs
     */
    @Test
    public void testTraffic() throws Exception {                
        String jobTypesStr = System.getProperty("JOB_TYPES",
                "SLEEP, WORDCOUNT");
        List<String> jobTypesStrList =
                Arrays.asList(jobTypesStr.split("\\s*,\\s*"));
        ArrayList<HADOOP_JOB_TYPE> jobTypesList =
                new ArrayList<HADOOP_JOB_TYPE>();
        for (String typeStr : jobTypesStrList) {
            TestSession.logger.debug("job type = " + typeStr);
            jobTypesList.add(HADOOP_JOB_TYPE.valueOf(typeStr));
        }
        HADOOP_JOB_TYPE[] jobTypes = 
                jobTypesList.toArray(new HADOOP_JOB_TYPE[jobTypesList.size()]);
        
        submitJobsUntilTimeoutOrFileFound(
                jobTypes,
                Integer.parseInt(System.getProperty("JOB_BATCH_SIZE", "6")),
                Integer.parseInt(System.getProperty("RUNTIME_INTERVAL_SEC", "60")),
                Integer.parseInt(System.getProperty("RUNTIME_DURATION_MIN", "60")),
                System.getProperty("TERMINATION_FILE", "/tmp/test-ru.done"),
                Float.parseFloat(System.getProperty("CAPACITY_THRESHOLD", "1.0f")));
    }
    
    /* Submit jobs until timeout or file is found by
     *
     * @param job types
     * @param batch size
     * @param interval
     * @param duration
     * @param termination file
     * @param capacity threshold
     */
    public void submitJobsUntilTimeoutOrFileFound(
            HADOOP_JOB_TYPE[] jobTypes,
            int batchSize, 
            int interval, 
            int maxDurationMin,
            String fileName,
            float capacityThreshold) throws Exception {
        
        TestSession.logger.info("Submit jobs until time limit '" +
            maxDurationMin + "' minutes is reached or file '" + fileName +
            "' is found.");

        long maxDurationMs = maxDurationMin*60*1000; 
        long startTime = System.currentTimeMillis();
        long elapsedTime = System.currentTimeMillis() - startTime;
        int elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
        File terminationFile = new File(fileName);

        TestSession.logger.info("Test data root dir is: "+ dataRootDir);
        Path dataRootDirPath = new Path(dataRootDir);
        FileSystem fs = TestSession.cluster.getFS();
        if (!fs.exists(dataRootDirPath)) {
	    TestSession.logger.info("Create test data root dir: "+ dataRootDir);
	    // TODO: mkdirs with FsPermission did not work. 
	    // might be a config issue.
            fs.mkdirs(dataRootDirPath,new FsPermission("777"));
            String[] cmd = {
		TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"),
		"--config",
		TestSession.cluster.getConf().getHadoopConfDir(),
		"fs", "-chmod", "777", dataRootDir,
	    };
	    TestSession.exec.runProcBuilderSecurity(cmd, "hdfsqa");
        }
        
        /* While elapsed time is less than the max duration, and 
         * the terminate flag file does not exist 
         */
        while ((elapsedTime < maxDurationMs) && (!terminationFile.exists())) {
            TestSession.logger.info("Submit " + batchSize + " jobs every " +
                    interval + " seconds.");
            // TODO: CLI is fully working. API will submit only serially.
            // Get the current capacity, if greater than threshold...
            
            Map<String, QueueInfo> queueInfo =
                    TestSession.cluster.getQueueInfo();
            float currentCapacity =
                    queueInfo.get("default").getCurrentCapacity();
            if (currentCapacity < capacityThreshold) {
                TestSession.logger.info(
                        "Current capacity '" + currentCapacity + 
                        "' is < the threshold value of '" +
                        capacityThreshold + "'");
                try {
                    submitJobs(jobTypes, batchSize, HADOOP_EXEC_MODE.CLI);
                } catch (Exception e) {
                    TestSession.logger.error("Job failed with exception: " +
                            e.toString());
                }
            } else {
                TestSession.logger.info(
                        "Current capacity '" + currentCapacity + 
                        "' is > the threshold value of '" +
                        capacityThreshold + "': wait a turn");
            }            
            
            elapsedTime = System.currentTimeMillis() - startTime;
            elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
            TestSession.logger.info("Elapsed time is: '" + elapsedTimeMin + 
                    "' minutes.");            
            if (elapsedTime >= maxDurationMs) {
                TestSession.logger.info("Max duration has been reached.");
                break;
            }
            if (terminationFile.exists()){
                TestSession.logger.info("Found file signaling termination.");
                break;
            }

            if ((elapsedTime < maxDurationMs) && (!terminationFile.exists())) {
                TestSession.logger.info("Sleep for " + interval + " seconds.");
                Thread.sleep(interval*1000);
            }
        }
        
        // Wait for all the jobs to succeed.
        int durationMin = 15;
        TestSession.logger.info("----------- Wait for all jobs to succeed: " +
                "for " + durationMin + " ---------------");

        JobClient jobClient = TestSession.cluster.getJobClient();
        TestSession.cluster.getJobClient().waitForSuccess(
                jobClient.getJobIDs(jobClient.getJobs(TestSession.testStartTime)),
                durationMin);
        
        // Check again after the first set of jobs all succeeded
        TestSession.cluster.getJobClient().waitForSuccess(
                jobClient.getJobIDs(jobClient.getJobs(TestSession.testStartTime)),
                durationMin);
        
        // Remove the termination indicator file if it exists
        if (terminationFile.exists()) {
            TestSession.logger.info("Delete termination file '" + 
                    terminationFile.toString() + "'");
            terminationFile.delete();            
        }
        
        // Cleanup                                                                                                                                                      
        TestSession.logger.info("----------- Clean Up: ---------------");
	// TODO: was not able to delete the directory via the API call due to 
	// permission issue. 
        // fs.delete(dataRootDirPath, true);
	String[] cmd = {
	    TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"),
	    "--config",
	    TestSession.cluster.getConf().getHadoopConfDir(),
	    "fs", "-rm", "-r", "-skipTrash", dataRootDir,
	};
	TestSession.exec.runProcBuilderSecurity(cmd, "hdfsqa");
    }
        
    /*
     * Run a batch of sleep jobs in the background in API mode
     */
    public void submitSleepJobCLI(String username) {
        TestSession.logger.info("Submit Sleep Job for user " + username + ":");
        SleepJob job = new SleepJob();
        job.setNumMappers(
                Integer.parseInt(System.getProperty("SLEEP_JOB_NUM_MAPPER", "100")));
        job.setNumReducers(
                Integer.parseInt(System.getProperty("SLEEP_JOB_NUM_REDUCER", "100")));
        job.setMapDuration(
                Integer.parseInt(System.getProperty("SLEEP_JOB_MAP_SLEEP_TIME", "30000")));
        job.setReduceDuration(
                Integer.parseInt(System.getProperty("SLEEP_JOB_RED_SLEEP_TIME", "30000")));
        job.setUser(username);
        job.setJobInitSetID(false);
        job.start();                        
    }

    /* 
     * Submit a teragen job
     */
    public void submitTeraGenJobCLI(String username) throws Exception {
        String outputDir = null; 
        
        String date = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());        
        outputDir = dataRootDir + "/teragen." + username + "." + date; 
        TestSession.logger.info("Target HDFS Directory is: "+ outputDir);

        Path outputDirPath = new Path(outputDir);
        FileSystem fs = TestSession.cluster.getFS();            
        if (fs.exists(outputDirPath)) {
            fs.delete(outputDirPath, true);
        }
        
        TestSession.cluster.setSecurityAPI("keytab-"+username, "user-"+username);
        // TODO: This caused the job to fail because teragen expects the directory                                                                                                                                                        
        // not to exist. However this fails silently. Need to make some changes                                                                                                                                                           
        // to output the error to help with future debugging.   
        // TestSession.cluster.getFS().mkdirs(new Path(outputDir));
        
        TeraGenJob job = new TeraGenJob();        
        job.setOutputPath(outputDir);
        job.setUser(username);
        job.setNumMapTasks(
                Long.parseLong(
                        System.getProperty("TERAGEN_NUM_MAP_TASKS", "8000")));
        /*
         * 10,000,000,000 rows of 100-byte per row = 1,000,000,000,000 TB
         */
        job.setNumDataRows(
                Long.parseLong(
                        System.getProperty("TERAGEN_NUM_DATA_ROWS", "10000000000")));
        job.setJobInitSetID(false);
        job.start();
    }    
    
    
    /* 
     * Submit a wordcount job
     */
    public void submitWordCountJobCLI(String username) throws Exception {
        String localDir = null; 
        String localFile = "input.txt";
        String outputDir = null; 
        String outputFile = "wc_output";

        TestSession.cluster.getFS();    
                
        localDir = TestSession.conf.getProperty("WORKSPACE") + "/resources//hadoop/data/pipes/";
        TestSession.logger.info("Target local Directory is: "+ localDir);
        TestSession.logger.info("Target local File Name is: " + localFile);
        
        outputDir = "/user/" + username + "/"; 
        TestSession.logger.info("Target HDFS Directory is: "+ outputDir);
        TestSession.logger.info("Target HDFS File Name is: " + outputFile);
        
        TestSession.cluster.setSecurityAPI("keytab-"+username, "user-"+username);
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localDir + localFile), new Path(outputDir + localFile));
        // Delete the file, if it exists in the same directory
        TestSession.cluster.getFS().delete(new Path(outputDir+outputFile), true);
        
        WordCountJob job = new WordCountJob();        
        job.setInputFile(outputDir + localFile);
        job.setOutputPath(outputDir + outputFile);
        job.setUser(username);
        job.setJobInitSetID(false);
        job.start();
    }    
    
    
    /* 
     * Submit a wordcount job via API call (WIP)
     */
    public void submitWordCountJobAPI() throws Exception {
        String[] args = { };
        /*
        String[] args = {inpath.toString(), outputDir + outputFile+"/"+Integer.toString(run_times), Integer.toString(jobNum), Integer.toString(qNum)};
        for (int i = 0; i < qNum ; i++) {
            args = append(args, qName[i]);
        }                   
        for (int i = 0; i < args.length; i++){
            TestSession.logger.info("args["+Integer.toString(i) + "]: " + args[i]);
        }
        */
        
        int rc;
        TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
        rc = ToolRunner.run(
                TestSession.cluster.getConf(), 
                new WordCountAPIJob(), 
                args);
        if (rc != 0) {
            TestSession.logger.error("Job failed!!!");
        }
    }

    /*
     * Run a batch of sleep jobs in the background
     * 
     * @param int Number of jobs
     * @param mode CLI or API mode
     * 
     */
    public void submitJobs(
            HADOOP_JOB_TYPE[] jobTypes, int numJobs, HADOOP_EXEC_MODE mode)
            throws Exception {
        if (mode.equals(HADOOP_EXEC_MODE.CLI)) {
            submitJobsCLI(jobTypes, numJobs);
        } else {
            submitJobsAPI(jobTypes, numJobs);            
        }
    }

    /*
     * Run a batch of sleep jobs in the background in CLI mode
     */
    public void submitJobsCLI(HADOOP_JOB_TYPE[] jobTypes, int numJobs)
            throws Exception {
        TestSession.logger.info("---> Submit '" + numJobs + "' jobs:");
        int index = 0;
        int numJobTypes = jobTypes.length;
        HADOOP_JOB_TYPE jobType;
        while (index < numJobs) {
            jobType = jobTypes[index%numJobTypes];
            jobIndex++;
            TestSession.logger.info("---------- Submit job #" + jobIndex +
                    " " + jobType + " (batch " + (index+1) + "/" + numJobs +
                    ") ---------------");
            /*
            TestSession.logger.info("-----------Submit job #" + (index+1) +
                    " " + jobType + "---------------");
                    */
            if (jobType.equals(HADOOP_JOB_TYPE.WORDCOUNT)) {
                this.submitWordCountJobCLI("hadoop" + ((index%20)+1));                
            } else if (jobType.equals(HADOOP_JOB_TYPE.TERAGEN)) {
                this.submitTeraGenJobCLI("hadoop" + ((index%20)+1));                
            } else {
                this.submitSleepJobCLI("hadoop" + ((index%20)+1));
            }
            index++;
            // Thread.sleep(1000);
        }
    }

    /*
     * Run a batch of sleep jobs in the background in API mode
     */
    public void submitJobsAPI(HADOOP_JOB_TYPE[] jobTypes, int numJobs)
            throws Exception {
        TestSession.logger.info("---> Run '" + numJobs + "' sleep jobs:");
        int index = 0;
        int rc;
        String[] args = { "-m", "10", "-r", "10", "-mt", "10000", "-rt", "10000"};
        Configuration conf = TestSession.cluster.getConf();
        while (index < numJobs) {
            String username = "hadoop" + ((index%20)+1);
            TestSession.cluster.setSecurityAPI("keytab-"+username, "user-"+username);
            TestSession.logger.info("---------- Submit job #" + (index+1) + " --------------");
            
            // TODO: this runs serially right now.
            rc = ToolRunner.run(conf, new org.apache.hadoop.SleepJob(), args);
            if (rc != 0) {
                TestSession.logger.error("Job failed!!!");
            }
            index++;
        }
    }
    
}
