package hadooptest.hadoop.regression;

import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.ClusterState;
import hadooptest.cluster.hadoop.HadoopCluster.HADOOP_EXEC_MODE;
import hadooptest.cluster.hadoop.HadoopCluster.HADOOP_JOB_TYPE;
import hadooptest.workflow.hadoop.job.JobClient;
import hadooptest.workflow.hadoop.job.JobState;
import hadooptest.workflow.hadoop.job.SleepJob;
import hadooptest.workflow.hadoop.job.WordCountAPIJob;
import hadooptest.workflow.hadoop.job.WordCountJob;
import hadooptest.workflow.hadoop.job.TeraGenJob;
import hadooptest.workflow.hadoop.job.TeraSortJob;
import hadooptest.workflow.hadoop.job.DFSIOJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;


/*
 * Generate continuous Hadoop job load. This can be used for testing Hadoop 
 * Rolling Upgrade, or other types of stress or load testing.  
 * It takes one or more of the following parameters: 
 * job types, batch size, job submission interval, runtime duration, 
 * termination file, and capacity threshold.
 * 
 */
@Category(SerialTests.class)
public class TestGenerateJobLoad extends TestSession {
    
    int jobIndex = 0;
    static float threshold = 1.0f;
    static String terminationFilename = null;
    static String jenkinsParentId = "";
    static int batchSize = 6; 
    static int interval = 60;
    static int maxDurationMin = 60;
    static int maxWaitMin = 20;
    static boolean waitForJobId = false;
    static boolean killPendingJobsToExit = false;
    static boolean multiUsersMode = false;
    String dataRootDir = "/tmp/genjobload." + 
            new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
    static int dfsioPercentage = 100; 

	@BeforeClass
	public static void setupTestClassConfig() throws Exception {
		TestSession.cluster.setupSingleQueueCapacity();		
		threshold = Float.parseFloat(
		        System.getProperty("CAPACITY_THRESHOLD", "1.0f"));
		jenkinsParentId = 
		        System.getProperty("ID", "");
		terminationFilename = 
		        System.getProperty("TERMINATION_FILE", "/tmp/test-ru.done");
	    batchSize =
	            Integer.parseInt(System.getProperty("JOB_BATCH_SIZE", "6"));
	    interval =
	            Integer.parseInt(System.getProperty("RUNTIME_INTERVAL_SEC", "60"));
	    maxDurationMin = 
	            Integer.parseInt(System.getProperty("RUNTIME_DURATION_MIN", "60"));
        maxWaitMin = 
                Integer.parseInt(System.getProperty("MAX_WAIT_MIN", "60"));
        waitForJobId = 
                Boolean.parseBoolean(System.getProperty("WAIT_FOR_JOB_ID", "false"));
        killPendingJobsToExit = 
                Boolean.parseBoolean(System.getProperty("KILL_PENDING_JOBS", "false"));
        multiUsersMode = 
                Boolean.parseBoolean(System.getProperty("MULTI_USERS_MODE", "false"));
        dfsioPercentage =
                Integer.parseInt(System.getProperty("DFSIO_PERCENT", "100"));
        
        TestSession.logger.info("threshold='" + threshold);
        TestSession.logger.info("terminationFilename='" + terminationFilename + "'");
        TestSession.logger.info("batchSize='" + batchSize + "'");
        TestSession.logger.info("interval='" + interval + "'");
        TestSession.logger.info("maxDurationMin='" + maxDurationMin + "'");
        TestSession.logger.info("maxWaitMin='" + maxWaitMin + "'");
        TestSession.logger.info("waitForJobId='" + waitForJobId + "'");
        TestSession.logger.info("killPendingJobsToExit='" + killPendingJobsToExit + "'");
        TestSession.logger.info("multiUsersMode='" + multiUsersMode + "'");
        TestSession.logger.info("dfsioPercent='" + dfsioPercentage + "'");
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
	
    /*
     * After each test, fetch the job task reports.
     */
    @After
    public void logTaskReportSummary() 
            throws InterruptedException, IOException {

        // Do Nothing For GDM
        if ((conf.getProperty("GDM_ONLY") != null) && 
            (conf.getProperty("GDM_ONLY").equalsIgnoreCase("true"))) {
            return;
        }
        
        JobClient jobClient = TestSession.cluster.getJobClient();
        JobStatus[] jobs = 
                jobClient.getJobs(TestSession.testStartTime);

        JobState state;
        int numNonSuccess=0;
        for (JobStatus job : jobs) {            
            state = JobState.getState(
                    jobClient.getJob(job.getJobID()).getJobState());
            if (!state.equals(JobState.SUCCEEDED)) {
                    numNonSuccess++;
            }
        }        
        StringBuffer buildSummary = new StringBuffer();
        buildSummary.append("Total jobs passed=");
        buildSummary.append((jobs.length - numNonSuccess));
        buildSummary.append("/");
        buildSummary.append(jobs.length);         
        if ((jenkinsParentId != null) && 
                (jenkinsParentId.trim().length() > 0)) {
            buildSummary.append(":ID=" + jenkinsParentId);
        }

        TestSession.logger.info(buildSummary);

        /*
        if (category.equals(ParallelMethodTests.class)) {
            TestSession.logger.debug(
                    "logTaskReportSummary currently does not support " +
                    "parallel method tests.");
            return;
        }
        */
        
        if (Boolean.parseBoolean(
                conf.getProperty("LOG_TASK_REPORT")) == false) {
            return;
        }
        
        TestSession.logger.info("--------- @After: TestSession: logTaskResportSummary ----------------------------");

        // Log the tasks report summary for jobs that ran as part of this test 
        int numAcceptableNonCompleteMapTasks = 20;
        int numAcceptableNonCompleteReduceTasks = 20;
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary(
                        TestSession.TASKS_REPORT_LOG, 
                        TestSession.testStartTime, HTF_TEST.METHOD),
                        numAcceptableNonCompleteMapTasks,
                        numAcceptableNonCompleteReduceTasks);        
    }


	
	/* 
	 * Initialize the Job Types to run.
	 */
	private HADOOP_JOB_TYPE[] getJobTypes() {
	    String DEFAULT_JOB_TYPES = "SLEEP, WORDCOUNT";
	    String jobTypesStr = System.getProperty("JOB_TYPES", DEFAULT_JOB_TYPES);
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
        return jobTypes;
    }

    /* 
     * Setup the test directories
     */
    private void setupTestDir()  throws Exception {
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
    }
    
    /*
     * Generate job load by submitting jobs continuously until either:
     * 1) set time limit is reached or 
     * 2) file used to indicate termination is found
     *
     * Jobs submissions are dependent on the following criteria: 
     * job types, batch size, interval, duration, termination file, and
     * capacity threshold.
     */
    @Test
    public void submitJobsContinuously() throws Exception {
        
        HADOOP_JOB_TYPE[] jobTypes = this.getJobTypes();
        int batchSize = TestGenerateJobLoad.batchSize;
        int interval = TestGenerateJobLoad.interval;
        int maxDurationMin = TestGenerateJobLoad.maxDurationMin;
        String fileName = TestGenerateJobLoad.terminationFilename;
        
        TestSession.logger.info("Submit jobs until time limit '" +
            maxDurationMin + "' minutes is reached or file '" + fileName +
            "' is found.");

        // Setup the required test directories
        this.setupTestDir();        

        long maxDurationMs = maxDurationMin*60*1000; 
        long startTime = System.currentTimeMillis();
        long elapsedTime; 
        int elapsedTimeMin;
        File terminationFile = new File(fileName);            

        /* While elapsed time is less than the max duration, and 
         * the terminate flag file does not exist 
         */
        do {
            TestSession.logger.info(
                    "================================================================================");
            TestSession.logger.info("Submit " + batchSize + " jobs every " +
                    interval + " seconds.");
            TestSession.logger.info(
                    "================================================================================");
            try {
                // TODO: CLI is fully working. API will submit only serially.
                // Get the current capacity, if greater than threshold...
                submitBatchJobs(
                        jobTypes, batchSize, HADOOP_EXEC_MODE.CLI);
            } catch (Exception e) {
                TestSession.logger.error("Job failed with exception: " +
                        e.toString());
            }
            
            elapsedTime = System.currentTimeMillis() - startTime;
            elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
            TestSession.logger.info(
                    "================================================================================");
            TestSession.logger.info("Elapsed time is: '" + elapsedTimeMin + 
                    "' minutes.");            
            if (elapsedTime >= maxDurationMs) {
                TestSession.logger.info("Max duration has been reached.");
            } else if (terminationFile.exists()) {
                TestSession.logger.info("Found file signaling termination.");
            } else {
                TestSession.logger.info("Sleep for " + interval + " seconds.");
                Thread.sleep(interval*1000);
            }
            TestSession.logger.info(
                    "================================================================================");
        } while ((elapsedTime < maxDurationMs) && (!terminationFile.exists()));
        
        String username="hadoopqa";
        TestSession.cluster.setSecurityAPI("keytab-"+username, "user-"+username);

        if (killPendingJobsToExit) {
            TestSession.logger.info(
                    "Kill any jobs still in PREP state to expedite the exit process");
            JobClient jobClient = TestSession.cluster.getJobClient();        
            JobID[] jobIDs = 
                    jobClient.getJobIDs(jobClient.getJobs(TestSession.testStartTime));
            JobState currentState = JobState.UNKNOWN;
            String jobIdStr;
            for (JobID jobID : jobIDs) {            
                jobIdStr = jobID.toString();
                currentState = JobState.getState(
                        jobClient.getJob(jobID).getJobState());
                if (currentState.equals(JobState.PREP)) {
                    TestSession.logger.info("Kill JOB '" + jobIdStr + 
                            "' STILL IN PREP STATE SO WE CAN EXIT");
                    jobClient.getJob(jobID).killJob();
                }
            }
        }
        
        // Wait for all the jobs to succeed.
        try {
            if (!this.waitForSuccessForAllJobs(maxWaitMin)) {
                fail("Wait for all jobs to succeed failed");                
            }
        } catch (Exception e) {
            TestSession.logger.error("Wait for all jobs to succeed failed with exception: " +
                    e.toString());
            // TODO print parent info before we exit on exception...   
            logTaskReportSummary();
            fail("Wait for all jobs to succeed failed with exception");
        }
        
        // Cleanup
        this.cleanUp(terminationFile);
    }

    /* 
     * Wait for all jobs to succeed.
     * TODO: checking it twice since there could be jobs queued up.
     */
    private boolean waitForSuccessForAllJobs(int maxWaitMin) throws Exception {
        JobClient jobClient = TestSession.cluster.getJobClient();        
        int count = 1;
        int maxCount = 3;
        boolean isSuccess=true;
        boolean allSuccess=true;
        JobStatus[] jobsStatus;
        do {
            TestSession.logger.info(
                    "----------- Wait for all jobs to succeed #" + count + 
                    "/" + maxCount + ": for " + maxWaitMin + 
                    " minute(s) ---------------");
            jobsStatus = jobClient.getJobs(TestSession.testStartTime);
            jobClient.displayJobList(jobsStatus);
            isSuccess = TestSession.cluster.getJobClient().waitForSuccess(
                    jobClient.getJobIDs(jobsStatus), maxWaitMin);
            if (!isSuccess) {
                allSuccess=false;
            }
            count++;

            // For jobs such as DFSIO and TeraSort which has two back to back                                                                                                                                                             
            // jobs ([teragen, terasort]; [dfsio write, dfsio read], wait for                                                                                                                                                             
            // a few seconds for the second job process to start                                                                                                                                                                          
            TestSession.logger.info("Sleep for 10 seconds for any subsequent background job to start.");
            Thread.sleep(10000);

        } while( count <= maxCount );   
        return allSuccess;
    }

    /* 
     * Initialize the Job Types to run.
     */
    private void cleanUp(File terminationFile) throws Exception {
        TestSession.logger.info("----------- Clean Up: ---------------");

        // Remove the termination indicator file if it exists
        if (terminationFile.exists()) {
            TestSession.logger.info("Delete termination file '" + 
                    terminationFile.toString() + "'");
            terminationFile.delete();            
        }
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
     * Run a batch of sleep jobs in the background
     * 
     * @param int Number of jobs
     * @param mode CLI or API mode
     * 
     */
    public void submitBatchJobs(
            HADOOP_JOB_TYPE[] jobTypes, int numJobs, HADOOP_EXEC_MODE mode)
            throws Exception {
        if (mode.equals(HADOOP_EXEC_MODE.CLI)) {
            submitBatchJobsCLI(jobTypes, numJobs);
        } else {
            submitBatchJobsAPI(jobTypes, numJobs);            
        }
    }

    /*
     * *************************************************************************
     * API BATCH JOBS 
     * TODO: NOT FULLY IMPLEMENTED
     * *************************************************************************
     */
    
    /*
     * Run a batch of jobs in the background in API mode
     * TODO: NOT FULLY IMPLEMENTED
     */
    public void submitBatchJobsAPI(HADOOP_JOB_TYPE[] jobTypes, int numJobs)
            throws Exception {
        TestSession.logger.info("---> Run '" + numJobs + "' sleep jobs:");
        int index = 0;
        int rc;
        String[] args = { "-m", "10", "-r", "10", "-mt", "10000", "-rt", "10000"};
        Configuration conf = TestSession.cluster.getConf();
        String username="hadoopqa";
        while (index < numJobs) {
            if (multiUsersMode) {
                username = "hadoop" + ((index%20)+1);
            }
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
     * *************************************************************************
     * CLI BATCH JOBS 
     * *************************************************************************
     */
    
    /*
     * Run a batch of jobs in the background in CLI mode
     */
    public void submitBatchJobsCLI(HADOOP_JOB_TYPE[] jobTypes, int numJobs)
            throws Exception {
        TestSession.logger.info("---> Submit '" + numJobs + "' jobs:");
        int index = 0;
        int numJobTypes = jobTypes.length;
        HADOOP_JOB_TYPE jobType;
        float capacityThreshold = TestGenerateJobLoad.threshold;
        Map<String, QueueInfo> queueInfo =
                TestSession.cluster.getQueueInfo();
        int delay = 5;
        while (index < numJobs) {

            // Check capacity before submitting each job
            float currentCapacity =
                    queueInfo.get("default").getCurrentCapacity();
            if (currentCapacity >= capacityThreshold) {
                TestSession.logger.info(
                        "Current capacity '" + currentCapacity + 
                        "' is >= the threshold value of '" +
                        capacityThreshold + "': skip a turn");
                break;
            } else {
                TestSession.logger.info(
                        "Current capacity '" + currentCapacity + 
                        "' is < the threshold value of '" +
                        capacityThreshold + "': proceed");   
                
                // Check for existing jobs that are in prep state
                JobClient jobClient = TestSession.cluster.getJobClient();        
                JobStatus[] jobsStatus = jobClient.getJobs(JobState.PREP);
                int numPrepJobs = jobsStatus.length;
                if (numPrepJobs > 0) {                    
                    jobClient.displayJobList(jobsStatus);
                    TestSession.logger.info(
                            "There are currently still '" + numPrepJobs + 
                            "' jobs in the PREP state: skip a turn");
                    break;
                } else {
                    TestSession.logger.info(
                            "There are currently no jobs in the PREP state: " +
                            "proceed");                    
                }
            }
            
            jobType = jobTypes[index%numJobTypes];
            jobIndex++;
            String username="hadoopqa";
            if (multiUsersMode) {
                username = "hadoop" + ((index%20)+1);
            }
            TestSession.logger.info("---------- Submit job #" + jobIndex +
                    " type=" + jobType + " user=" + username + 
                    " (batch job " + (index+1) + "/" + numJobs +
                    ") ---------------");
            switch (jobType) {
                case WORDCOUNT: 
                    this.submitWordCountJobCLI(username);                
                    break;
                case TERAGEN: 
                    this.submitTeraGenJobCLI(username);                
                    break;
                case TERASORT:
                    this.submitTeraSortJobCLI(username);                
                    break;
                case DFSIO:
                    this.submitDFSIOJobCLI(username);                
                    break;
                default:
                    this.submitSleepJobCLI(username);
                    break;
            }
            index++;
            // Sleep for delay seconds if job is not waiting for job id. 
            // This is so we don't overload the queue unnecessarily.
            if (!waitForJobId) {
                TestSession.logger.info("Sleep for " + delay + " seconds.");
                Thread.sleep(interval*1000);
            }
        }
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
        job.setJobInitSetID(waitForJobId);
        job.start();                        
    }

    /* 
     * Submit a DFSIO job
     */
    public void submitDFSIOJobCLI(String username) throws Exception {
        
        // DFSIO Write Job
        TestSession.logger.info("Run DFSIO write test:");
        DFSIOJob writeJob = new DFSIOJob();
        String timestamp = writeJob.getTimestamp();
        writeJob.setTestDir(timestamp);
        writeJob.setupTestDir();
        writeJob.setUser(username);
        writeJob.setJobInitSetID(waitForJobId);
        writeJob.setOperation("write");
        writeJob.setPercentage(dfsioPercentage);
        writeJob.start();
        
        // Wait for Job ID for 30 seconds from the job thread before continue. 
        // Otherwise TestGenerateJobLoad may terminates prematurely before the 
        // job has a chance to start in its thread.
        if (waitForJobId) {
            if (!writeJob.waitForID(maxWaitMin)) {
                fail("ERROR: Unable to get job id for DFSIO write test after " +
                        maxWaitMin + "minutes!!!");                
            }
        }
        
        // DFSIO Read Job
        TestSession.logger.info("Run DFSIO read test:");
        DFSIOJob readJob = new DFSIOJob();
        readJob.setTestDir(timestamp);
        readJob.setWriteJobID(writeJob.getID());
        readJob.setUser(username);
        readJob.setJobInitSetID(waitForJobId);
        readJob.setOperation("read");
        readJob.setPercentage(dfsioPercentage);
        readJob.start();            
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
        job.setJobInitSetID(waitForJobId);
        job.start();
    }    
    
    /* 
     * Submit a terasort job
     */
    public void submitTeraSortJobCLI(String username) throws Exception {
        
        // submitTeraGenJobCLI(String username);
        // but have to wait until the job is done ....
        
        String teraGenOutputDir = null; 
        String teraSortOutputDir = null; 
        
        String date = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        teraGenOutputDir = dataRootDir + "/teragen." + username + "." + date; 
        TestSession.logger.info("Target HDFS Directory (teragen) is: " +
                teraGenOutputDir);
        teraSortOutputDir = dataRootDir + "/teragsort." + username + "." + date; 
        TestSession.logger.info("Target HDFS Directory (terasort) is: " +
                teraGenOutputDir);

        Path outputDirPath = new Path(teraGenOutputDir);
        FileSystem fs = TestSession.cluster.getFS();            
        if (fs.exists(outputDirPath)) {
            fs.delete(outputDirPath, true);
        }
        
        TestSession.cluster.setSecurityAPI("keytab-"+username, "user-"+username);
        // TODO: This caused the job to fail because teragen expects the directory                                                                                                                                                        
        // not to exist. However this fails silently. Need to make some changes                                                                                                                                                           
        // to output the error to help with future debugging.   
        // TestSession.cluster.getFS().mkdirs(new Path(outputDir));
        
        // hdfs://gsbl90741.blue.ygrid.yahoo.com/user/hadoopqa/terasort/teraInputDir 
        // hdfs://gsbl90741.blue.ygrid.yahoo.com/user/hadoopqa/terasort/teraOutputDir 

        /*
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/terasort";
        
        // Generate sort data
        String dataDir = testDir + "/teraInputDir";
        createData(dataDir);
        
        // Sort the data
        String sortInputDir = dataDir;
        String sortOutputDir = testDir + "/teraOutputDir";
        runSortJob(sortInputDir, sortOutputDir);

        // Validate the data
        String sortReport = testDir + "/teraReport";
        validateSortResults(sortOutputDir, sortReport);
        */
        
        
        TeraSortJob job = new TeraSortJob();
        job.setTeraGenOutputPath(teraGenOutputDir);
        job.setTeraSortOutputPath(teraSortOutputDir);
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
        job.setJobInitSetID(waitForJobId);
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
                
        localDir = TestSession.conf.getProperty("WORKSPACE") + "/htf-common/resources//hadoop/data/pipes/";
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
        job.setJobInitSetID(waitForJobId);
        job.start();
    }
}
