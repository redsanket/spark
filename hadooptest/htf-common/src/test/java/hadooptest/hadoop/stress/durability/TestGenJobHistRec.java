package hadooptest.hadoop.stress.durability;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Example: 
 * ./scripts/run_hadooptest -c theoden -m -n -w `pwd` -t TestGenJobHistRec -DJOB_RECORD=1000000
 * 
 * Takes about 8 minues to generate 1,000,000 history records.
 */
public class TestGenJobHistRec extends TestSession {
    
    private static DFS hdfs;

    class GenDataThread extends Thread {
        int index;
        String jobRecDir;
        int batchSize;
        int partitionSize;
        DFS hdfs;
        FileSystem fs;

        // Instantiate a thread for creating a subset of job records
        GenDataThread(int index, String jobRecDir, int batchSize,
                int partitionSize, DFS hdfs) {
            super("Gen Data Thread: #" + index);
            this.index=index;
            this.jobRecDir = jobRecDir;
            this.batchSize = batchSize;
            this.partitionSize = partitionSize;
            this.hdfs = hdfs;            
            try {
                fs = FileSystem.get(TestSession.getCluster().getConf());
            } catch (Exception e) {
                TestSession.logger.error("Exception: " + e.toString());
                e.printStackTrace();
            }
        }

        // Thread run method for creating job records as mapredqa.
        public void run() {
            int startIndex=this.index*partitionSize;
            String paddedPartIndex = String.format("%06d", index);
            String user = "hadoopqa";
            String jobType = "Sleep";
            String jobState = "SUCCEEDED";
            String jobRecPartDir = this.jobRecDir + "/" + paddedPartIndex;
            String jobRecExt1 = "_conf.xml";
            long time = System.currentTimeMillis();
            String jobNamePrefix = "job_" + time + "_";
            String jobRecExt2 = "-" + time + "-" + user + "-" + jobType +
                    "+job-" + time + "-1-1-" + jobState + "-default.jhist";
            String jobStrPrefix = jobRecPartDir + "/" + jobNamePrefix;
            TestSession.logger.info("Generate " + batchSize +
                    " job records in " + jobRecPartDir + " for " + 
                    jobNamePrefix + "[" + String.format("%06d", startIndex) +
                    "-" + String.format("%06d", (startIndex+batchSize-1)) +
                    "]");
            try {
                // Create the directory
                FsShell fsShell = TestSession.cluster.getFsShell();
                String[] cmd = {"-mkdir", "-p", jobRecPartDir};
                TestSession.logger.trace("Create directory: hadoop fs " +
                        StringUtils.join(cmd, " "));
                fsShell.run(cmd);

                // Create n job records
                for(int i = startIndex; i < (startIndex+batchSize); i++) {
                    String jobStr = jobStrPrefix + String.format("%06d", i);
                    Path path1 = new Path(jobStr + jobRecExt1);
                    Path path2 = new Path(jobStr + jobRecExt2);                    
                    FSDataOutputStream fss1 = fs.create(path1);
                    fss1.close();
                    FSDataOutputStream fss2 = fs.create(path2);
                    fss2.close();
                }
            } catch (Exception e) {
                TestSession.logger.info("Caught exception in child thread #" +
                        this.index + ": " + e.toString());
                e.printStackTrace();
            }
            TestSession.logger.trace("Exiting thread.");
        }
    }


    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();        
        hdfs = new DFS();
        // String hdfsBaseURL = hdfs.getBaseUrl();
    }

    @Test
    public void testGenData() throws Exception {
        int numRecords = 0;
        try {
            numRecords = 
                    Integer.parseInt(System.getProperty("JOB_RECORD", "1000")); 
        } catch (NumberFormatException e) {
            System.err.println("Argument must be an integer!!!");
            System.exit(1);
        }

        int partitionSize = 1000;
        int numPartition =  numRecords/partitionSize;
        int partialBatchProcessSize= numRecords%partitionSize;
        if (partialBatchProcessSize > 0) {
            numPartition++;
        }
        
        String jobRecRootDir = "/mapred/history/done";
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        String dateStr = sdf.format(date);
        String jobRecDir = jobRecRootDir + "/" + dateStr;

        TestSession.logger.info(
                "--> Generate " + numRecords + " job records in " +
                numPartition + " paritions of size " + partitionSize +
                " in " + jobRecDir + ":");

        // Spawn a thread for each partition of job records
        GenDataThread[] threads = new GenDataThread[numPartition];
        int batchSize = partitionSize;

        // Run as mapredqa
        try {
            // Initialize API security for the FullyDistributedCluster type only.
            if (cluster instanceof FullyDistributedCluster) {
                cluster.setSecurityAPI("keytab-mapredqa", "user-mapredqa");
            }
        }
        catch (IOException ioe) {
            logger.error("Failed to set the Hadoop API security.", ioe);
            ioe.printStackTrace();
        }
        
        // Create n job records per partition per thread 
        try {        
            for(int index = 0; index < numPartition ; index++) {
                if ((index+1 == numPartition) && (partialBatchProcessSize > 0)) {
                    batchSize = partialBatchProcessSize;
                }
                threads[index] = new GenDataThread(index, jobRecDir, batchSize,
                        partitionSize, hdfs);
                threads[index].start();
                // Thread.sleep(100);
            }
            
            for(int index = 0; index < numPartition ; index++) {
                threads[index].join();                
            }
        } catch (InterruptedException e) {
            TestSession.logger.error("Main thread interrupted.");
            e.printStackTrace();
        }
        TestSession.logger.info(
                "--> Generated " + numRecords + " job records in " +
                numPartition + " paritions of size " + partitionSize +
                " in " + jobRecDir + ":");
    }
        
}