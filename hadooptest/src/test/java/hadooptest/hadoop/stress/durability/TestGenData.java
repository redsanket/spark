package hadooptest.hadoop.stress.durability;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;

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
 * ./scripts/run_hadooptest -c theoden -m -n -w `pwd` -t TestGenData -DNUM_DATA_FILES=1000000
 * ./scripts/run_hadooptest -c theoden -m -n -w `pwd` -t TestGenData -DNUM_DATA_FILES=1000000 -DDATA_FILE_SIZE=1048576
 * 
 * For creating 0 byte files: 100,000 files in ~1 min, 1,000,000 in ~6 min
 * For creating 1 MB files: 10,000 files in ~2 min
 * 
 */
public class TestGenData extends TestSession {
    
    private static DFS hdfs;

    class GenDataThread extends Thread {
        int index;
        String jobRecDir;
        int batchSize;
        int partitionSize;
        DFS hdfs;
        FileSystem fs;
        
        String dataSizeStr = System.getProperty("DATA_FILE_SIZE", "1048576");
        byte[] dataBuf = 
                new byte[Integer.parseInt(System.getProperty("DATA_FILE_SIZE", "1048576"))];
                
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
            String jobRecPartDir = this.jobRecDir + "/" + paddedPartIndex;
            String jobRecExt = ".data";
            long time = System.currentTimeMillis();
            String jobNamePrefix = "file_" + time + "_";
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
                    Path path = new Path(jobStr + jobRecExt);
                    FSDataOutputStream fss = fs.create(path);
                    // FSDataOutputStream fss = fs.create(path, false, 100);
                    fss.write(dataBuf);
                    fss.close();
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
        int numDataFiles = 0;
        try {
            numDataFiles = 
                    Integer.parseInt(System.getProperty("NUM_DATA_FILES", "1000")); 
        } catch (NumberFormatException e) {
            System.err.println("Argument must be an integer!!!");
            System.exit(1);
        }        
        
        int partitionSize = 1000;
        int numPartition =  numDataFiles/partitionSize;
        int partialBatchProcessSize= numDataFiles%partitionSize;
        if (partialBatchProcessSize > 0) {
            numPartition++;
        }
        
        String dataRootDir = System.getProperty("DATA_ROOT_DIR", "test-data"); 

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        String dateStr = sdf.format(date);
        String dataRecDir = dataRootDir + "/" + dateStr;

        TestSession.logger.info(
                "--> Generate " + numDataFiles + " data records in " +
                numPartition + " paritions of size " + partitionSize +
                " in " + dataRecDir + ":");

        // Spawn a thread for each partition of job records
        GenDataThread[] threads = new GenDataThread[numPartition];
        int batchSize = partitionSize;

        /*
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
        */
        
        // Create n job records per partition per thread 
        try {        
            for(int index = 0; index < numPartition ; index++) {
                if ((index+1 == numPartition) && (partialBatchProcessSize > 0)) {
                    batchSize = partialBatchProcessSize;
                }
                threads[index] = new GenDataThread(index, dataRecDir, batchSize,
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
                "--> Generated " + numDataFiles + " job records in " +
                numPartition + " paritions of size " + partitionSize +
                " in " + dataRecDir + ":");
    }
        
}
