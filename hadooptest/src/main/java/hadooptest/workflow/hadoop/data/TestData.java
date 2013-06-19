/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.data;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.workflow.hadoop.job.RandomWriterJob;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class for handling the common test data generation, status, etc.
 * 
 * E.g.
 * . Benchmarks Scan: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR randomwriter ${YARN_OPTIONS} scanInputDir
 * . Benchmarks Sort: $HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar $HADOOP_MAPRED_EXAMPLES_JAR randomwriter ${YARN_OPTIONS} sortInputDir
 */
public class TestData {

    String dataDir = null;
    
    /**
     * Class constructor.
     */
    public TestData() throws Exception {
        this.dataDir = this.getDefaultDataDir();
    }

    public TestData(String dataDir) {
        this.dataDir = dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getDataDir() {
        return this.dataDir;
    }

    public String getDefaultDataDir() throws Exception {
        DFS dfs = new DFS();
        String dataDir = dfs.getBaseUrl() + "/user/" +
                    System.getProperty("user.name") + "/randomwriter";
        return dataDir;
    }

    /*
     *  Run a randomwriter job to generate Random Byte Data. Produce 60 GB data.
     *      1.1G    /grid/0/tmp/randomwriter/part-m-00000
     *      1.1G    /grid/0/tmp/randomwriter/part-m-00001
     *      1.1G    /grid/0/tmp/randomwriter/part-m-00002
     *      ...
     *      1.1G    /grid/0/tmp/randomwriter/part-m-00059
     *      
     *      bash-3.2$ hadoop fs -du -s -h hdfs://gsbl90269.blue.ygrid.yahoo.com/user/hadoopqa/randomwriter/
     *      60.2g  hdfs://gsbl90269.blue.ygrid.yahoo.com/user/hadoopqa/randomwriter
     */
    public void create() throws Exception {
        RandomWriterJob rwJob = new RandomWriterJob();
        rwJob.setOutputDir(this.dataDir);
        rwJob.start();
        rwJob.waitForID(600);
        boolean isSuccessful = rwJob.waitForSuccess(20);
        assertTrue("Unable to run randomwriter job: cmd=" +
                StringUtils.join(rwJob.getCommand(), " "), isSuccessful);    
    }

    // Setup random byte data
    public void createIfEmpty() throws Exception {
        if (!this.exists()) {
            create();            
        }        
    }    
    
    public boolean exists() throws Exception {
        DFS dfs = new DFS();
        FileSystem fs = TestSession.cluster.getFS();
        if (fs.exists(new Path(this.dataDir))) {
            TestSession.logger.info("Test data directory '" + this.dataDir +
                    "' already exists.");
            dfs.fsls(this.dataDir, new String[] {"-d"});
            dfs.fsdu(this.dataDir, new String[] {"-s", "-h"});
            return true;
        }
        else {
            return false;
        }        
    }
}
