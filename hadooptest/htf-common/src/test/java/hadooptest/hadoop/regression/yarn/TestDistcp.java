package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.dfs.DFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCp;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.net.URI;

/**
 * Test Case for dynamic DistCp
 */
public class TestDistcp extends TestSession {
    private Path listFile;
    private Path target;
    private Configuration conf = TestSession.cluster.getConf();

    @Test
    public void testDistcp() throws Exception {
        FsShell fsShell = null;
        String testDir = null;
        try {
            FileSystem fs = TestSession.cluster.getFS();
            fsShell = TestSession.cluster.getFsShell();
            DFS dfs = new DFS();
            testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/distcpTest/";

            //Make the test Directory for placing the source listing and output files
            if (!fs.exists(new Path(testDir))) {
                fsShell.run(new String[]{"-mkdir", testDir});
            }
            Path file1 = new Path(testDir.concat("/file1.txt"));
            Path file2 = new Path(testDir.concat("/file2.txt"));

            Path srcList1 = new Path(testDir.concat("/srcList1.txt"));
            Path target1 = new Path(testDir.concat("/outputs"));
            Path srcList2 = new Path(testDir.concat("/srcList2.txt"));
            Path target2 = new Path(testDir.concat("/outputs2"));

            //Make the output directories
            if (!fs.exists(target1)) {
                fsShell.run(new String[]{"-mkdir", target1.toString()});
            }
            if (!fs.exists(target2)) {
                fsShell.run(new String[]{"-mkdir", target2.toString()});
            }
            //Write dummy values in the files being copied
            FSDataOutputStream os1 = fs.create(file1);
            byte[] fileData = new byte[] {65, 66, 67, 68, 69, 70, 71, 72, 73, 74};
            os1.write(fileData);
            os1.close();
            os1 = fs.create(file2);
            os1.write(fileData);
            os1.close();

            //create the source listing file contents for the first job
            os1 = fs.create(srcList1);
            os1.writeBytes(file1.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString());
            os1.writeBytes("\n");
            os1.writeBytes(file2.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString());
            os1.close();
            setParams(srcList1.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()), target1.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()), conf);
            //create the source listing file contents for the second job
            os1 = fs.create(srcList2);
            os1.writeBytes(file1.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString());
            os1.writeBytes("\n");
            os1.writeBytes(file2.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString());
            os1.close();
            FileStatus firstStatusBefore = fs.getFileStatus(target1);
            FileStatus secondStatusBefore = fs.getFileStatus(target2);
            Job firstDistcpJob = runDistCpJob();
            setParams(srcList2.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()), target2.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()), conf);
            Job secondDistcpJob = runDistCpJob();
            FileStatus firstStatusAfter = fs.getFileStatus(target1);
            FileStatus secondStatusAfter = fs.getFileStatus(target2);

            URI uriTarget = target1.toUri();
            FileChecksum fileChecksumDst1 = fs.getFileChecksum(new Path(uriTarget + "/file1.txt"));
            FileChecksum fileChecksumSrc1 = fs.getFileChecksum(file1);
            FileChecksum fileChecksumDst2 = fs.getFileChecksum(new Path(uriTarget + "/file2.txt"));
            FileChecksum fileChecksumSrc2 = fs.getFileChecksum(file2);

            assertTrue("Copy Failed! for " + firstDistcpJob.getJobID(), (firstStatusAfter.getModificationTime()
                - firstStatusBefore.getModificationTime() != 0) && fileChecksumDst1.equals(fileChecksumSrc1)
                && fileChecksumDst2.equals(fileChecksumSrc2));

            uriTarget = target2.toUri();
            fileChecksumDst1 = fs.getFileChecksum(new Path(uriTarget + "/file1.txt"));
            fileChecksumDst2 = fs.getFileChecksum(new Path(uriTarget + "/file2.txt"));

            assertTrue("Copy Failed! for "+ secondDistcpJob.getJobID(), (secondStatusAfter.getModificationTime()
              - secondStatusBefore.getModificationTime() != 0) && fileChecksumDst1.equals(fileChecksumSrc1)
                && fileChecksumDst2.equals(fileChecksumSrc2));
        }
        catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
        } finally {
            //Delete the test directory
            fsShell.run(new String[]{"-rm","-r", testDir});
        }
    }
    private void setParams(Path srclist, Path dest, Configuration config) {
        this.listFile = srclist;
        this.target = dest;
        this.conf = config;
    }

    public Job runDistCpJob() {
        DistCpOptions options = new DistCpOptions(listFile, target);
        options.setCopyStrategy("dynamic");
        options.setOverwrite(true);
        Job distCpJob = null;
        try {
          DistCp d = new DistCp(conf, options);
          distCpJob = d.execute();
        } catch (Exception e) {
          e.printStackTrace();
        }
        return distCpJob;
    }
}
