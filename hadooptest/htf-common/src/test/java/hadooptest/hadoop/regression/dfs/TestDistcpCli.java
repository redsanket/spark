package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.monitoring.Monitorable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestDistcpCli extends DfsTestsBaseClass {

    private static boolean isDataCopiedAcrossConcernedClusters = false;
    private String parametrizedCluster;
    private String localHadoopVersion;
    private String remoteHadoopVersion;
    private static Properties crossClusterProperties;
    private static HashMap<String, String> versionStore;

    @BeforeClass
    public static void startTestSession() throws Exception {

        TestSession.start();
        crossClusterProperties = new Properties();
        try {
            crossClusterProperties.load(new FileInputStream(
                    HadooptestConstants.Location.TestProperties.CrossClusterProperties));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        versionStore = new HashMap<String, String>();

    }

    @Monitorable @Ignore("use case not supported yet") @Test public void testWebhdfsToWebhdfs_local() throws Exception { testWebhdfsToWebhdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable @Ignore("use case not supported yet") @Test public void testWebhdfsToWebhdfs_remote() throws Exception { testWebhdfsToWebhdfs(System.getProperty("REMOTE_CLUSTER")); }

    @Monitorable @Test public void testWebhdfsToHdfs_local() throws Exception { testWebhdfsToHdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable @Test public void testWebhdfsToHdfs_remote() throws Exception { testWebhdfsToHdfs(System.getProperty("REMOTE_CLUSTER")); }

    @Monitorable @Ignore("hftp not supported in 2.x") @Test public void testHftpToWebhdfs_local() throws Exception { testHftpToWebhdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable @Ignore("hftp not supported in 2.x") @Test public void testHftpToWebhdfs_remote() throws Exception { testHftpToWebhdfs(System.getProperty("REMOTE_CLUSTER")); }

    @Monitorable @Ignore("hftp not supported in 2.x") @Test public void testHftpToHdfs_local() throws Exception { testHftpToHdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable @Ignore("hftp not supported in 2.x") @Test public void testHftpToHdfs_remote() throws Exception { testHftpToHdfs(System.getProperty("REMOTE_CLUSTER")); }

    @Monitorable  @Ignore("use case not supported yet") @Test public void testHdfsToWebhdfs_local() throws Exception { testHdfsToWebhdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable  @Ignore("use case not supported yet") @Test public void testHdfsToWebhdfs_remote() throws Exception { testHdfsToWebhdfs(System.getProperty("REMOTE_CLUSTER")); }

    @Monitorable @Test public void testHdfsToHdfs_local() throws Exception { testHdfsToHdfs(System.getProperty("CLUSTER_NAME")); }
    @Monitorable @Test public void testHdfsToHdfs_remote() throws Exception { testHdfsToHdfs(System.getProperty("REMOTE_CLUSTER")); }

    private static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Clusters
                { System.getProperty("CLUSTER_NAME") },
                { System.getProperty("REMOTE_CLUSTER") }, });
    }

    private void testSetup(String cluster) throws Exception {

        this.parametrizedCluster = cluster;
        this.localCluster = System.getProperty("CLUSTER_NAME");

        getVersions();
        ensureDataPresenceAcrossClusters();

        logger.info("Test invoked for local cluster:[" + this.localCluster
                + "] remote cluster:[" + cluster + "]");
    }

    private void testWebhdfsToWebhdfs(String cluster) throws Exception {
        testSetup(cluster);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        String destinationFile;
        GenericCliResponseBO genericCliResponse;

        for (String justTheFile : fileMetadata.keySet()) {
            // Push
            String appendStringOnCopiedFile = ".srcWebhdfs."
                    + this.localCluster + ".dstWebhdfs."
                    + this.parametrizedCluster;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile
                    + appendStringOnCopiedFile;
            genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA, this.localCluster,
                    this.parametrizedCluster, DATA_DIR_IN_HDFS + justTheFile,
                    destinationFile, HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.WEBHDFS);
            Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);


            dfsCommonCliCommands
            .rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS,
                    this.parametrizedCluster, Recursive.YES, Force.YES,
                    SkipTrash.YES,
                    destinationFile);

            // Pull
            appendStringOnCopiedFile = ".srcWebhdfs."
                    + this.parametrizedCluster + ".dstWebhdfs."
                    + this.localCluster;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile
                    + appendStringOnCopiedFile;
            genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    this.parametrizedCluster, this.localCluster,
                    DATA_DIR_IN_HDFS + justTheFile, destinationFile,
                    HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.WEBHDFS);
            Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

            dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS, this.localCluster,
                    Recursive.YES, Force.YES,
                    SkipTrash.YES, destinationFile);

        }
    }

    private void testWebhdfsToHdfs(String cluster) throws Exception {
        testSetup(cluster);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        for (String justTheFile : fileMetadata.keySet()) {
            if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion.startsWith("0")) ||
                    (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion.startsWith("2"))) {

                // Push
                appendString = ".srcWebhdfs." + this.localCluster + ".dstHdfs."
                        + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster, this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile, destinationFile,
                        HadooptestConstants.Schema.WEBHDFS,
                        HadooptestConstants.Schema.HDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster, Recursive.YES, Force.YES,
                        SkipTrash.YES,
                        destinationFile);

            }
            // Pull
            appendString = ".srcWebhdfs." + this.parametrizedCluster
                    + ".dstHdfs." + this.localCluster;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
            genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    this.parametrizedCluster, this.localCluster,
                    DATA_DIR_IN_HDFS + justTheFile, destinationFile,
                    HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.HDFS);
            Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

            dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS, this.localCluster,
                    Recursive.YES, Force.YES,
                    SkipTrash.YES, destinationFile);

        }
    }

    private void testHftpToWebhdfs(String cluster) throws Exception {
        testSetup(cluster);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        for (String justTheFile : fileMetadata.keySet()) {
            // Push
            if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
                    .startsWith("0"))
                    || (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
                            .startsWith("2"))) {
                appendString = ".srcHftp." + this.localCluster + ".dstWebHdfs."
                        + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster, this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile, destinationFile,
                        HadooptestConstants.Schema.HFTP,
                        HadooptestConstants.Schema.WEBHDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster, Recursive.YES, Force.YES,
                        SkipTrash.YES,
                        destinationFile);

            }
            // No Pull, since HFTP is read-only
        }

    }

    private void testHftpToHdfs(String cluster) throws Exception {
        testSetup(cluster);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
                .startsWith("0"))
                || (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
                        .startsWith("2"))) {

            for (String justTheFileName : fileMetadata.keySet()) {
                // Push
                appendString = ".srcHftp." + this.localCluster + ".dstHdfs."
                        + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFileName
                        + appendString;
                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster, this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFileName, destinationFile,
                        HadooptestConstants.Schema.HFTP,
                        HadooptestConstants.Schema.HDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster, Recursive.YES, Force.YES,
                        SkipTrash.YES,
                        destinationFile);

                // No Pull, since HFTP is readonly
            }
        } else {
            logger.info("Skipping test because of possible RPC version mismatch....Local version="
                    + this.localHadoopVersion
                    + " Destination version="
                    + this.remoteHadoopVersion);
        }

    }

    private void testHdfsToWebhdfs(String cluster) throws Exception {
        testSetup(cluster);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        for (String justTheFileName : fileMetadata.keySet()) {
            // Push
            appendString = ".srcHdfs." + this.localCluster + ".dstWebhdfs."
                    + this.parametrizedCluster;
            destinationFile = DATA_DIR_IN_HDFS + justTheFileName + appendString;
            genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA, this.localCluster,
                    this.parametrizedCluster, DATA_DIR_IN_HDFS
                    + justTheFileName, destinationFile,
                    HadooptestConstants.Schema.HDFS,
                    HadooptestConstants.Schema.WEBHDFS);
            Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

            dfsCommonCliCommands
            .rm(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS,
                    this.parametrizedCluster, Recursive.YES, Force.YES,
                    SkipTrash.YES,
                    destinationFile);

            // Pull
            if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
                    .startsWith("0"))
                    || (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
                            .startsWith("2"))) {
                appendString = ".srcHdfs." + this.parametrizedCluster
                        + ".dstWebhdfs." + this.localCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFileName
                        + appendString;

                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.parametrizedCluster, this.localCluster,
                        DATA_DIR_IN_HDFS + justTheFileName, destinationFile,
                        HadooptestConstants.Schema.HDFS,
                        HadooptestConstants.Schema.WEBHDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS, this.localCluster,
                        Recursive.YES, Force.YES,
                        SkipTrash.YES, destinationFile);

            }
        }

    }

    private void testHdfsToHdfs(String cluster) throws Exception {
        testSetup(cluster);

        // @Ignore("Not valid for cross colo distcp")
        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
                .startsWith("0"))
                || (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
                        .startsWith("2"))) {

            for (String justTheFileName : fileMetadata.keySet()) {
                // Push
                appendString = ".srcHdfs." + this.localCluster + ".dstHdfs."
                        + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFileName
                        + appendString;
                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster, this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFileName, destinationFile,
                        HadooptestConstants.Schema.HDFS,
                        HadooptestConstants.Schema.HDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster, Recursive.YES, Force.YES,
                        SkipTrash.YES,
                        destinationFile);

                // Pull
                appendString = ".srcHdfs." + this.parametrizedCluster
                        + ".dstHdfs." + this.localCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFileName
                        + appendString;

                genericCliResponse = dfsCommonCliCommands.distcp(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.parametrizedCluster, this.localCluster,
                        DATA_DIR_IN_HDFS + justTheFileName, destinationFile,
                        HadooptestConstants.Schema.HDFS,
                        HadooptestConstants.Schema.HDFS);
                Assert.assertTrue("distcp exited with non-zero exit code", genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS, this.localCluster,
                        Recursive.YES, Force.YES,
                        SkipTrash.YES, destinationFile);

            }

        } else {
            logger.info("Skipping test because of possible RPC version mismatch....Local version="
                    + this.localHadoopVersion
                    + " Destination version="
                    + this.remoteHadoopVersion);
        }
    }

    public void getVersions() {
        ResourceManagerHttpUtils rmUtils = new ResourceManagerHttpUtils();
        if (versionStore.containsKey(this.localCluster)) {
            // Do not make an unnecessary call to get the version, if you've
            // already made it once.
            localHadoopVersion = versionStore.get(this.localCluster);
        } else {
            localHadoopVersion = rmUtils.getHadoopVersion(this.localCluster);
            localHadoopVersion = localHadoopVersion.split("\\.")[0];
            versionStore.put(this.localCluster, localHadoopVersion);
        }

        if (versionStore.containsKey(this.parametrizedCluster)) {
            // Do not make an unnecessary call to get the version, if you've
            // already made it once.
            remoteHadoopVersion = versionStore.get(this.parametrizedCluster);
        } else {
            remoteHadoopVersion = rmUtils
                    .getHadoopVersion(this.parametrizedCluster);
            remoteHadoopVersion = remoteHadoopVersion.split("\\.")[0];
            versionStore.put(this.parametrizedCluster, remoteHadoopVersion);

        }
    }

    public void ensureDataPresenceAcrossClusters() throws Exception {
        pathsChmodedSoFar = new HashMap<String, Boolean>();
        Set<String> clusters = new HashSet<String>();
        for (Object[] row : TestDistcpCli.data()) {
            for (Object parameter : row) {
                clusters.add(((String) parameter).trim().toLowerCase());
            }
        }
        // For if you are running this test from a 3rd cluster
        clusters.add(this.localCluster);
        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        if (isDataCopiedAcrossConcernedClusters == false) {
            for (String aCluster : clusters) {
                for (String justTheFile : fileMetadata.keySet()) {
                    GenericCliResponseBO doesFileExistResponseBO = dfsCommonCliCommands
                            .test(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA,
                                    HadooptestConstants.Schema.WEBHDFS,
                                    aCluster, DATA_DIR_IN_HDFS + justTheFile,
                                    DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
                    if (doesFileExistResponseBO.process.exitValue() != 0) {
                        dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA,
                                HadooptestConstants.Schema.WEBHDFS, aCluster,
                                DATA_DIR_IN_HDFS);
                        doChmodRecursively(aCluster, DATA_DIR_IN_HDFS);
                        dfsCommonCliCommands.copyFromLocal(EMPTY_ENV_HASH_MAP,
                                HadooptestConstants.UserNames.HDFSQA,
                                HadooptestConstants.Schema.WEBHDFS, aCluster,
                                DATA_DIR_IN_LOCAL_FS + justTheFile,
                                DATA_DIR_IN_HDFS + justTheFile);
                    }
                }
            }
            //Reset it for each cluster
            pathsChmodedSoFar = new HashMap<String, Boolean>();
        }
        isDataCopiedAcrossConcernedClusters = true;

    }

}
