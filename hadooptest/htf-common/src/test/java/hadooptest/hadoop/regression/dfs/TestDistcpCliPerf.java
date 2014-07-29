package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.monitoring.Monitorable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import hadooptest.SerialTests;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestDistcpCliPerf extends DfsTestsBaseClass {

    static Logger logger = Logger.getLogger(TestDistcpCliPerf.class);

    private static boolean isDataCopiedAcrossConcernedClusters = false;
    private String parametrizedCluster;
    private String localHadoopVersion = "2.x";
    private String remoteHadoopVersion = "2.x";
    private static Properties crossClusterProperties;
    private static HashMap<String, String> versionStore;

    @BeforeClass
    public static void startTestSession() throws Exception {

        TestSession.start();
        TestSession.cluster.setupSingleQueueCapacity();
        crossClusterProperties = new Properties();
        try {
            crossClusterProperties.load(new FileInputStream(
                            HadooptestConstants.Location.TestProperties.CrossClusterProperties));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        versionStore = new HashMap<String, String>();
    }

    /*
     * Data Driven DISTCP tests... The tests are invoked with the following
     * parameters.
     */
    @Parameters
    public static Collection<Object[]> data() {

        if (DfsTestsBaseClass.crosscoloPerf == true) {
            return Arrays.asList(new Object[][] {
            // Clusters
            { System.getProperty("REMOTE_CLUSTER") }, });
        } else {
            return Arrays.asList(new Object[][] {
                    // Clusters
                    { System.getProperty("CLUSTER_NAME") },
                    { System.getProperty("REMOTE_CLUSTER") }, });
        }
    }

    public TestDistcpCliPerf(String cluster) {

        this.parametrizedCluster = cluster;
        this.localCluster = System.getProperty("CLUSTER_NAME");

        logger.info("Test invoked for local cluster:[" + this.localCluster
                + "] remote cluster:[" + cluster + "]");
    }

    @Before
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

        if (DfsTestsBaseClass.crosscoloPerf == false) {
            if (versionStore.containsKey(this.parametrizedCluster)) {
                // Do not make an unnecessary call to get the version, if you'
                // ve
                // already made it once.
                remoteHadoopVersion = versionStore
                        .get(this.parametrizedCluster);
            } else {
                remoteHadoopVersion = rmUtils
                        .getHadoopVersion(this.parametrizedCluster);
                remoteHadoopVersion = remoteHadoopVersion.split("\\.")[0];
                versionStore.put(this.parametrizedCluster, remoteHadoopVersion);

            }
        }
    }

    @Before
    public void ensureDataPresenceAcrossClusters() throws Exception {
        pathsChmodedSoFar = new HashMap<String, Boolean>();
        Set<String> clusters = new HashSet<String>();
        for (Object[] row : TestDistcpCliPerf.data()) {
            for (Object parameter : row) {
                clusters.add(((String) parameter).trim().toLowerCase());
            }
        }
        // For if you are running this test from a 3rd cluster
        clusters.add(this.localCluster);
        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        String filePattern;
        if (isDataCopiedAcrossConcernedClusters == false) {
            for (String aCluster : clusters) {

                TestSession.logger
                        .info("****************************ensure data present across clusters: "
                                + aCluster);

                for (String justTheFile : fileMetadataPerf.keySet()) {
                    filePattern = justTheFile + "_{1.."
                            + fileMetadataPerf.get(justTheFile) + "}";

                    GenericCliResponseBO doesFileExistResponseBO = dfsCommonCliCommands
                            .test(EMPTY_ENV_HASH_MAP,
                                    HadooptestConstants.UserNames.HDFSQA,
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
                                DATA_DIR_IN_LOCAL_FS + filePattern,
                                DATA_DIR_IN_HDFS);

                    }
                }
            }
            // Reset it for each cluster
            pathsChmodedSoFar = new HashMap<String, Boolean>();
        }
        isDataCopiedAcrossConcernedClusters = true;

    }

    // @Monitorable
    @Test
    public void testWebhdfsToHdfsPerf() throws Exception {

        // @Ignore("Only valid for cross colo distcp")
        Assume.assumeTrue(DfsTestsBaseClass.crosscoloPerf);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        for (String justTheFile : fileMetadataPerf.keySet()) {

            if ((this.localHadoopVersion.startsWith("0") && this.remoteHadoopVersion
                    .startsWith("0"))
                    || (this.localHadoopVersion.startsWith("2") && this.remoteHadoopVersion
                            .startsWith("2"))) {

                // Push
                // appendString = ".srcWebhdfs." + this.localCluster +
                // ".dstHdfs."
                // + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS;

                destinationFile = DATA_DIR_IN_HDFS + justTheFile + "/";
                dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA, "",
                        this.localCluster, destinationFile);
                dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA, "",
                        this.parametrizedCluster, destinationFile);

                // + justTheFile + appendString;
                genericCliResponse = dfsCommonCliCommands.distcp(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster, this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                                + fileMetadataPerf.get(justTheFile) + "}",
                        destinationFile, HadooptestConstants.Schema.WEBHDFS,
                        HadooptestConstants.Schema.HDFS);
                Assert.assertTrue("distcp exited with non-zero exit code",
                        genericCliResponse.process.exitValue() == 0);

                dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster, Recursive.YES, Force.YES,
                        SkipTrash.YES, destinationFile);

            }
            // Pull
            appendString = ".srcWebhdfs." + this.parametrizedCluster
                    + ".dstHdfs." + this.localCluster;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
            genericCliResponse = dfsCommonCliCommands.distcp(
                    EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    this.parametrizedCluster,
                    this.localCluster,
                    DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                            + fileMetadataPerf.get(justTheFile) + "}",
                    destinationFile, HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.HDFS);
            Assert.assertTrue("distcp exited with non-zero exit code",
                    genericCliResponse.process.exitValue() == 0);

            dfsCommonCliCommands.rm(EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS, this.localCluster,
                    Recursive.YES, Force.YES, SkipTrash.YES, destinationFile);

        }
    }

    @After
    public void logTaskResportSummary() throws Exception {
        // Override to hide the Test Session logs

        if (crosscoloPerf == true) {
            ArrayList<String> cmd = new ArrayList<String>();
            cmd.add(TestSession.conf.getProperty("WORKSPACE")
                    + "/scripts/calc_perf");
            cmd.add("-cluster=" + TestSession.cluster.getClusterName());
            cmd.add("-started_time_begin=" + TestSession.startTime);
            String[] command = cmd.toArray(new String[0]);
            String[] output = TestSession.exec.runProcBuilderSecurity(command);
            TestSession.addLoggerFileAppender(TestSession.CROSS_COLO_PERF_LOG);
            TestSession.logger.info(Arrays.toString(output));
            TestSession
                    .removeLoggerFileAppender(TestSession.CROSS_COLO_PERF_LOG);
        }

    }

}
