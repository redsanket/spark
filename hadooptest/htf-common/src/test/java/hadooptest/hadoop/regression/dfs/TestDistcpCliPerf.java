package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.monitoring.Monitorable;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
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

/*
 * Push file from Blue to Tan:
 * E.g. hadoop distcp -pbugp webhdfs://gsbl90882.blue.ygrid.yahoo.com/HTF/testdata/dfs/file_16M_{1..64} hdfs://gsta325n38.tan.ygrid.yahoo.com/HTF/testdata/dfs/file_16M/
 *
 * Pull file from Tan to Blue:
 * E.g. hadoop distcp -pbugp webhdfs://gsta325n38.tan.ygrid.yahoo.com/HTF/testdata/dfs/file_16M_{1..64} hdfs://gsbl90882.blue.ygrid.yahoo.com/HTF/testdata/dfs/file_16M/
 *
 */

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

        if ((DfsTestsBaseClass.crosscoloPerf == true) ||
            (DfsTestsBaseClass.crossclusterPerf == true)) {
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
                remoteHadoopVersion =
                        versionStore.get(this.parametrizedCluster);
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


    @Before
    public void setupProxyHost() throws Exception {
        if (DfsTestsBaseClass.crosscoloPerf) {
            String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
            if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                    !httpProxyHost.equals("default")) {

                PrintWriter writer = new PrintWriter("/tmp/webhdfs-proxy.xml", "UTF-8");
                writer.println("<?xml version=\"1.0\"?>");
                writer.println("<configuration>");
                writer.println("<property>");
                writer.println("  <name>dfs.webhdfs.proxy</name>");
                writer.println("  <value>HTTP " + httpProxyHost + ":4080</value>");
                writer.println("  <final>true</final>");
                writer.println("</property>");
                writer.println("</configuration>");
                writer.close();
                // yinst set -root /home/gs/gridre/yroot.densed HadoopConfiggeneric10node12diskblue.TODO_INCLUDE_PROXY_CONFIG='<xi:include href="webhdfs-proxy.xml"><xi:fallback></xi:fallback></xi:include>'
                String[] yinst_cmd = {
                    "/usr/local/bin/yinst",
                    "set",
                    "-root",
                    "/home/gs/gridre/yroot." + this.localCluster,
                    "HadoopConfiggeneric10node12diskblue." +
                            "TODO_INCLUDE_PROXY_CONFIG=" +
                            "'<xi:include href=" +
                            "\"/tmp/webhdfs-proxy.xml\"" +
                            "><xi:fallback></xi:fallback></xi:include>'"
                };
                String output[] = TestSession.exec.runProcBuilder(yinst_cmd);
                TestSession.logger.debug(Arrays.toString(output));
            }
        }
    }

    /*
     * Cross Colo Webhdfs to Hdfs tests from both directions: Push and Pull.
     */
    // @Monitorable
    @Test
    public void testWebhdfsToHdfsPerf() throws Exception {

        // @Ignore("Only valid for cross colo distcp")
        Assume.assumeTrue(DfsTestsBaseClass.crosscoloPerf);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        // Option args for distcp
        String optionArgs = "";

        // Option args for distcp, for remote cluster
        String optionArgsRC = "";
        /*
        String httpProxyHostRC =
                System.getProperty("HTTP_PROXY_HOST_REMOTE_CLUSTER", "");
        if (httpProxyHostRC != null && !httpProxyHostRC.isEmpty() &&
                !httpProxyHost.equals("default")) {
            optionArgsRC = "-Dhttp.proxyHost=" + httpProxyHostRC;
            optionArgsRC = optionArgsRC + " -Dhttp.proxyPort=4080";
        }
        */

        HashMap<String, String> proxyEnv = new HashMap<String, String>();
        String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
        if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
            /*
            optionArgs = "-Dhttp.proxyHost=" + httpProxyHost;
            optionArgs = optionArgs + " -Dhttp.proxyPort=4080";
            */
            String proxyStr = "-Dhttp.proxyHost=" + httpProxyHost +
                    " -Dhttp.proxyPort=4080";
            proxyEnv.put("HADOOP_OPTS", proxyStr);
        }

        /*
        String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
        if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
            optionArgs = "-Dhttp.proxyHost=" + httpProxyHost;
            optionArgs = optionArgs + " -Dhttp.proxyPort=4080";
        }
        */

        for (String justTheFile : fileMetadataPerf.keySet()) {
            /*
             * Push: Push file from blue (webhdfs) to tan (hdfs)
             * In this case, since we are going cross colo with rpc based hdfs,
             * it will not go through the ycrypt proxy but rather the
             * Jupiner h/w.
             * If we wanted it to go through the ycrypt proxy, we
             * would have to push file from blue (hdfs) to tan (webhdfs). But
             * in this case, webhdfs write is not supported.
             */
            if ((this.localHadoopVersion.startsWith("0") &&
                    this.remoteHadoopVersion.startsWith("0")) ||
                    (this.localHadoopVersion.startsWith("2") &&
                            this.remoteHadoopVersion.startsWith("2"))) {
                logger.info(
                        "Since this is cross colo with rpc based hdfs write" +
                        "it will go through the IPSec (Juniper) boxes " +
                                "intead of going through ycrypt proxy.");
                // appendString = ".srcWebhdfs." + this.localCluster +
                // ".dstHdfs." + this.parametrizedCluster;
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
                        this.localCluster,
                        this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                                + fileMetadataPerf.get(justTheFile) + "}",
                        destinationFile,
                        HadooptestConstants.Schema.WEBHDFS,
                        HadooptestConstants.Schema.HDFS,
                        optionArgs);
                Assert.assertTrue("distcp exited with non-zero exit code",
                        genericCliResponse.process.exitValue() == 0);
                dfsCommonCliCommands.rm(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster,
                        Recursive.YES,
                        Force.YES,
                        SkipTrash.YES,
                        destinationFile);
            }

            /*
             * Pull
             */
            appendString = ".srcWebhdfs." + this.parametrizedCluster
                    + ".dstHdfs." + this.localCluster;
            // destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile + "/";
            genericCliResponse = dfsCommonCliCommands.distcp(
                    EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    this.parametrizedCluster,
                    this.localCluster,
                    DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                            + fileMetadataPerf.get(justTheFile) + "}",
                    destinationFile,
                    HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.HDFS,
                    optionArgsRC);
            Assert.assertTrue("distcp exited with non-zero exit code",
                    genericCliResponse.process.exitValue() == 0);
            dfsCommonCliCommands.rm(
                    EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS,
                    this.localCluster,
                    Recursive.YES,
                    Force.YES,
                    SkipTrash.YES,
                    destinationFile);

            /*
             * Check the ycrypt proxy server log dir /home/y/logs/trafficserver
             * extended2.log that the transaction got routed through.
             * E.G.
             * PUT https://fsta773n00.tan.ygrid.yahoo.com:4443/webhdfs
             */
        }
    }

    /*
     * Cross Colo Webhdfs to Hdfs tests from both directions: Push and Pull.
     */
    // @Monitorable
    @Test
    public void testWebhdfsToHdfsPerfCC() throws Exception {

        // @Ignore("Only valid for cross colo distcp")
        Assume.assumeTrue(DfsTestsBaseClass.crossclusterPerf);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        // Option args for distcp
        String optionArgs = "";

        // Option args for distcp, for remote cluster
        String optionArgsRC = "";
        /*
        String httpProxyHostRC =
                System.getProperty("HTTP_PROXY_HOST_REMOTE_CLUSTER", "");
        if (httpProxyHostRC != null && !httpProxyHostRC.isEmpty() &&
                !httpProxyHost.equals("default")) {
            optionArgsRC = "-Dhttp.proxyHost=" + httpProxyHostRC;
            optionArgsRC = optionArgsRC + " -Dhttp.proxyPort=4080";
        }
        */

        HashMap<String, String> proxyEnv = new HashMap<String, String>();
        String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
        if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
            /*
            optionArgs = "-Dhttp.proxyHost=" + httpProxyHost;
            optionArgs = optionArgs + " -Dhttp.proxyPort=4080";
            */
            String proxyStr = "-Dhttp.proxyHost=" + httpProxyHost +
                    " -Dhttp.proxyPort=4080";
            proxyEnv.put("HADOOP_OPTS", proxyStr);
        }

        /*
        String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
        if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
            optionArgs = "-Dhttp.proxyHost=" + httpProxyHost;
            optionArgs = optionArgs + " -Dhttp.proxyPort=4080";
        }
        */

        for (String justTheFile : fileMetadataPerf.keySet()) {
            /*
             * Push: Push file from blue (webhdfs) to tan (hdfs)
             * In this case, since we are going cross colo with rpc based hdfs,
             * it will not go through the ycrypt proxy but rather the
             * Jupiner h/w.
             * If we wanted it to go through the ycrypt proxy, we
             * would have to push file from blue (hdfs) to tan (webhdfs). But
             * in this case, webhdfs write is not supported.
             */
            if ((this.localHadoopVersion.startsWith("0") &&
                    this.remoteHadoopVersion.startsWith("0")) ||
                    (this.localHadoopVersion.startsWith("2") &&
                            this.remoteHadoopVersion.startsWith("2"))) {
                logger.info(
                        "Since this is cross colo with rpc based hdfs write" +
                        "it will go through the IPSec (Juniper) boxes " +
                                "intead of going through ycrypt proxy.");
                // appendString = ".srcWebhdfs." + this.localCluster +
                // ".dstHdfs." + this.parametrizedCluster;
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
                        this.localCluster,
                        this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                                + fileMetadataPerf.get(justTheFile) + "}",
                        destinationFile,
                        HadooptestConstants.Schema.WEBHDFS,
                        HadooptestConstants.Schema.HDFS,
                        optionArgs);
                Assert.assertTrue("distcp exited with non-zero exit code",
                        genericCliResponse.process.exitValue() == 0);
                dfsCommonCliCommands.rm(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster,
                        Recursive.YES,
                        Force.YES,
                        SkipTrash.YES,
                        destinationFile);
            }

            /*
             * Pull
             */
            appendString = ".srcWebhdfs." + this.parametrizedCluster
                    + ".dstHdfs." + this.localCluster;
            // destinationFile = DATA_DIR_IN_HDFS + justTheFile + appendString;
            destinationFile = DATA_DIR_IN_HDFS + justTheFile + "/";
            genericCliResponse = dfsCommonCliCommands.distcp(
                    EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    this.parametrizedCluster,
                    this.localCluster,
                    DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                            + fileMetadataPerf.get(justTheFile) + "}",
                    destinationFile,
                    HadooptestConstants.Schema.WEBHDFS,
                    HadooptestConstants.Schema.HDFS,
                    optionArgsRC);
            Assert.assertTrue("distcp exited with non-zero exit code",
                    genericCliResponse.process.exitValue() == 0);
            dfsCommonCliCommands.rm(
                    EMPTY_ENV_HASH_MAP,
                    HadooptestConstants.UserNames.HDFSQA,
                    HadooptestConstants.Schema.WEBHDFS,
                    this.localCluster,
                    Recursive.YES,
                    Force.YES,
                    SkipTrash.YES,
                    destinationFile);

            /*
             * Check the ycrypt proxy server log dir /home/y/logs/trafficserver
             * extended2.log that the transaction got routed through.
             * E.G.
             * PUT https://fsta773n00.tan.ygrid.yahoo.com:4443/webhdfs
             */
        }
    }


    /*
     * This test will only run for HttpProxy cross cluster tests
     * (i.e. DfsTestsBaseClass is crossclusterPerf)
     */
    // @Monitorable
    // @Test
    public void testHdfsToHdfs() throws Exception {

        // @Ignore("Only valid for cross colo distcp")
        // Assume.assumeTrue(DfsTestsBaseClass.crossclusterPerf);

        DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
        GenericCliResponseBO genericCliResponse;
        String destinationFile;
        String appendString;

        // Option args for distcp
        String optionArgs = "";

        for (String justTheFile : fileMetadataPerf.keySet()) {

            if ((this.localHadoopVersion.startsWith("0") &&
                    this.remoteHadoopVersion.startsWith("0")) ||
                    (this.localHadoopVersion.startsWith("2") &&
                            this.remoteHadoopVersion.startsWith("2"))) {
                // Push
                appendString = ".srcHdfs." + this.localCluster + ".dstHdfs."
                        + this.parametrizedCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFile + "/";
                genericCliResponse = dfsCommonCliCommands.distcp(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.localCluster,
                        this.parametrizedCluster,
                        DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                                + fileMetadataPerf.get(justTheFile) + "}",
                        destinationFile,
                        HadooptestConstants.Schema.HDFS,
                        HadooptestConstants.Schema.HDFS,
                        optionArgs);
                Assert.assertTrue("distcp exited with non-zero exit code", 
                        genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.parametrizedCluster,
                        Recursive.YES,
                        Force.YES,
                        SkipTrash.YES,
                        destinationFile);

                // Pull
                appendString = ".srcHdfs." + this.parametrizedCluster
                        + ".dstHdfs." + this.localCluster;
                destinationFile = DATA_DIR_IN_HDFS + justTheFile + "/";
                genericCliResponse = dfsCommonCliCommands.distcp(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        this.parametrizedCluster,
                        this.localCluster,
                        DATA_DIR_IN_HDFS + justTheFile + "_{1.."
                                + fileMetadataPerf.get(justTheFile) + "}",
                        destinationFile,
                        HadooptestConstants.Schema.HDFS,
                        HadooptestConstants.Schema.HDFS,
                        optionArgs);
                Assert.assertTrue("distcp exited with non-zero exit code",
                        genericCliResponse.process.exitValue()==0);

                dfsCommonCliCommands.rm(
                        EMPTY_ENV_HASH_MAP,
                        HadooptestConstants.UserNames.HDFSQA,
                        HadooptestConstants.Schema.WEBHDFS,
                        this.localCluster,
                        Recursive.YES,
                        Force.YES,
                        SkipTrash.YES,
                        destinationFile);
            } else {
            logger.info("Skipping test because of possible RPC version mismatch....Local version="
                    + this.localHadoopVersion
                    + " Destination version="
                    + this.remoteHadoopVersion);
            }
        }
    }


    @After
    public void logTaskReportSummary() throws Exception  {
        // Override to hide the Test Session logs

        if ((DfsTestsBaseClass.crosscoloPerf == true) ||
            (DfsTestsBaseClass.crossclusterPerf == true)) {
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

    @After
    public void resetProxyHost() throws Exception {
        if (DfsTestsBaseClass.crosscoloPerf) {
            String httpProxyHost = System.getProperty("HTTP_PROXY_HOST", "");
            if (httpProxyHost != null && !httpProxyHost.isEmpty() &&
                !httpProxyHost.equals("default")) {
                // yinst set -root /home/gs/gridre/yroot.densed HadoopConfiggeneric10node12diskblue.TODO_INCLUDE_PROXY_CONFIG='<xi:include href="webhdfs-proxy.xml"><xi:fallback></xi:fallback></xi:include>'
                String[] yinst_cmd = {
                    "/usr/local/bin/yinst",
                    "set",
                    "-root",
                    "/home/gs/gridre/yroot." +
                            this.localCluster,
                    "HadoopConfiggeneric10node12diskblue." +
                            "TODO_INCLUDE_PROXY_CONFIG=" +
                            "'<xi:include href=" +
                            "\"webhdfs-proxy.xml\"" +
                            "><xi:fallback></xi:fallback></xi:include>'"
                };
                String output[] = TestSession.exec.runProcBuilder(yinst_cmd);
                TestSession.logger.debug(Arrays.toString(output));
            }
        }
    }
}
