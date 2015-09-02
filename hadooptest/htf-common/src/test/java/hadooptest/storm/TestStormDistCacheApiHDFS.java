
package hadooptest.storm;

import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;
import hadooptest.cluster.storm.ModifiableStormCluster;
import static org.junit.Assume.assumeTrue;
import hadooptest.cluster.storm.StormDaemon;

@Category(SerialTests.class)
public class TestStormDistCacheApiHDFS extends TestStormDistCacheApi {

    static ModifiableStormCluster mc = null;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        start();
        mc = (ModifiableStormCluster)cluster;
        cluster.setDrpcAclForFunction("blobstore");
        cluster.setDrpcAclForFunction("permissions");
        cluster.setDrpcAclForFunction("md5");
        if (mc != null) {
            mc.setConf("NIMBUS_EXTRA_CLASSPATHS", conf.getProperty("HADOOP_CLASSPATH"), StormDaemon.NIMBUS);
            mc.setConf("SUPERVISOR_EXTRA_CLASSPATHS", conf.getProperty("HADOOP_CLASSPATH"), StormDaemon.SUPERVISOR);
            mc.setConf("blobstore_dir", conf.getProperty("BLOBSTORE_DIR"));
            mc.setConf("blobstore_hdfs_keytab", conf.getProperty("HDFS_KEYTAB"));
            mc.setConf("blobstore_hdfs_principal", conf.getProperty("HDFS_PRINCIPAL"));
            mc.setConf("client_blobstore_class", "backtype.storm.blobstore.NimbusBlobStore", StormDaemon.NIMBUS);
            mc.setConf("nimbus_blobstore_class", "backtype.storm.blobstore.HdfsBlobStore", StormDaemon.NIMBUS);
            mc.setConf("supervisor_blobstore_class", "backtype.storm.blobstore.HdfsClientBlobStore", StormDaemon.SUPERVISOR);
            mc.restartCluster();
            String cmdHost = conf.getProperty("BLOB_SETUP_HDFS_HOST");
            String mkUserScript = conf.getProperty("BLOB_SETUP_HDFS_CMD");
            if ( cmdHost !=null && mkUserScript != null ) {
                exec.runProcBuilder( new String[] { "ssh", cmdHost, mkUserScript }, true );
            }
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
          mc.resetConfigsAndRestart();
        }
        stop();
    }
}
