
package hadooptest.storm;

import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpMethod;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;
import hadooptest.cluster.storm.ModifiableStormCluster;
import static org.junit.Assume.assumeTrue;
import hadooptest.cluster.storm.StormDaemon;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.generated.SettableBlobMeta;

@Category(SerialTests.class)
public class TestStormDistCacheApiLocal extends TestStormDistCacheApi {

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
            mc.setConf("client_blobstore_class", "backtype.storm.blobstore.NimbusBlobStore");
            mc.setConf("nimbus_blobstore_class", "backtype.storm.blobstore.LocalFsBlobStore");
            mc.setConf("supervisor_blobstore_class", "backtype.storm.blobstore.NimbusBlobStore");
            mc.restartCluster();
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
