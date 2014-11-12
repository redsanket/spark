
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

@Category(SerialTests.class)
public class TestStormDistCacheApiLocal extends TestStormDistCacheApi {

    @BeforeClass
    public static void setup() throws Exception {
        logger.info("TestStormDistCacheApiLocal setup");
        cluster.setDrpcAclForFunction("blobstore");
        cluster.setDrpcAclForFunction("permissions");
    }
}
