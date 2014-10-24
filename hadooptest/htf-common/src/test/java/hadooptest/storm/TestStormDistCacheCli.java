package hadooptest.storm;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

@Category(SerialTests.class)
public class TestStormDistCacheCli extends TestSessionStorm {

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }


  @Test(timeout=600000)
  public void testDistCacheCli() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobACLs = "u:hadoopqa@DEV.YGRID.YAHOO.COM:rwa";
    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey,
            "-f", fileName,
            "-a", blobACLs}, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    String[] listReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "list", blobKey}, true);
    assertTrue( "Could not list the blob", listReturnValue[0].equals("0"));

    String[] catReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "cat", blobKey, "-f", "/tmp/cat-"+blobKey}, true);
    assertTrue( "Could not cat the blob", catReturnValue[0].equals("0"));

    String[] setACLReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", "-s", blobACLs + ",o::r-a", blobKey}, true);
    assertTrue( "Could not set-acl the blob", setACLReturnValue[0].equals("0"));

    String[] updateReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "update", blobKey, "-f",
            fileName}, true);
    assertTrue( "Could not update the blob", updateReturnValue[0].equals("0"));

    String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));
  }


}
