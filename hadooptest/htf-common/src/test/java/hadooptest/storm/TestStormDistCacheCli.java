package hadooptest.storm;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;

@Category(SerialTests.class)
public class TestStormDistCacheCli extends TestSessionStorm {

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }

  public void testCreateAccessDelete(String blobKey, String blobACLs) throws Exception {
    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = null;
    if (blobACLs == null) {
        returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
                "create", blobKey,
                "-f", fileName }, true);
    } else {
        returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
                "create", blobKey,
                "-f", fileName, "-a", blobACLs}, true);
    }
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    String[] listReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "list", blobKey}, true);
    assertTrue( "Could not list the blob", listReturnValue[0].equals("0"));

    String[] catReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "cat", blobKey, "-f", "/tmp/cat-"+blobKey}, true);
    assertTrue( "Could not cat the blob", catReturnValue[0].equals("0"));

    String[] setACLReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", "-s", blobACLs == null ? "u:bogus:r-a,o::rwa" : blobACLs + ",o::r-a", blobKey}, true);
    assertTrue( "Could not set-acl the blob", setACLReturnValue[0].equals("0"));

    String[] resetACLReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", "-s", blobACLs == null ? "o::rwa" : blobACLs + ",o::rwa", blobKey}, true);
    assertTrue( "Could not reset-acl the blob", resetACLReturnValue[0].equals("0"));

    String[] updateReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "update", blobKey, "-f",
            fileName}, true);
    assertTrue( "Could not update the blob", updateReturnValue[0].equals("0"));

    String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));
  }

  @Test(timeout=600000)
  public void testDistCacheCliNoUserWrite() throws Exception {
    // Attempt to turn off my own permissions on a blob in a variety of ways.
    // storm blobstore create test-empty-acls.txt -f ~/samples.sh -a ""  -c "java.security.auth.login.config=/jaas/gw-jaas.conf"
    //
    // test-empty-acls.txt 1415298525000 ("u:mapredqa:rwa" "u:kpatil:rwa")
    //
    // storm blobstore create test-bl-acls.txt -f ~/samples.sh  -c "java.security.auth.login.config=/jaas/gw-jaas.conf"
    //
    // test-bl-acls.txt 1415298612000 ("u:mapredqa:rwa" "u:kpatil:rwa")
    //
    // storm blobstore create test-bad-admin-acl-st.txt -a u:kpatil:r--,o::r -f ~/sample_curl.txt  -c "java.security.auth.login.config=/jaas/gw-jaas.conf"
    // test-bad-admin-acl-st.txt 1415299176000 ("u:kpatil:rwa" "o::r--" "u:mapredqa:rwa")
    //
    // Before: empty-acls.txt 1415150326000 ("u:kpatil:rwa" "u:mapredqa:rwa")
    // storm blobstore set-acl empty-acls.txt -s "u:kpatil:rw-"  -c "java.security.auth.login.config=/jaas/gw-jaas.conf"
    // After: empty-acls.txt 1415150326000 ("u:kpatil:rwa" "u:mapredqa:rwa")
    //
    // Before: test-bad-admin-acl-st.txt 1415299176000 ("u:kpatil:rwa" "o::r--" "u:mapredqa:rwa")
    // storm blobstore set-acl test-bad-admin-acl-st.txt -c "java.security.auth.login.config=/jaas/gw-jaas.conf"| tail -1
    // After: test-bad-admin-acl-st.txt 1415299176000 ("u:mapredqa:rwa" "u:kpatil:--a")
    //

    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String blobKey = UUID.randomUUID().toString();
    String[] returnValue = null;

    // Create it with empty string permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName, "-a", "" }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:rwa");

    // Now delete it.
    String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));

    // Create blob without permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:rwa");

    // Now delete it.
    deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));

    // Create blob with bad permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName, "-a", "u:hadoopqa:r--,o::r" }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));
    // storm blobstore set-acl empty-acls.txt -s "u:kpatil:rw-"  -c "java.security.auth.login.config=/jaas/gw-jaas.conf"

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:rwa");
    findAclInFile(blobKey, "o::r--");

    // modify blob with bad permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey, "-s", "u:hadoopqa:rw-" }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:rwa");

    // modify blob with no permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Switch to superuser
    kinit(conf.getProperty("BLOB_SUPERUSER_KEYTAB"), conf.getProperty("BLOB_SUPERUSER_PRINCIPAL") );

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:--a");

    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey, "-s", "u:hadoopqa:rwa" }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:hadoopqa:rwa");

    // switch back
    kinit();

    // Now delete it.
    deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));
  }

  public ArrayList<String> getAclsForFile(String blobKey) throws Exception {
    ArrayList<String> returnValue = new ArrayList<String>();
    String[] listReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "list", blobKey}, true);
    assertTrue( "Could not list the blob", listReturnValue[0].equals("0"));
    logger.info(" blobstor list returned " + listReturnValue[1]);
    Pattern p = Pattern.compile("\"(\\w*:\\w*:[rwa-]*)\"");
    Matcher regexMatcher = p.matcher(listReturnValue[1]);
        
    while (regexMatcher.find()) {
        String thisAcl = regexMatcher.group(1);
        logger.info(" Found acl = " + thisAcl);
        returnValue.add(thisAcl);
    }
    
    return returnValue;
  }

  Boolean findAclInFile(String blobKey, String aclToMatch) throws Exception {
    Boolean returnValue = false;

    for (String acl : getAclsForFile(blobKey)) {
        logger.info("    trying to match " + aclToMatch + " with " + acl );
        if ( acl.equals(aclToMatch)) {
            returnValue = true;
        }
    }
    
    return returnValue;
  }

  @Test(timeout=600000)
  public void testDistCacheCli() throws Exception {
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", "u:hadoopqa:rwa");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", "u:hadoopqa:r");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", null);
  }
}
