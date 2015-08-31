package hadooptest.storm;

import hadooptest.Util;
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

  @BeforeClass
  public static void setup() throws Exception {
      start();
      cluster.setDrpcAclForFunction("blobstore");
      cluster.setDrpcAclForFunction("permissions");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }

  public void testCreateAccessDelete(String blobKey, String blobACLs, String expectedListAcls) throws Exception {
    String fileName = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormDistCacheCli/input.txt";
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

    String[] noOtherACLReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", "-s", blobACLs == null ? "o::rwa" : blobACLs, blobKey}, true);
    assertTrue( "Could not reset-acl the blob with no other permissions", noOtherACLReturnValue[0].equals("0"));
    // Storm makes sure admin priveleges are there so expected is different from set
    findAclInFile(blobKey, expectedListAcls);

    // set permissions back to include other to do update below
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
    String fileName = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormDistCacheCli/input.txt";
    String blobKey = UUID.randomUUID().toString();
    String[] returnValue = null;

    // Create it with empty string permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName, "-a", "" }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":rwa");

    // Now delete it.
    String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));

    // Create blob without permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":rwa");

    // Now delete it.
    deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "delete", blobKey}, true);
    assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));

    // Create blob with bad permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName, "-a", "u:"+conf.getProperty("USER")+":r--,o::r" }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":rwa", "o::r--");

    // modify blob with bad permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey, "-s", "u:"+conf.getProperty("USER")+":rw-" }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":rwa");

    // modify blob with no permissions
    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":--a");

    returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "set-acl", blobKey, "-s", "u:"+conf.getProperty("USER")+":rwa" }, true);
    assertTrue( "Could not modify the blob", returnValue[0].equals("0"));

    // Make sure the one we want is there.
    findAclInFile(blobKey, "u:"+conf.getProperty("USER")+":rwa");

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

  void findAclInFile(String blobKey, String... aclsToMatch) throws Exception {
    int matchCount = 0;

    for (String acl : getAclsForFile(blobKey)) {
        for (String toMatch : aclsToMatch) {
            logger.info("    trying to match " + toMatch + " with " + acl );
            if (acl.equals(toMatch)) {
                matchCount++;
            }
        }
    }
    
    assertEquals("Didn't find all the acls", aclsToMatch.length, matchCount);
  }


  // uncompress flag indicates whether storm should uncompress the blob
  public void launchBlobStoreTopologyWithUncompressFlag(String key, String filename, boolean uncompress) throws Exception {
    String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, 
        "hadooptest.topologies.LocalFileTopology",
        "blob", "-c", 
        "topology.blobstore.map={\""+key+"\": {\"localname\": \""+filename+"\" , \"uncompress\": "+uncompress+"}}" }, true);
    assertTrue( "Could not launch topology", returnValue[0].equals("0") );
  }

  // when blobstore map is empty, it uses the key as the local filename
  public void launchBlobStoreTopologyEmptyMap(String key) throws Exception {
    String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.LocalFileTopology",
        "blob", "-c", "topology.blobstore.map={\""+key+"\": {}}" }, true);
    assertTrue( "Could not launch topology", returnValue[0].equals("0") );
  }

  public void launchBlobStoreTopology(String key, String filename) throws Exception {
    String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.LocalFileTopology",
        "blob", "-c", "topology.blobstore.map={\""+key+"\": {\"localname\": \""+filename+"\"}}" }, true);
    assertTrue( "Could not launch topology", returnValue[0].equals("0") );
  }

  public void testCreateModifyFromTopology(String blobKey, String blobACLs, boolean useLocalName) throws Exception {
     String fileName = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormDistCacheCli/input.txt";
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

    try {
      String expectedBlobName = "myFile";
      if (useLocalName) {
        launchBlobStoreTopology( blobKey, expectedBlobName );
      } else {
        expectedBlobName = blobKey;
        launchBlobStoreTopologyEmptyMap( blobKey);
      }
      Util.sleep(30); 
  
      // Hit it with drpc function
      String drpcResult = cluster.DRPCExecute( "blobstore", expectedBlobName );
      logger.debug("drpc result = " + drpcResult);
  
      String permsResult = cluster.DRPCExecute( "permissions", expectedBlobName );
      logger.debug("permissions result = " + permsResult);
  
      assertTrue("Did not get expected result back from blobstore topology", drpcResult.equals("This is original content."));
      assertTrue("File was not created with proper permissions",
          permsResult.equals(conf.getProperty("USER")+":r--rw----"));
    } finally {
        killAll();

        String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
                "delete", blobKey}, true);
        assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));
    }
  }

  public void testBlobUncompressFlagFromTopology(String blobKey, boolean uncompress) throws Exception {
    String fileName = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormDistCacheCli/test.tgz";
    String[] returnValue = null;
      returnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
            "create", blobKey, "-f", fileName }, true);
    assertTrue( "Could not create the blob", returnValue[0].equals("0"));
    
    try {
      String expectedBlobName = "myFile.tgz";
      launchBlobStoreTopologyWithUncompressFlag( blobKey, expectedBlobName, uncompress );
      Util.sleep(30); 
   
      // use permissions check to make sure file was uncompressed 
      if (uncompress) {
        String permsResult = cluster.DRPCExecute( "permissions", expectedBlobName );
        logger.debug("permissions result = " + permsResult);
        assertTrue("File was not created with proper permissions",
          permsResult.equals(conf.getProperty("USER")+":r-xrwx---"));
    
        permsResult = cluster.DRPCExecute( "permissions", expectedBlobName + "/testxfile.sh" );
        logger.debug("permissions result = " + permsResult);
        assertTrue("File was not created with proper permissions",
          permsResult.equals(conf.getProperty("USER")+":r-xrw----"));
    
        permsResult = cluster.DRPCExecute( "permissions", expectedBlobName + "/testdir/foo.txt" );
        logger.debug("permissions result = " + permsResult);
        assertTrue("File was not created with proper permissions",
          permsResult.equals(conf.getProperty("USER")+":r--rw----"));
    
      } else {
        String permsResult = cluster.DRPCExecute( "permissions", expectedBlobName );
        logger.debug("permissions result = " + permsResult);
   
        assertTrue("File was not created with proper permissions",
          permsResult.equals(conf.getProperty("USER")+":r--rw----"));
      }
    } finally {
        killAll();

        String[] deleteReturnValue = exec.runProcBuilder(new String[] { "storm", "blobstore",
                "delete", blobKey}, true);
        assertTrue( "Could not delete the blob", deleteReturnValue[0].equals("0"));
    }
  }

  @Test(timeout=240000)
  public void testTopoWithFullAclCompressedFlagOff() throws Exception {
    testBlobUncompressFlagFromTopology(UUID.randomUUID().toString() + ".tgz", false);
  }

  @Test(timeout=240000)
  public void testTopoWithFullAclCompressedFlagOn() throws Exception {
    testBlobUncompressFlagFromTopology(UUID.randomUUID().toString() + ".tgz", true);
  }

  @Test(timeout=240000)
  public void testTopoWithFullAclEmptyMap() throws Exception {
    testCreateModifyFromTopology(UUID.randomUUID().toString() + ".jar", "u:"+conf.getProperty("USER")+":rwa", false);
  }

  @Test(timeout=240000)
  public void testTopoWithFullAcl() throws Exception {
    testCreateModifyFromTopology(UUID.randomUUID().toString() + ".jar", "u:"+conf.getProperty("USER")+":rwa", true);
  }

  @Test(timeout=240000)
  public void testTopoWithNoAcl() throws Exception {
    testCreateModifyFromTopology(UUID.randomUUID().toString() + ".jar", null, true);
  }

  @Test(timeout=600000)
  public void testDistCacheCli() throws Exception {
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":rwa", 
                           "u:"+conf.getProperty("USER")+":rwa");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":r", 
                           "u:"+conf.getProperty("USER")+":r-a");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":--a", 
                           "u:"+conf.getProperty("USER")+":--a");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":r-a", 
                           "u:"+conf.getProperty("USER")+":r-a");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":-wa", 
                           "u:"+conf.getProperty("USER")+":-wa");
    // these are ok because storm should always add admin access for current owner
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":-w-", 
                           "u:"+conf.getProperty("USER")+":-wa");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           "u:"+conf.getProperty("USER")+":rw-", 
                           "u:"+conf.getProperty("USER")+":rwa");
    testCreateAccessDelete(UUID.randomUUID().toString() + ".jar", 
                           null, 
                           "u:"+conf.getProperty("USER")+":--a");
  }
}
