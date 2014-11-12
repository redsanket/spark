package hadooptest.storm;

import hadooptest.Util;
import backtype.storm.Config;
import backtype.storm.blobstore.AtomicOutputStream;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

@Category(SerialTests.class)
public class TestStormDistCacheApi extends TestSessionStorm {

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction("blobstore");
        cluster.setDrpcAclForFunction("permissions");
    }

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }

  public void launchBlobStoreTopology(String key, String filename) throws Exception {
    String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.LocalFileTopology", "blob", "-c", "topology.blobstore.map={\""+key+"\":\""+filename+"\"}" }, true);
    assertTrue( "Could not launch topology", returnValue[0].equals("0") );
  }

  // Doing this as a forced re-evaulation in case configs change and we use different storage
  public ClientBlobStore getClientBlobStore() throws Exception {
    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    return Utils.getClientBlobStore(theconf);
  }

  public  SettableBlobMeta makeAclBlobMeta(String blobACLs) {
    List<AccessControl> acls = new LinkedList<AccessControl>();
    AccessControl blobACL = Utils.parseAccessControl(blobACLs);
    acls.add(blobACL);
    return new SettableBlobMeta(acls);
  }

  @Test(timeout=240000)
  public void testDistCacheOtherAcl() throws Exception {
    testDistCacheIntegration("o::rwa");
  }

  @Test(timeout=240000)
  public void testDistUserNoWrite() throws Exception {
    testDistCacheIntegration("u:hadoopqa:r--");
  }

  @Test(timeout=240000)
  public void testDistCacheEmptyAcl() throws Exception {
    testDistCacheIntegration("");
  }

  @Test(timeout=240000)
  public void testDistCacheNoFqdnAcl() throws Exception {
    testDistCacheIntegration("u:hadoopqa:rwa");
  }

  //@Test(timeout=240000)
  public void testDistCacheInvalidAcl() throws Exception {
    Boolean didItFail = false;
    testDistCacheIntegration("u:bogus:rwa", true, true);
  }

  public void testDistCacheIntegration(String blobACLs) throws Exception {
    testDistCacheIntegration(blobACLs, false, false);
  }

  public void testDistCacheIntegration(String blobACLs, Boolean changeUser, Boolean expectFailure ) throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "This is integration blob content";
    String fileName = "myFile";

    // Just in case a hung test left a residual topology...
    killAll();

    if (changeUser) {
        kinit(conf.getProperty("SECONDARY_KEYTAB"), conf.getProperty("SECONDARY_PRINCIPAL") );
    }

    ClientBlobStore clientBlobStore = getClientBlobStore();

    SettableBlobMeta settableBlobMeta = makeAclBlobMeta(blobACLs);

    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    if (changeUser) {
        kinit();
    }

    try {
        // Launch a topology that will read a local file we give it over drpc
        logger.debug("About to launch topology");
        launchBlobStoreTopology(blobKey, fileName);

        // Wait for it to come up
        Util.sleep(30);
    
        // Hit it with drpc function
        String drpcResult = cluster.DRPCExecute( "blobstore", fileName );
        logger.debug("drpc result = " + drpcResult);

        String permsResult = cluster.DRPCExecute( "permissions", fileName );
        logger.debug("permissions result = " + permsResult);

        // Make sure the value returned is correct.
        if (expectFailure) {
            assertTrue("Did not get expected failure", drpcResult.equals("Got IO exception"));
        } else {
            assertTrue("Did not get expected result back from blobstore topology", drpcResult.equals(blobContent));
            assertTrue("Did not get expected result back from permissions check", permsResult.equals("hadoopqa:rwxrwx---"));
        }

        String modifiedBlobContent = "This is modified integration content";
        updateBlobWithContent(blobKey, clientBlobStore, modifiedBlobContent);
        String actualModfiedContent = getBlobContent(blobKey, clientBlobStore);

        // Wait for content to get pushed
        Util.sleep(30);

        // Hit it with drpc function
        drpcResult = cluster.DRPCExecute( "blobstore", fileName );
        logger.debug("drpc result = " + drpcResult);

        // Make sure the value returned is correct.
        // Skipping until feature fix is ready.
        if (expectFailure) {
            assertTrue("Did not get expected failure", drpcResult.equals("Got IO exception"));
        } else {
            //assertTrue("Did not get updated result back from blobstore topology", drpcResult.equals(modifiedBlobContent));
        }
    } finally {
        killAll();
        clientBlobStore.deleteBlob(blobKey);
    }
  }

  @Test(timeout=240000)
  // The purpose of this test is to make sure that we can't create a blob that the owner cannot set acls on
  public void testDistCacheNoAcl() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "Testing this blob content.";

    // Create with empty acl list and see what it does
    ClientBlobStore clientBlobStore = getClientBlobStore();
    List<AccessControl> acls = new LinkedList<AccessControl>();
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    // Now verify hadoopqa (primary user) is in the acl list.
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    assertTrue("Hadoopqa was not found in the acl list", lookForAcl(clientBlobStore, blobKey, "u:hadoopqa:rwa"));

    // Delete it
    clientBlobStore.deleteBlob(blobKey);
  }

  @Test(timeout=240000)
  // The purpose of this test is to make sure that we can't create a blob that the owner cannot set acls on
  public void testDistCacheUserWithNoWriteAcl() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "Testing this blob content.";

    // Create with acl list with user-wa and see what it does
    ClientBlobStore clientBlobStore = getClientBlobStore();
    List<AccessControl> acls = new LinkedList<AccessControl>();
    AccessControl blobACL = Utils.parseAccessControl("u:hadoopqa:r--");
    acls.add(blobACL);
    blobACL = Utils.parseAccessControl("o::r");
    acls.add(blobACL);
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    // Now verify hadoopqa (primary user) is in the acl list.
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    assertTrue("Hadoopqa was not found in the acl list", lookForAcl(clientBlobStore, blobKey, "u:hadoopqa:rwa"));
    assertTrue("other was not found in the acl list", lookForAcl(clientBlobStore, blobKey, "o::r--"));

    //Now try to unset my own permissions
    AccessControl updateAcl = Utils.parseAccessControl("u:hadoopqa:rw-");
    List<AccessControl> updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list", lookForAcl(clientBlobStore, blobKey, "u:hadoopqa:rwa"));

    //Now turn off my own write, and then try to write 
    AccessControl noWriteAcl = Utils.parseAccessControl("u:hadoopqa:r-a");
    List<AccessControl> noWriteAcls = new LinkedList<AccessControl>();
    noWriteAcls.add(noWriteAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(noWriteAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list", lookForAcl(clientBlobStore, blobKey, "u:hadoopqa:r-a"));
    String modifiedBlobContent = "This is modified content";
    Boolean updateFailed = false;
    try {
        updateBlobWithContent(blobKey, clientBlobStore, modifiedBlobContent);
    } catch (Exception e) {
        updateFailed = true;
    }
    assertTrue("Was able to modify content when we should not", updateFailed);

    //Now try and do it with an empty acl list
    List<AccessControl> emptyAcls = new LinkedList<AccessControl>();
    modifiedSettableBlobMeta = new SettableBlobMeta(emptyAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    // We have to use superuser permissions to read the acl list once we have turned off our own read permissions
    kinit(conf.getProperty("BLOB_SUPERUSER_KEYTAB"), conf.getProperty("BLOB_SUPERUSER_PRINCIPAL") );
    ClientBlobStore clientSUBlobStore = getClientBlobStore();
    assertTrue("Hadoopqa was not found in the empty acl list", lookForAcl(clientSUBlobStore, blobKey, "u:hadoopqa:--a"));

    // Turn on rw again.
    updateAcl = Utils.parseAccessControl("u:hadoopqa:rwa");
    List<AccessControl> restoreAcls = new LinkedList<AccessControl>();
    restoreAcls.add(updateAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(restoreAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the restored acl list", lookForAcl(clientBlobStore, blobKey, "u:hadoopqa:rwa"));

    // Delete it
    clientBlobStore.deleteBlob(blobKey);

    // Restore user
    kinit();
  }

  public boolean lookForAcl(ClientBlobStore clientBlobStore, String blobKey, String aclString) throws Exception {
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    Boolean foundAcl = false;
    for (AccessControl acl :  blobMeta.get_settable().get_acl() ) {
        String thisAclString = Utils.toString(acl);
        logger.info("Looking for: " + aclString + " Found: " + thisAclString);
        if (thisAclString.equals(aclString)) {
            foundAcl = true;
        }
    }
    return foundAcl;
  }

  @Test(timeout=600000)
  public void testDistCacheApi() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobACLs = "u:hadoopqa:rwa";
    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String blobContent = "This is a sample blob content";

    ClientBlobStore clientBlobStore = getClientBlobStore();

    List<AccessControl> acls = new LinkedList<AccessControl>();
    AccessControl blobACL = Utils.parseAccessControl(blobACLs);
    acls.add(blobACL);
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);

    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    AccessControl actualAcl = blobMeta.get_settable().get_acl().get(0);
    String actualAclString = Utils.toString(actualAcl);
    assertEquals("Actual ACL is different for created blob", blobACLs, actualAclString);

    String actualContent = getBlobContent(blobKey, clientBlobStore);
    assertEquals("Blob Content is not matching", blobContent, actualContent);

    String modifiedBlobContent = "This is modified content";
    updateBlobWithContent(blobKey, clientBlobStore, modifiedBlobContent);
    String actualModfiedContent = getBlobContent(blobKey, clientBlobStore);
    assertEquals("Updated Blob Content is not matching", modifiedBlobContent, actualModfiedContent);

    String otherACLString = "o::r-a";
    AccessControl othersACL = Utils.parseAccessControl(otherACLString);
    acls.add(othersACL);
    SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(acls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    ReadableBlobMeta modifiedBlobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(modifiedBlobMeta);
    AccessControl modifiedActualAcl = modifiedBlobMeta.get_settable().get_acl().get(1);
    String modifiedActualAclString = Utils.toString(modifiedActualAcl);
    AccessControl myActualAcl = modifiedBlobMeta.get_settable().get_acl().get(0);
    String myActualAclString = Utils.toString(myActualAcl);
    logger.info("Actual modified ACL is " + modifiedActualAclString);
    logger.info("Other modified ACL is " + modifiedActualAclString);
    logger.info("My modified ACL is " + modifiedActualAclString);
    assertEquals("Actual ACL is different for modified blob", otherACLString, modifiedActualAclString);

    boolean keyFound = isBlobFound(blobKey, clientBlobStore);
    assertTrue("Blob is not listed on the blobstore", keyFound);

    clientBlobStore.deleteBlob(blobKey);

    boolean deletedKeyFound = isBlobFound(blobKey, clientBlobStore);
    assertFalse("Deleted Blob is listed on the blobstore", deletedKeyFound);

    // Add API test for principal to local
    UUID uuid2 = UUID.randomUUID();
    String blobKey2 = uuid2.toString() + ".jar";
    String blobNoFqdnACLs = "u:hadoopqa:rwa";
    SettableBlobMeta noFqdnBlobMeta = makeAclBlobMeta(blobNoFqdnACLs);
    createBlobWithContent(blobKey2, blobContent, clientBlobStore, noFqdnBlobMeta);
    
    // Now read it back
    actualContent = getBlobContent(blobKey2, clientBlobStore);
    assertEquals("Blob Content with no fqdn is not matching", blobContent, actualContent);
    
    // Delete it
    clientBlobStore.deleteBlob(blobKey2);

    // Check for deletion
    deletedKeyFound = isBlobFound(blobKey2, clientBlobStore);
    assertFalse("Deleted Blob with local principal is listed on the blobstore", deletedKeyFound);

    // Add API test for using other acl.  It should default to "o::r-a"
    UUID uuid3 = UUID.randomUUID();
    String blobKey3 = uuid3.toString() + ".jar";
    String worldAll = "o::rwa";
    
    // Other acl list
    SettableBlobMeta otherBlobMeta = makeAclBlobMeta(worldAll);
    createBlobWithContent(blobKey3, blobContent, clientBlobStore, otherBlobMeta);
    ReadableBlobMeta otherReadBlobMeta = clientBlobStore.getBlobMeta(blobKey3);
    assertNotNull(otherReadBlobMeta);
    AccessControl otherActualAcl = otherReadBlobMeta.get_settable().get_acl().get(1);
    AccessControl anotherActualAcl = otherReadBlobMeta.get_settable().get_acl().get(0);
    printACLs("  Other ACL only", otherReadBlobMeta.get_settable());
    String otherActualAclString = Utils.toString(anotherActualAcl);
    logger.info("In entry point 0, " + Utils.toString(anotherActualAcl));
    logger.info("No acl string is " + otherActualAclString);
    assertEquals("No Acl isnt as expected", otherActualAclString, worldAll);

    // Now read it back
    actualContent = getBlobContent(blobKey3, clientBlobStore);
    assertEquals("Blob Content with no acl is not matching", blobContent, actualContent);
    
    // Delete it
    clientBlobStore.deleteBlob(blobKey3);

    // Check for deletion
    deletedKeyFound = isBlobFound(blobKey3, clientBlobStore);
    assertFalse("Deleted Blob with local principal is listed on the blobstore", deletedKeyFound);
  }

  private boolean isBlobFound(String blobKey, ClientBlobStore clientBlobStore) {
    boolean keyFound = false;
    Iterator<String> stringIterator = clientBlobStore.listKeys();
    while(stringIterator.hasNext()) {
      String key = stringIterator.next();
      if(key.equals(blobKey))
      {
        keyFound = true;
        break;
      }
    }
    return keyFound;
  }

  private void updateBlobWithContent(String blobKey, ClientBlobStore clientBlobStore, String modifiedBlobContent) throws KeyNotFoundException, AuthorizationException, IOException {
    AtomicOutputStream blobOutputStream = clientBlobStore.updateBlob(blobKey);
    blobOutputStream.write(modifiedBlobContent.getBytes());
    blobOutputStream.close();
  }

  private void createBlobWithContent(String blobKey, String blobContent, ClientBlobStore clientBlobStore, SettableBlobMeta settableBlobMeta) throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
    logger.info("About to create content <" + blobContent + "> for key " + blobKey);
    printACLs("  attempting to Create", settableBlobMeta);
    AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey,settableBlobMeta);
    blobStream.write(blobContent.getBytes());
    blobStream.close();

    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    printACLs("  after Create", blobMeta.get_settable());
  }

  private void printACLs(String prefix, SettableBlobMeta settableBlobMeta) {
    for (AccessControl acl :  settableBlobMeta.get_acl() ) {
        String aclString = Utils.toString(acl);
        logger.info(prefix + " " + aclString);
    }
  }

  private String getBlobContent(String blobKey, ClientBlobStore clientBlobStore) throws AuthorizationException, KeyNotFoundException, IOException {
    InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
    BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
    return r.readLine();
  }
}
