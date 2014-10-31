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
  public void testDistCacheNoFqdnAcl() throws Exception {
    testDistCacheIntegration("u:hadoopqa:rwa");
  }

  @Test(timeout=240000)
  public void testDistCacheInvalidAcl() throws Exception {
    Boolean didItFail = false;
    try {
        testDistCacheIntegration("u:bogus:rwa");
    } catch (Exception e) {
        didItFail = true;
    }
    assertTrue( "We launched topology when we should not have", didItFail );
  }

  public void testDistCacheIntegration(String blobACLs) throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "This is integration blob content";
    String fileName = "myFile";

    ClientBlobStore clientBlobStore = getClientBlobStore();

    SettableBlobMeta settableBlobMeta = makeAclBlobMeta(blobACLs);

    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    try {
        // Launch a topology that will read a local file we give it over drpc
        logger.debug("About to launch topology");
        launchBlobStoreTopology(blobKey, fileName);

        // Wait for it to come up
        Util.sleep(30);
    
        // Hit it with drpc function
        String drpcResult = cluster.DRPCExecute( "blobstore", fileName );
        logger.debug("drpc result = " + drpcResult);

        // Make sure the value returned is correct.
        assertTrue("Did not get expected result back from blobstore topology", drpcResult.equals(blobContent));

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
        //assertTrue("Did not get updated result back from blobstore topology", drpcResult.equals(modifiedBlobContent));
    } finally {
	killAll();
        clientBlobStore.deleteBlob(blobKey);
    }
  }

  @Test(timeout=600000)
  public void testDistCacheApi() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobACLs = "u:hadoopqa@DEV.YGRID.YAHOO.COM:rwa";
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
    String otherACLs = "o::rwa";
    
    // Other acl list
    SettableBlobMeta otherBlobMeta = makeAclBlobMeta(otherACLs);
    createBlobWithContent(blobKey3, blobContent, clientBlobStore, otherBlobMeta);
    ReadableBlobMeta otherReadBlobMeta = clientBlobStore.getBlobMeta(blobKey3);
    assertNotNull(otherReadBlobMeta);
    AccessControl otherActualAcl = modifiedBlobMeta.get_settable().get_acl().get(1);
    String otherActualAclString = Utils.toString(otherActualAcl);
    logger.info("No acl string is " + otherActualAclString);
    assertEquals("No Acl isnt as expected", otherActualAclString, otherACLs);

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

  private void createBlobWithContent(String blobKey, String blobContent, ClientBlobStore clientBlobStore, SettableBlobMeta settableBlobMeta) throws AuthorizationException, KeyAlreadyExistsException, IOException {
    AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey,settableBlobMeta);
    blobStream.write(blobContent.getBytes());
    blobStream.close();
  }

  private String getBlobContent(String blobKey, ClientBlobStore clientBlobStore) throws AuthorizationException, KeyNotFoundException, IOException {
    InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
    BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
    return r.readLine();
  }
}
