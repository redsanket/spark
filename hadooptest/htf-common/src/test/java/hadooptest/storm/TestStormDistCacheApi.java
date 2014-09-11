package hadooptest.storm;

import backtype.storm.Config;
import backtype.storm.blobstore.AtomicOutputStream;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
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

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }


  @Test(timeout=600000)
  public void testDistCacheApi() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobACLs = "u:hadoopqa@DEV.YGRID.YAHOO.COM:rwa";
    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String blobContent = "This is a sample blob content";

    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);

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
