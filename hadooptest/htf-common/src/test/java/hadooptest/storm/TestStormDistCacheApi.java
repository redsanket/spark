package hadooptest.storm;

import hadooptest.Util;
import backtype.storm.Config;
import backtype.storm.blobstore.AtomicOutputStream;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import backtype.storm.blobstore.BlobStoreAclHandler;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assume.assumeTrue;

import hadooptest.cluster.storm.StormDaemon;
import hadooptest.automation.utils.http.HTTPHandle;
import org.apache.commons.httpclient.HttpMethod;
import hadooptest.automation.utils.http.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.lang.System;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;

import static org.junit.Assert.*;

@Category(SerialTests.class)
public class TestStormDistCacheApi extends TestSessionStorm {
  int MAX_RETRIES = 10;
  @BeforeClass
  public static void setup() throws Exception {
    start();
    cluster.setDrpcAclForFunction("blobstore");
    cluster.setDrpcAclForFunction("permissions");
    cluster.setDrpcAclForFunction("md5");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }

  public void launchBlobStoreTopology(String key, String filename) throws Exception {
    String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
    String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.LocalFileTopology", "blob", "-c", "topology.blobstore.map={\""+key+"\": { \"localname\" : \""+filename+"\"}}" }, true);
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
    AccessControl blobACL = BlobStoreAclHandler.parseAccessControl(blobACLs);
    acls.add(blobACL);
    return new SettableBlobMeta(acls);
  }

  @Test(timeout=240000)
  public void testDistCacheOtherAcl() throws Exception {
    testDistCacheIntegration("o::rwa");
  }

  @Test(timeout=240000)
  public void testDistUserNoWrite() throws Exception {
    testDistCacheIntegration("u:"+conf.getProperty("USER")+":r--");
  }

  @Test(timeout=240000)
  public void testDistCacheEmptyAcl() throws Exception {
    testDistCacheIntegration("");
  }

  @Test(timeout=240000)
  public void testDistCacheNoFqdnAcl() throws Exception {
    testDistCacheIntegration("u:"+conf.getProperty("USER")+":rwa");
  }

  @Test(timeout=240000)
  public void testDistCacheInvalidAcl() throws Exception {
    testDistCacheIntegration("u:bogus:rwa", true, true);
  }

  @Test(timeout=240000)
  // The purpose of this test is to check whether supervisor crashes if an authorization exception is triggered
  // in the blobstore
  public void testDistCacheAuthExceptionForSupervisorCrash() throws Exception {
    testDistCacheForSupervisorCrash("u:test:rwa", true);
  }

  @Test(timeout=240000)
  // The purpose of this test is to check whether supervisor crashes if an keynotfound exception is triggered
  // in the blobstore
  public void testDistCacheKeyNotFoundExceptionForSupervisorCrash() throws Exception {
    testDistCacheForSupervisorCrash("u:"+conf.getProperty("USER")+":rwa", false);
  }

  public void testDistCacheForSupervisorCrash(String blobACLs, boolean changeUser) throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "This is integration blob content";
    String fileName = "myFile";

    // Just in case a hung test left a residual topology...
    killAll();

    if(changeUser) {
      kinit(conf.getProperty("SECONDARY_KEYTAB"), conf.getProperty("SECONDARY_PRINCIPAL"));
      ClientBlobStore clientBlobStore = getClientBlobStore();
      SettableBlobMeta settableBlobMeta = makeAclBlobMeta(blobACLs);
      createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);
      kinit();
    }

    JSONArray supervisorsUptimeBeforeTopoLaunch = null;
    JSONArray supervisorsUptimeAfterTopoLaunch = null;

    try {
      // Launch a topology that will read a local file we give it over drpc
      logger.info("About to launch topology");

      // It's possible that a prior test restarted supervisors.  Make sure they are all up.
      HTTPHandle client = bouncerAuthentication();
      int tryCount = 0;
      do {
        if (tryCount > 0) {
            Util.sleep(10);
        }
        supervisorsUptimeBeforeTopoLaunch = getSupervisorsUptime();
      } while ( supervisorsUptimeBeforeTopoLaunch.size() < cluster.lookupRole(StormDaemon.SUPERVISOR).size() && ++tryCount < 10);
      assertTrue("All supervisors were not up to start with", tryCount < 10);

      // Now that all of the supervisors are up, we can get on with it.
      launchBlobStoreTopology(blobKey, fileName);
      // Wait for it to come up
      Util.sleep(30);
      supervisorsUptimeAfterTopoLaunch = getSupervisorsUptime();

      // Test for supervisors not crashing
      assertTrue("Supervisor Crashed", !didSupervisorCrash(supervisorsUptimeBeforeTopoLaunch, supervisorsUptimeAfterTopoLaunch, 30));

    } finally {
      if ( supervisorsUptimeBeforeTopoLaunch != null && supervisorsUptimeAfterTopoLaunch != null ) {
        // If we got supervisor uptimes, print them out.
        for (int i=0; i<supervisorsUptimeBeforeTopoLaunch.size(); i++) {
              logger.warn("Starting uptime for sup " + Integer.toString(i) + " " + (String)supervisorsUptimeBeforeTopoLaunch.getJSONObject(i).get("uptime"));
              logger.warn("Ending uptime for sup " + Integer.toString(i) + " " + (String)supervisorsUptimeAfterTopoLaunch.getJSONObject(i).get("uptime"));
        }
      }
      killAll();
    }
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

        // Make sure topology is up
        if (expectFailure) {
            Boolean failed = false;
            try {
                // If we can't get the log, then let's flag that an error.
                String log = getLogForTopology("blob");
                logger.info("Got log=" + log);
                failed = (log.contains("Page not found"));
            } catch (Exception il) {
                failed = true;
            } finally {
                killAll();
            }
            assertTrue("Did not get expected failure", failed);
            return;
        }
 
        // Hit it with drpc function
        String drpcResult = cluster.DRPCExecute( "blobstore", fileName );
        logger.debug("drpc result = " + drpcResult);

        String permsResult = cluster.DRPCExecute( "permissions", fileName );
        logger.debug("permissions result = " + permsResult);

        // Make sure the value returned is correct.
        assertTrue("Did not get expected result back from blobstore topology",
            drpcResult.equals(blobContent));
        assertTrue("Did not get expected result back from permissions check",
            permsResult.equals(conf.getProperty("USER")+":r--rw----"));

        String modifiedBlobContent = "This is modified integration content";
        updateBlobWithContent(blobKey, clientBlobStore, modifiedBlobContent);
        String actualModfiedContent = getBlobContent(blobKey, clientBlobStore);

        // Wait for content to get pushed
        // Let's check this a few times, and exit early if we have "success".

        Boolean updatedIt = false;
        int tryCount = 8;

        while (!updatedIt && --tryCount > 0) {
            Util.sleep(15);
            drpcResult = cluster.DRPCExecute( "blobstore", fileName );
            logger.debug("drpc result = " + drpcResult);
            updatedIt = drpcResult.equals(modifiedBlobContent) || drpcResult.equals("Got IO exception");
        }

        assertTrue("Did not get updated result back from blobstore topology", drpcResult.equals(modifiedBlobContent));

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
    assertTrue("Hadoopqa was not found in the acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":rwa"));

    // Delete it
    clientBlobStore.deleteBlob(blobKey);
  }

  @Test(timeout=240000)
  // The purpose of this test is to make sure we can list the acl with various permissions
  public void testDistCacheUserSetAcls() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "Testing this blob content.";

    // Create with acl list with user-wa and see what it does
    ClientBlobStore clientBlobStore = getClientBlobStore();
    List<AccessControl> acls = new LinkedList<AccessControl>();
    AccessControl blobACL = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":r--");
    acls.add(blobACL);
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    // Now verify hadoopqa (primary user) is in the acl list.
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    assertTrue("Hadoopqa was not found in the acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":rwa"));

    //Now set admin only
    AccessControl updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":--a");
    List<AccessControl> updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":--a"));

    //Now set write only - storm will add in admin
    updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":-w-");
    updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":-wa"));

    //Now set read/admin
    updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":r-a");
    updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":r-a"));

    //Now set write/admin
    updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":-wa");
    updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":-wa"));

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
    AccessControl blobACL = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":r--");
    acls.add(blobACL);
    blobACL = BlobStoreAclHandler.parseAccessControl("o::r");
    acls.add(blobACL);
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    // Now verify hadoopqa (primary user) is in the acl list.
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    assertTrue("Hadoopqa was not found in the acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":rwa"));
    assertTrue("other was not found in the acl list",
        lookForAcl(clientBlobStore, blobKey, "o::r--"));

    //Now try to unset my own permissions
    AccessControl updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":rw-");
    List<AccessControl> updateAcls = new LinkedList<AccessControl>();
    updateAcls.add(updateAcl);
    SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":rwa"));

    //Now turn off my own write, and then try to write
    AccessControl noWriteAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":r-a");
    List<AccessControl> noWriteAcls = new LinkedList<AccessControl>();
    noWriteAcls.add(noWriteAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(noWriteAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the modified acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":r-a"));
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
    assertTrue("Hadoopqa was not found in the empty acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":--a"));

    // Turn on rw again.
    updateAcl = BlobStoreAclHandler.parseAccessControl("u:"+conf.getProperty("USER")+":rwa");
    List<AccessControl> restoreAcls = new LinkedList<AccessControl>();
    restoreAcls.add(updateAcl);
    modifiedSettableBlobMeta = new SettableBlobMeta(restoreAcls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    assertTrue("Hadoopqa was not found in the restored acl list",
        lookForAcl(clientBlobStore, blobKey, "u:"+conf.getProperty("USER")+":rwa"));

    // Delete it
    clientBlobStore.deleteBlob(blobKey);
  }

  public boolean lookForAcl(ClientBlobStore clientBlobStore, String blobKey, String aclString) throws Exception {
    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    Boolean foundAcl = false;
    for (AccessControl acl :  blobMeta.get_settable().get_acl() ) {
        String thisAclString = BlobStoreAclHandler.accessControlToString(acl);
        logger.info("Looking for: " + aclString + " Found: " + thisAclString);
        if (thisAclString.equals(aclString)) {
            foundAcl = true;
        }
    }
    return foundAcl;
  }

    public String blobFileMD5 (String blobFile) throws Exception {
        String line = "md5sum " + blobFile;
        CommandLine cmdline = CommandLine.parse(line);
        DefaultExecutor executor = new DefaultExecutor();
        DefaultExecuteResultHandler handler=new DefaultExecuteResultHandler();
        ByteArrayOutputStream stdout=new ByteArrayOutputStream();
        executor.setStreamHandler(new PumpStreamHandler(stdout));
        executor.execute(cmdline, handler);
        while (!handler.hasResult()) {
            try {
                handler.waitFor();
            }
            catch (InterruptedException e) {
                logger.info ("error in executing md5: " + e.getMessage());
            }
        }
        if (handler.getExitValue()!=0) {
            return ("MD5 return value is not 0, bad execution");
        } else {
            String[] split = stdout.toString().split(" ");
            return split[0];
        }
    }

    @Test(timeout = 1000*30*60)
    // The purpose of this test is to make sure that large blobs do not overwrite each other, and that the whole
    // system can cope with large file transfers.
    public void testLargeBlob() throws Exception {
        String tmpDir = conf.getProperty("STORM_TMP_DIR");
        assumeTrue(tmpDir != null);
        String file1 = tmpDir + "/file1";
        String file2 = tmpDir + "/file2";
        String topoBlobName = "blobFile";
        ClientBlobStore clientBlobStore = getClientBlobStore();

        makeRandomTempFile(file1, 4096);
        makeRandomTempFile(file2, 4096);
        String md5hash1 = blobFileMD5(file1);
        String md5hash2 = blobFileMD5(file2);

        UUID uuid = UUID.randomUUID();
        String blobKey = uuid.toString();
        String blobACLs = "o::rwa";
        SettableBlobMeta blobMeta = makeAclBlobMeta(blobACLs);
        assertNotNull(blobMeta);
        Boolean nimbusHasSupervisor = false;
        try {
            nimbusHasSupervisor = turnOffNimbusSupervisor();
            logger.info("About to create first blob");
            createBlobFromFile(blobKey, file1, clientBlobStore, blobMeta);
            logger.info("First blob created");

            // Launch a topology that will read a local file we give it over drpc
            logger.info("Launching the topology");
            launchBlobStoreTopology(blobKey, topoBlobName);

            logger.info("Test phase 1");
            // Wait a minute for the topology to come up.
            logger.info("Waiting until the DRPC topology starts.");
            Util.sleep(150);
            // Hit it with drpc function
            // We should wait until the DRPC topology starts before hitting it with a DRPCExecute.
            //It should finish in about 90~100 seconds, and we add a minute for safety margin
            logger.info("About to call DRPC");
            String md5Result = cluster.DRPCExecute("md5", topoBlobName);
            logger.info("drpc result = " + md5Result);

            //Running MD5 on a 4GB blob should take about 10 seconds
            assertTrue("1. Did not get the expected MD5 for blob. Expecting md5hash1: " + md5hash1 + " received: " + md5Result,
                    md5Result.equalsIgnoreCase(md5hash1));

            logger.info("Test phase 2");
            updateBlobFromFile(blobKey, file2, clientBlobStore);  //This is a blocking call, when it returns Nimbus has the updated blob
            Util.sleep(10);
            //Not enough time for the new key to propagate from Nimbus to supervisor. Worker should still return the old MD5
            md5Result = cluster.DRPCExecute("md5", topoBlobName);
            assertTrue("2. Did not get the expected MD5 for blob. Expecting md5hash1: " + md5hash1 + " received: " + md5Result,
                    md5Result.equalsIgnoreCase(md5hash1));

            //This should be enough time for the new key to propagate to supervisor (30 seconds update frequency +
            // about a minute to upload the new file). The worker should return the new MD5
            logger.info("Test phase 3");
            int numTries = MAX_RETRIES;
            do {
                Util.sleep(60);
                md5Result = cluster.DRPCExecute("md5", topoBlobName);
                numTries--;
                logger.info("Still waiting for the proper MD5 (step 3, LargeBlob). Will wait for " + numTries + " more minutes");
            } while (!md5Result.equalsIgnoreCase(md5hash2) && numTries>0);
            assertTrue("3. Did not get the expected MD5 for blob. Expecting md5hash2: " + md5hash2 + " received: " + md5Result,
                    md5Result.equalsIgnoreCase(md5hash2));

            logger.info("Test phase 4");
            updateBlobFromFile(blobKey, file1, clientBlobStore);
            Util.sleep(10);
            //Not enough time for the new key to propagate to supervisor. Worker should still return the old MD5
            md5Result = cluster.DRPCExecute("md5", topoBlobName);
            assertTrue("4. Did not get the expected MD5 for blob. Expecting md5hash2: " + md5hash2 + " received: " + md5Result,
                    md5Result.equalsIgnoreCase(md5hash2));

            //This should be enough time for the new key to propagate to supervisor (30 seconds update frequency +
            // less than a minute to upload the new file). The worker should return the new MD5
            logger.info("Test phase 5");
            numTries = MAX_RETRIES;
            do {
                Util.sleep(60);
                md5Result = cluster.DRPCExecute("md5", topoBlobName);
                numTries--;
                logger.info("Still waiting for the proper MD5 (step 5, LargeBlob). Will wait for " + numTries + " more minutes");
            } while (!md5Result.equalsIgnoreCase(md5hash1) && numTries>0);
            assertTrue("5. Did not get the expected MD5 for blob. Expecting md5hash1: " + md5hash1 + " received: " + md5Result,
                    md5Result.equalsIgnoreCase(md5hash1));
        } finally {
            killAll();
            clientBlobStore.deleteBlob(blobKey);
            if (nimbusHasSupervisor) {
                turnOnNimbusSupervisor();
            }
        }
    }

  @Test(timeout=600000)
  public void testDistCacheApi() throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobACLs = "u:"+conf.getProperty("USER")+":rwa";
    String fileName = "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    String blobContent = "This is a sample blob content";

    ClientBlobStore clientBlobStore = getClientBlobStore();

    List<AccessControl> acls = new LinkedList<AccessControl>();
    AccessControl blobACL = BlobStoreAclHandler.parseAccessControl(blobACLs);
    acls.add(blobACL);
    SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);

    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);

    ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(blobMeta);
    AccessControl actualAcl = blobMeta.get_settable().get_acl().get(0);
    String actualAclString = BlobStoreAclHandler.accessControlToString(actualAcl);
    assertEquals("Actual ACL is different for created blob",
        blobACLs, actualAclString);

    String actualContent = getBlobContent(blobKey, clientBlobStore);
    assertEquals("Blob Content is not matching",
        blobContent, actualContent);

    String modifiedBlobContent = "This is modified content";
    updateBlobWithContent(blobKey, clientBlobStore, modifiedBlobContent);
    String actualModfiedContent = getBlobContent(blobKey, clientBlobStore);
    assertEquals("Updated Blob Content is not matching",
        modifiedBlobContent, actualModfiedContent);

    String otherACLString = "o::r-a";
    AccessControl othersACL = BlobStoreAclHandler.parseAccessControl(otherACLString);
    acls.add(othersACL);
    SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(acls);
    clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
    ReadableBlobMeta modifiedBlobMeta = clientBlobStore.getBlobMeta(blobKey);
    assertNotNull(modifiedBlobMeta);
    AccessControl modifiedActualAcl = modifiedBlobMeta.get_settable().get_acl().get(1);
    String modifiedActualAclString = BlobStoreAclHandler.accessControlToString(modifiedActualAcl);
    AccessControl myActualAcl = modifiedBlobMeta.get_settable().get_acl().get(0);
    String myActualAclString = BlobStoreAclHandler.accessControlToString(myActualAcl);
    logger.info("Actual modified ACL is " + modifiedActualAclString);
    logger.info("Other modified ACL is " + modifiedActualAclString);
    logger.info("My modified ACL is " + modifiedActualAclString);
    assertEquals("Actual ACL is different for modified blob",
        otherACLString, modifiedActualAclString);

    boolean keyFound = isBlobFound(blobKey, clientBlobStore);
    assertTrue("Blob is not listed on the blobstore", keyFound);

    clientBlobStore.deleteBlob(blobKey);

    boolean deletedKeyFound = isBlobFound(blobKey, clientBlobStore);
    assertFalse("Deleted Blob is listed on the blobstore", deletedKeyFound);

    // Add API test for principal to local
    UUID uuid2 = UUID.randomUUID();
    String blobKey2 = uuid2.toString() + ".jar";
    String blobNoFqdnACLs = "u:"+conf.getProperty("USER")+":rwa";
    SettableBlobMeta noFqdnBlobMeta = makeAclBlobMeta(blobNoFqdnACLs);
    createBlobWithContent(blobKey2, blobContent, clientBlobStore, noFqdnBlobMeta);
    
    // Now read it back
    actualContent = getBlobContent(blobKey2, clientBlobStore);
    assertEquals("Blob Content with no fqdn is not matching", blobContent, actualContent);
    
    // Delete it
    clientBlobStore.deleteBlob(blobKey2);

    // Check for deletion
    deletedKeyFound = isBlobFound(blobKey2, clientBlobStore);
    assertFalse("Deleted Blob with local principal is listed on the blobstore",
        deletedKeyFound);

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
    String otherActualAclString = BlobStoreAclHandler.accessControlToString(anotherActualAcl);
    logger.info("In entry point 0, " + BlobStoreAclHandler.accessControlToString(anotherActualAcl));
    logger.info("No acl string is " + otherActualAclString);
    assertEquals("No Acl isnt as expected", otherActualAclString, worldAll);

    // Now read it back
    actualContent = getBlobContent(blobKey3, clientBlobStore);
    assertEquals("Blob Content with no acl is not matching", blobContent, actualContent);
    
    // Delete it
    clientBlobStore.deleteBlob(blobKey3);

    // Check for deletion
    deletedKeyFound = isBlobFound(blobKey3, clientBlobStore);
    assertFalse("Deleted Blob with local principal is listed on the blobstore",
        deletedKeyFound);
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

    private void fillBlobStreamFromFile(AtomicOutputStream blobStream, String fileName) throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
        logger.info("About to fill Blob Stream from File");
        File inputFile = new File(fileName);
        byte[] bytes = new byte[1024*1024];
        BufferedInputStream bs = new BufferedInputStream(new FileInputStream( inputFile ));
        int pos = 0;
        int numRead = -1;
        logger.info ("About to read from the bufferedInputStream for the first time");
        while ( (numRead = bs.read(bytes, 0, 1024*1024) ) > 0 ) {
            blobStream.write(bytes, 0, numRead );
            pos += numRead;
        }
        blobStream.close();
        bs.close();
    }

    private void updateBlobFromFile(String blobKey, String fileName, ClientBlobStore clientBlobStore) throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
        logger.info("About to update blob. New content from <" + fileName + "> for key " + blobKey);
        AtomicOutputStream blobStream = clientBlobStore.updateBlob(blobKey);
        fillBlobStreamFromFile(blobStream, fileName);
    }


    private void createBlobFromFile(String blobKey, String fileName, ClientBlobStore clientBlobStore, SettableBlobMeta settableBlobMeta) throws AuthorizationException, KeyAlreadyExistsException, IOException,KeyNotFoundException {
        logger.info("About to transfer content from <" + fileName + "> for key " + blobKey);
        printACLs("  attempting to Create", settableBlobMeta);
        logger.info("About to create a blob from the given key: " + blobKey + " and bloBmeta: ");
        AtomicOutputStream blobStream = clientBlobStore.createBlob(blobKey, settableBlobMeta);
        fillBlobStreamFromFile(blobStream, fileName);
        logger.info("Finished fill Blob Stream from File");

        ReadableBlobMeta blobMeta = clientBlobStore.getBlobMeta(blobKey);
        printACLs("  after Create from File", blobMeta.get_settable());
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
        String aclString = BlobStoreAclHandler.accessControlToString(acl);
        logger.info(prefix + " " + aclString);
    }
  }

  private String getBlobContent(String blobKey, ClientBlobStore clientBlobStore) throws AuthorizationException, KeyNotFoundException, IOException {
    InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
    BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
    return r.readLine();
  }
}
