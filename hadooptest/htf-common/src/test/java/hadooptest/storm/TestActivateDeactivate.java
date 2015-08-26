package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.Util;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.YahooStormCluster;
import hadooptest.automation.utils.http.HTTPHandle;
import org.apache.commons.httpclient.HttpMethod;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.StormDaemon;
import hadooptest.Util;
import static org.junit.Assume.assumeTrue;
import org.json.simple.JSONValue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.*;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Category(SerialTests.class)
public class TestActivateDeactivate extends TestSessionStorm {

    static YahooStormCluster mc = null;
    static String topologyName = "commandTopology";
    static backtype.storm.Config stormConfig = null;

    public class DirEntry {
        public final String name;
        public final String permissions;
        public final String user;
        public final String group;
        public DirEntry( String name, String permissions, String user, String group) {
            this.name = name;
            this.permissions = permissions;
            this.user = user;
            this.group = group;
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction("command");
        mc = (YahooStormCluster) cluster;
        assumeTrue(mc != null);
        stormConfig = new backtype.storm.Config();
        stormConfig.putAll(backtype.storm.utils.Utils.readStormConfig());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    public void launchTopology() throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.CommandTopology",  topologyName }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );
    }

    public Map<String, DirEntry> getListing(String directory) throws Exception{
        String drpcResult = cluster.DRPCExecute( "command", "ls -l " + directory );
        String lines[] = drpcResult.split("\\r?\\n");

        // First line should have return code
        if (lines.length == 0) {
            return null;
        }

        // Get return code
        String firstLine[] = lines[0].split(",");
        if ( !firstLine[0].equals("0")) {
            logger.warn(" Listing of director returned non zero status " + lines[0]);
            return null;
        }

        HashMap<String, DirEntry> returnValue = new HashMap<String, DirEntry>();
        // Parse it.
        for (int i = 1 ; i < lines.length ; i++) {
            String dirElements[] = lines[i].split("\\s+");
            DirEntry entry = new DirEntry( dirElements[8], dirElements[0], dirElements[2], dirElements[3]);
        
            returnValue.put(entry.name, entry);
        }

        return returnValue;
    }

    public void checkTopDirPerms() throws Exception {
        ArrayList<String> subdirs = new ArrayList<String>() {{
            add("supervisor");
            add("workers-users");
            add("workers");
            add("workers-artifacts");
        }};

        String localDir = (String)stormConfig.get("storm.local.dir");
        String workerLauncherGroup = (String)mc.getConf("ystorm.worker_launcher_group", StormDaemon.SUPERVISOR);
        String clusterUser = (String)mc.getConf("ystorm.storm_cluster_user", StormDaemon.SUPERVISOR);
        logger.info("localDir=" + localDir);
        logger.info("group=" + workerLauncherGroup);
        logger.info("user=" + clusterUser);

        Map<String, DirEntry> dir = getListing(localDir);
        assertNotNull( "Was not able to list " + localDir, dir);

        for (int i = 0 ; i < subdirs.size() ; i++) {
            String subdir = subdirs.get(i);
            assertTrue( "permissions were " + dir.get(subdir).permissions + " expecting drwxr-xr-x", dir.get(subdir).permissions.equals("drwxr-xr-x")); 
            assertTrue( "user was " + dir.get(subdir).user + " expecting " + clusterUser, dir.get(subdir).user.equals(clusterUser)); 
            assertTrue( "group was " + dir.get(subdir).group + " expecting " + workerLauncherGroup, dir.get(subdir).group.equals(workerLauncherGroup)); 
        }
    }

    public void checkWorkerDirPerms() throws Exception {
        String topoId = getFirstTopoIdForName(topologyName);

        // Worker Host
        TopologyInfo ti = cluster.getTopologyInfo(topoId);
        int workerPort = ti.get_executors().get(0).get_port();

        String localDir = (String)stormConfig.get("storm.local.dir");
        String workerLauncherGroup = (String)mc.getConf("ystorm.worker_launcher_group", StormDaemon.SUPERVISOR);
        String clusterUser = (String)mc.getConf("ystorm.storm_cluster_user", StormDaemon.SUPERVISOR);
        String myUser = conf.getProperty("USER");
        logger.info("localDir=" + localDir);
        logger.info("group=" + workerLauncherGroup);
        logger.info("user=" + clusterUser);

        String topoPath = localDir + "/workers-artifacts/" + topoId + "/" + Integer.toString(workerPort);
        logger.info("Doing a listing of =" + topoPath);
        Map<String, DirEntry> dir = getListing(topoPath);
        assertNotNull( "Was not able to list " + topoPath, dir);

        HashMap<String, DirEntry> expected = new HashMap<String, DirEntry>();
        expected.put( "worker.log", new DirEntry( "worker.log", "-rw-r-----", myUser, workerLauncherGroup ));
        expected.put( "worker.log.out", new DirEntry( "worker.log.out", "-rw-r-----", myUser, workerLauncherGroup ));
        expected.put( "worker.log.err", new DirEntry( "worker.log.err", "-rw-r-----", myUser, workerLauncherGroup ));
        expected.put( "worker.yaml", new DirEntry( "worker.yaml", "-rw-r--r--", clusterUser, workerLauncherGroup ));

        for (Map.Entry<String, DirEntry> entry : expected.entrySet()) {
            DirEntry found = dir.get( entry.getKey() );
            assertTrue( "permissions are " + found.permissions + " expecting " + entry.getValue().permissions, found.permissions.equals(entry.getValue().permissions)); 
            assertTrue( "user is " + found.user + " expecting " + entry.getValue().user, found.user.equals(entry.getValue().user)); 
            assertTrue( "group is " + found.group + " expecting " + entry.getValue().group, found.group.equals(entry.getValue().group)); 
        }
    }

    @Test(timeout=600000)
    public void DeactivateActivateTest() throws Exception {
        launchTopology();

        // Wait for it to come up
        Util.sleep(30);

        try {
            // Is everything in top dir correct?
            checkTopDirPerms();

            // Check out workers-artifacts
            checkWorkerDirPerms();

            // Stop, deactivate, activate, start supervisor daemons
            String topoId = getFirstTopoIdForName(topologyName);
            TopologyInfo ti = cluster.getTopologyInfo(topoId);
            String host = ti.get_executors().get(0).get_host();

            mc.stopDaemonNode( StormDaemon.SUPERVISOR, host );
            String myPackage = mc.deactivateYinstPackageOnNode( StormDaemon.SUPERVISOR, host );
            mc.activateYinstPackageOnNode( myPackage, host );
            mc.startDaemonNode( StormDaemon.SUPERVISOR, host );
            Util.sleep(30);
            
            // Is everything in top dir correct?
            checkTopDirPerms();

            // Check out workers-artifacts
            checkWorkerDirPerms();
        } finally {
            cluster.killTopology(topologyName);
        }
    }
}
