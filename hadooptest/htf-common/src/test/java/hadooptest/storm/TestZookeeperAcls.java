package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.ExclaimBolt;
import hadooptest.workflow.storm.topology.bolt.ExclaimBoltWithErrors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;

@Category(SerialTests.class)
public class TestZookeeperAcls extends TestSessionStorm {
  private static final Logger LOG = LoggerFactory.getLogger(TestZookeeperAcls.class);
  private static JSONObject nimbusConfJson = null;

  @BeforeClass
  public static void setup() throws Exception {
    nimbusConfJson =
        (JSONObject) JSONValue.parseWithException(cluster.getNimbusConf());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop();
  }

  @Test(timeout=300000)
  public void TestZkErrorNodesNotAutovivicated() throws Exception {
    assumeTrue(cluster instanceof ModifiableStormCluster);
    StormTopology topology = buildSimpleExclamationTopology();
    Config stormConfig = new Config();
    stormConfig.setDebug(true);
    final String topoName = "TestZkErrorNodesNotAutovivicated";
    try {
      cluster.submitTopology(getTopologiesJarFile(), topoName, stormConfig,
          topology);
      final String topoId = getFirstTopoIdForName(topoName);
      waitForTopoUptimeSeconds(topoId, 10);

      ZooKeeper zk = makeZkClient();
      final String stormZkRoot =
          (String) nimbusConfJson.get(Config.STORM_ZOOKEEPER_ROOT);
      List<String> errorsList  = zk.getChildren(stormZkRoot + "/errors", false);
      HashSet<String> errorSet = new HashSet<String>(errorsList);
      assumeTrue("No znodes present under errors for "+topoId,
          !errorSet.contains(topoId));
      assumeTrue("TopologyInfo lists no errors", 0 == getTopoInfoErrorCount(topoId));
    } finally {
      cluster.killTopology(topoName);
    }
  }

  private long getTopoInfoErrorCount(final String topoId) throws Exception{
    long count = 0;
    for (List<ErrorInfo> l :
        cluster.getTopologyInfo(topoId).get_errors().values()) {
      count += l.size();
    }
    return count;
  }

  @Test(timeout=300000)
  public void TestZkErrorNodesHaveCorrectAcls() throws Exception {
    assumeTrue(cluster instanceof ModifiableStormCluster);
    final String errorGeneratingComponentName = "exclaim2errors";
    StormTopology topology =
        buildSimpleExclamationWithErrorsTopology(errorGeneratingComponentName);
    Config stormConfig = new Config();
    stormConfig.setDebug(true);
    final ACL clusterACE = getClusterACE();
    final String zkUser = this.getClass().getName();
    final String zkPass = "AReallyLamePassword";
    final String zkPayload = zkUser + ":" + zkPass;
    stormConfig.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, zkPayload);
    final String zkDigest =
        DigestAuthenticationProvider.generateDigest(zkPayload);
    final ACL userACE = new ACL(Perms.ALL, new Id("digest", zkDigest));
    final String topoName = "TestZkErrorNodesHaveCorrectAcls";
    try {
      cluster.submitTopology(getTopologiesJarFile(), topoName, stormConfig,
          topology);
      final String topoId = getFirstTopoIdForName(topoName);
      waitForTopoUptimeSeconds(topoId, 10);

      ZooKeeper zk = makeZkClient();
      final String stormZkRoot =
          (String) nimbusConfJson.get(Config.STORM_ZOOKEEPER_ROOT);
      List<String> errorsList = zk.getChildren(stormZkRoot + "/errors", false);
      HashSet<String> errorSet = new HashSet<String>(errorsList);
      assertTrue("Errors znode is present for topo "+topoId,
          errorSet.contains(topoId));
      List<ACL> actualACL =
          zk.getACL(stormZkRoot +"/errors/"+ topoId, new Stat());
      List<ACL> expectedACL = new ArrayList<ACL>(2);
      expectedACL.add(clusterACE);
      expectedACL.add(userACE);
      assertEquals(expectedACL, actualACL);
    } finally {
      cluster.killTopology(topoName);
    }
  }

  private ACL getClusterACE() throws Exception {
    String superACL =
        (String) nimbusConfJson.get(Config.STORM_ZOOKEEPER_SUPERACL);
    String parts[] = superACL.split(":", 2);
    ACL clusterACE = new ACL(Perms.ALL, new Id(parts[0], parts[1]));
    return clusterACE;
  }

  private ZooKeeper makeZkClient() throws Exception {
    final String host = (String) ((JSONArray) nimbusConfJson
        .get(Config.STORM_ZOOKEEPER_SERVERS)).get(0);
    final int port =
        ((Number) nimbusConfJson.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
    return new ZooKeeper(host + ":" + port, 30000, new Watcher() {
      public void process(WatchedEvent event) {
        // Do nothing.
      }
    });
  }

  private StormTopology buildSimpleExclamationTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclaimBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclaimBolt(), 2)
        .shuffleGrouping("exclaim1");
    return builder.createTopology();
  }

  private StormTopology buildSimpleExclamationWithErrorsTopology(
        String errorGeneratingComponentName) {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclaimBolt(), 3).shuffleGrouping("word");
    builder.setBolt(errorGeneratingComponentName, new ExclaimBoltWithErrors(), 2)
       .shuffleGrouping("exclaim1");

    return builder.createTopology();
  }
}
