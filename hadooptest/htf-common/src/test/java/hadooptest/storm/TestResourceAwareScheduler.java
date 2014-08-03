package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.cluster.storm.StormDaemon;
import hadooptest.storm.ResourceAwareSchedulerTestFuncs.ResoureAwareTestType;
import hadooptest.workflow.storm.topology.bolt.Split;
import hadooptest.workflow.storm.topology.spout.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;



@Category(SerialTests.class)
public class TestResourceAwareScheduler extends TestSessionStorm {
  static ModifiableStormCluster mc;
  private static final int MIN_NODES_FOR_TEST = 2;
  private static final long LONGEVITY_RUNTIME = 300000; //5 MIN

  @BeforeClass
  public static void setup() throws Exception {
    assumeTrue(cluster instanceof ModifiableStormCluster);
    mc = (ModifiableStormCluster) cluster;
    logger.info("Setting to use Mulit-tenant Resource Aware scheduler");
    mc.setConf("storm.scheduler",
        "backtype.storm.scheduler.multitenant.MultitenantScheduler");
    mc.setConf("supervisor.memory.capacity.mb", 20480.0);
    mc.setConf("supervisor.cpu.capacity", 100.0);
    logger.info("Restart Nimbus");
    mc.restartCluster();
  }

  @AfterClass
  public static void cleanup() throws Exception {
      if (mc != null) {
        mc.resetConfigsAndRestart();
      }
      stop();
  }

  /**
   * Submit topologies with the aggregate Resource requirement 
   * MUCH less than the capacity of the cluster, 
   * test whether all executors have been scheduled. 
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testBasicScheduling() throws Exception {
    //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >=MIN_NODES_FOR_TEST);

    //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

    //Get topology for test
    StormTopology topology= ResourceAwareSchedulerTestFuncs
        .getBasicSchedulingTopology(sum, ResoureAwareTestType.RESOURCE_AWARE);

    //more configurations
    String topoName = "topology-testBasicScheduling";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    //Submit topology
    cluster.submitTopology(jar, topoName, config, topology);

     //Wait for topology submittion to complete
    Utils.sleep(10000);

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
  }

  /**
   * Submit a topology with a total Resource requirement larger 
   * than the capacity of the cluster, test that the scheduling 
   * GRACEFULLY failed and no executors have been scheduled. 
   * Increase the number of nodes in the cluster until the cluster 
   * has enough resource capacity to accomodate the topology and 
   * test is the topology is successfully scheduled.
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testOverCapacityScheduling() throws Exception {
    //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >=MIN_NODES_FOR_TEST);

  //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

  //kill supervisor
    logger.info("killing supervisor on node: "+sum.get_supervisors().get(0).get_host());
    String sup_killed = sum.get_supervisors().get(0).get_host();
    mc.stopDaemonNode(StormDaemon.SUPERVISOR, sum.get_supervisors().get(0).get_host());
    Utils.sleep(30000); //sleep for 30 secs to be safe since zookeeper 
                        //will take by default 15 secs to determine is a sup is down

  //update cluster info
    sum = cluster.getClusterInfo();

    //Get topology for test
    StormTopology topology= ResourceAwareSchedulerTestFuncs
        .getOverCapacityTopology(sum, ResoureAwareTestType.RESOURCE_AWARE);

    //more configurations
    String topoName = "topology-testOverCapacityScheduling";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    //Submit topology
    cluster.submitTopology(jar, topoName, config, topology);

     //Wait for topology submittion to complete
    Utils.sleep(10000);

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies unsuccessfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesFailed(sum, TestResourceAwareScheduler.class);

  //starting supervisor
    logger.info("Starting supervisor on node: "+sup_killed);
    mc.startDaemonNode(StormDaemon.SUPERVISOR, sup_killed);
    Utils.sleep(30000); // sleep for 30 secs 

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
  }

  /**
   * Test submitting two topologies.  Each topology will requires most of
   * the resources the cluster will have to offer which means only one topology 
   * is going to be scheduled.  Kill one of the topologies to see if the other topology
   * will correctly be scheduled once there is capacity
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testOverSubscribe() throws Exception {
  //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >=MIN_NODES_FOR_TEST);

    //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

    //Get topology for test
    StormTopology topology= ResourceAwareSchedulerTestFuncs
        .getFaultToleranceTopology(sum, ResoureAwareTestType.RESOURCE_AWARE); //reuse here since its 
                                                                         //just a topology that will consume 
                                                                        //almost all the resources of the cluster

    //more configurations
    String topoName_1 = "topology-testOverSubscribe-1";
    String topoName_2 = "topology-testOverSubscribe-2";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    //Submit topologies
    cluster.submitTopology(jar, topoName_1, config, topology);
    Utils.sleep(10000);
    cluster.submitTopology(jar, topoName_2, config, topology);
    Utils.sleep(10000);

    //update cluster info
    sum = cluster.getClusterInfo();
    //first topology successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologySuccess(sum, topoName_1, TestResourceAwareScheduler.class);
    //second topology not successfully scheduled since not enough space
    ResourceAwareSchedulerTestFuncs.assertTopologyFailed(sum, topoName_2, TestResourceAwareScheduler.class);

    //killing first topology to make room for second topology
    logger.info("killing topology " + topoName_1);
    KillOptions opt =new KillOptions();
    opt.set_wait_secs(0);
    cluster.killTopology(topoName_1, opt);
    Utils.sleep(30000);

    //update cluster info
    sum = cluster.getClusterInfo();

    //check if second topology now successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologySuccess(sum, topoName_2, TestResourceAwareScheduler.class);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
  }

  /**
   * Test fault tolerance
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testFaultTolerance() throws Exception {
    //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >= MIN_NODES_FOR_TEST);

  //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

    //Get topology for test
    StormTopology topology= ResourceAwareSchedulerTestFuncs
        .getFaultToleranceTopology(sum, ResoureAwareTestType.RESOURCE_AWARE);

    //more configurations
    String topoName = "topology-testFaultTolerance";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    //Submit topology
    cluster.submitTopology(jar, topoName, config, topology);

     //Wait for topology submittion to complete
    Utils.sleep(10000);

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //kill supervisor
    logger.info("killing supervisor on node: "+sum.get_supervisors().get(0).get_host());
    String sup_killed = sum.get_supervisors().get(0).get_host();
    mc.stopDaemonNode(StormDaemon.SUPERVISOR, sum.get_supervisors().get(0).get_host());
    Utils.sleep(30000); //sleep for 30 secs to be safe since zookeeper 
                        //will take by default 15 secs to determine is a sup is down

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies unsuccessfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesFailed(sum, TestResourceAwareScheduler.class);

    //starting supervisor
    logger.info("Starting supervisor on node: "+sup_killed);
    mc.startDaemonNode(StormDaemon.SUPERVISOR, sup_killed);
    Utils.sleep(30000); // sleep for 30 secs 

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
  }

  /**
   * Test hardcore fault tolerance. Kills all supervisors and nimbus
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testHardCoreFaultTolerance() throws Exception {
    int numTopologiesToSubmit=10;
  //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >= MIN_NODES_FOR_TEST);

  //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

    int slots=0;
    for (SupervisorSummary sup : sum.get_supervisors()) {
      slots+=sup.get_num_workers();
    }
    int numberOfTopos=(int)(slots*0.75);
    logger.info("Number of slots avail: "+slots);
    logger.info("Number of topos: "+numberOfTopos);
    
    //Get topology for test
    List<StormTopology> topos= ResourceAwareSchedulerTestFuncs
        .getSmallTopologies(sum, ResoureAwareTestType.RESOURCE_AWARE, numberOfTopos);

    //more configurations
    String topoName = "topology-testHardCoreFaultTolerance";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    
    for (int i=0; i<topos.size(); i++) {
      logger.info("submitting topology: "+topoName+"-"+i);
      cluster.submitTopology(jar, topoName+"-"+i, config, topos.get(i));
    }
     //Wait for topology submittion to complete
    Utils.sleep(10000);

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    List<String> sups_killed = new ArrayList<String>();
    //kill supervisors
    for(SupervisorSummary sup : sum.get_supervisors()) {
      logger.info("killing supervisor on node: "+sup.get_host());
      sups_killed.add(sup.get_host());
      mc.stopDaemonNode(StormDaemon.SUPERVISOR, sup.get_host());
    }

    Utils.sleep(30000); //sleep for 30 secs to be safe since zookeeper 
                        //will take by default 15 secs to determine is a sup is down

    //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies failed to schedule
    ResourceAwareSchedulerTestFuncs.assertTopologiesFailed(sum, TestResourceAwareScheduler.class);

    //restart nimbus
    mc.restartDaemon(StormDaemon.NIMBUS);
    Utils.sleep(60000); 

    //start supervisors
    for(String sup : sups_killed) {
      logger.info("starting supervisor on node: "+sup);
      mc.startDaemonNode(StormDaemon.SUPERVISOR, sup);
    }

    Utils.sleep(60000*2); //sleep for 30 secs to be safe since zookeeper 
    //will take by default 15 secs to determine is a sup is down

  //update cluster info
    sum = cluster.getClusterInfo();

    //assert topologies successfully scheduled
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);
  }

  /**
   * Creating a bunch of topologies and then
   * kill all of them in a concurrent multi-threaded fashion
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void concurrentTopologyKill() throws Exception {
    //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();

    int slots=0;
    for (SupervisorSummary sup : sum.get_supervisors()) {
      slots+=sup.get_num_workers();
    }
    int numberOfTopos=(int)(slots*0.75);
    logger.info("Number of slots avail: "+slots);
    logger.info("Number of topos: "+numberOfTopos);
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >=MIN_NODES_FOR_TEST);

    //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestResourceAwareScheduler.class);

    //Get topology for test
    List <StormTopology> topos= ResourceAwareSchedulerTestFuncs
        .getSmallTopologies(sum, ResoureAwareTestType.RESOURCE_AWARE, numberOfTopos);

  //more configurations
    String topoName = "topology-concurrentTopologyKill";

    //Create the configurations
    Config config = ResourceAwareSchedulerTestFuncs.createConfig();

    // TODO turn this into a utility that has a conf setting
    File jar = new File(
        conf.getProperty("WORKSPACE")
            + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");

    for (int i=0; i<topos.size(); i++) {
      logger.info("submitting topology: "+topoName+"-"+i);
      cluster.submitTopology(jar, topoName+"-"+i, config, topos.get(i));
    }
    Utils.sleep(10000);

    sum = cluster.getClusterInfo();
    ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestResourceAwareScheduler.class);

    //kill all topologies concurrently using threads
    ResourceAwareSchedulerTestFuncs.killAllTopologies_multiTheaded(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
    Utils.sleep(10000);

    
    //assert all topologies have been killed
    sum = cluster.getClusterInfo();
    assertTrue("No topologies left", sum.get_topologies().size()==0);

    //cleanup
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestResourceAwareScheduler.class);
  }

  /**
   * Tests the spread scheduler.  Schedule a topology with components that need to be
   * spread across multiple machines.  Kill a certain number of machines so that the cluster 
   * cannot accommodate the spread topology.  Check is unsuccessfully scheduled.  Bring the nodes back up
   * and check if the topology got correctly rescheduled to run successfully
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testSpreadTopology() throws Exception {
  //initial setup for secure cluster if necessary
    ResourceAwareSchedulerTestFuncs.initialSetup(mc, null, null);

    //Get cluster info
    ClusterSummary sum = cluster.getClusterInfo();
    //make sure enough nodes for testing to run
    assertTrue("There are nodes we can use in cluster",  sum.get_supervisors().size() >=MIN_NODES_FOR_TEST);

    //kill all left over topologies
    ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), cluster, TestCPUAwareScheduling.class);

    sum = cluster.getClusterInfo();
    int spreadSize = sum.get_supervisors().size();
    logger.info("number of supervisors: "+sum.get_supervisors().size());
    String topoName = "topology-testSpreadTopology";
    //Get topology for test
    TopologyBuilder builder = new TopologyBuilder();
    SpoutDeclarer s1 = builder.setSpout("spout-1", new TestWordSpout(), 5);
    BoltDeclarer b1 = builder.setBolt("bolt-1", new ResourceAwareSchedulerTestFuncs.TestBolt(), spreadSize);

    BoltDeclarer b2 = builder.setBolt("bolt-2", new ResourceAwareSchedulerTestFuncs.TestBolt(), spreadSize);
    b1.shuffleGrouping("spout-1");
    b2.shuffleGrouping("bolt-1");
     
     Config config = new Config();
     config.setDebug(true);
     config.setNumWorkers(3);
     config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
     config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
     config.put(config.TOPOLOGY_ACKER_EXECUTORS, 0);
     List<String>comp = new ArrayList<String>();
     comp.add("bolt-1");
     comp.add("bolt-2");
     config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, comp);

     // TODO turn this into a utility that has a conf setting
     File jar = new File(
         conf.getProperty("WORKSPACE")
             + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
     
     cluster.submitTopology(jar, topoName, config, builder.createTopology());
     Utils.sleep(10000);

     sum = cluster.getClusterInfo();
     ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestCPUAwareScheduling.class);

   //kill supervisor
     logger.info("killing supervisor on node: "+sum.get_supervisors().get(0).get_host());
     String sup_killed = sum.get_supervisors().get(0).get_host();
     mc.stopDaemonNode(StormDaemon.SUPERVISOR, sum.get_supervisors().get(0).get_host());
     Utils.sleep(30000); //sleep for 30 secs to be safe since zookeeper 
                         //will take by default 15 secs to determine is a sup is down
     sum = cluster.getClusterInfo();
     ResourceAwareSchedulerTestFuncs.assertTopologyFailed(sum, topoName, TestCPUAwareScheduling.class);

   //starting supervisor
     logger.info("Starting supervisor on node: "+sup_killed);
     mc.startDaemonNode(StormDaemon.SUPERVISOR, sup_killed);
     Utils.sleep(30000); // sleep for 30 secs 

     //update cluster info
     sum = cluster.getClusterInfo();

     //assert topologies successfully scheduled
     ResourceAwareSchedulerTestFuncs.assertTopologiesSuccess(sum, TestCPUAwareScheduling.class);

     //cleanup
     ResourceAwareSchedulerTestFuncs.killAllTopologies(sum.get_topologies(), mc, TestCPUAwareScheduling.class);
  }
}
