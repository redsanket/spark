package hadooptest.storm;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hadooptest.TestSessionCore;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.cluster.storm.StormCluster;

import org.apache.log4j.Logger;

import storm.starter.ExclamationTopology_spread.ExclamationBolt;
import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
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

public class ResourceAwareSchedulerTestFuncs {

  public enum ResoureAwareTestType {
    MEMORY_AWARE, CPU_AWARE, RESOURCE_AWARE
  }

  public static void initialSetup(ModifiableStormCluster mc, String user, String pw) throws Exception{
    backtype.storm.Config theconf = new backtype.storm.Config();
    theconf.putAll(backtype.storm.utils.Utils.readStormConfig());
    String filter = (String) theconf.get("ui.filter");
    // Only get bouncer auth on secure cluster.
    if (filter != null) {
      if (mc != null) {
        user = mc.getBouncerUser();
        pw = mc.getBouncerPassword();
      }
    }
  }

  public static void assertTopology(ClusterSummary sum, String topoName, String assertion, Class clazz) {
    Logger logger = Logger.getLogger(clazz);
    for (TopologySummary topo : sum.get_topologies()) {
      if (topo.get_name().equals(topoName)) {
        logger.info("!!!!!!! topo: " + topo.get_name() + " with id: "
            + topo.get_id() + " status: " + topo.get_status() + " sched status: "
            + topo.get_sched_status() + " !!!!!!!!!");
        assertTrue("Whether topology was successfully scheduled",
            topo.get_sched_status()
                .equals(assertion) == true);
      }
    }
  }

  public static void assertTopologySuccess(ClusterSummary sum, String topoName, Class clazz) {
    assertTopology(sum, topoName, "Fully Scheduled", clazz);
  }

  public static void assertTopologyFailed(ClusterSummary sum, String topoName, Class clazz) {
    assertTopology(sum, topoName, "Unsuccessfull in scheduling topology", clazz);
  }

  public static void assertTopologiesSuccess(ClusterSummary sum, Class clazz) {
    assertTopologies(sum, "Fully Scheduled", clazz);
  }

  public static void assertTopologiesFailed(ClusterSummary sum, Class clazz) {
    assertTopologies(sum, "Unsuccessfull in scheduling topology", clazz);
  }

  public static void assertTopologies(ClusterSummary sum, String assertion, Class clazz) {
    Logger logger = Logger.getLogger(clazz);
    logger.info("!!!!!!!!begin Tests... num of topo: "
        + sum.get_topologies().size() + "!!!!!!!!!!!");
    for (TopologySummary topo : sum.get_topologies()) {
      logger.info("!!!!!!! topo: " + topo.get_name() + " with id: "
          + topo.get_id() + " status: " + topo.get_status() + " sched status: "
          + topo.get_sched_status() + " !!!!!!!!!");
      assertTrue("Whether topology was successfully scheduled",
          topo.get_sched_status()
              .equals(assertion) == true);
    }
  }

  public static Config createConfig() {
    Config config = new Config();
    config.setDebug(true);
    config.setNumWorkers(3);
    config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
    config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
    config.put(config.TOPOLOGY_ACKER_EXECUTORS, 0);
    return config;
  }

  public static StormTopology getBasicSchedulingTopology(ClusterSummary sum, ResoureAwareTestType type) {
    int numNodes = sum.get_supervisors().size();
    Map<String, Double> spoutResource = new HashMap<String, Double>();
    Map<String, Double> boltResource = new HashMap<String, Double>();
    int numSpouts=0;
    int numBolts=0;
    int spout_parallelism=0;
    int bolt_parrallelism=0;
    Double totalClusterMem = 0.0;
    Double totalClusterCPU = 0.0;
    //get total resource capacity
    for (SupervisorSummary sup : sum.get_supervisors()) {
      totalClusterMem += 20480.0;// hack for now
      totalClusterCPU += 100.0;// hack for now
    }

    switch (type) {
      case MEMORY_AWARE:
        spoutResource.put("cpu", 0.0);
        spoutResource.put("memory", totalClusterMem / 40.0);
        boltResource.put("cpu", 0.0);
        boltResource.put("memory", totalClusterMem / 40.0);
        numSpouts = 2;
        numBolts = 4;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        break;
      case CPU_AWARE:
        spoutResource.put("cpu", totalClusterCPU/20.0);
        spoutResource.put("memory", 0.0);
        boltResource.put("cpu", totalClusterCPU/20.0);
        boltResource.put("memory", 0.0);
        numSpouts = 2;
        numBolts = 4;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        break;
      case RESOURCE_AWARE:
        spoutResource.put("cpu", totalClusterCPU/20.0);
        spoutResource.put("memory", totalClusterMem / 40.0);
        boltResource.put("cpu", totalClusterCPU/20.0);
        boltResource.put("memory", totalClusterMem / 40.0);
        numSpouts = 2;
        numBolts = 4;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        break;
      default:
        return null;
    }
    StormTopology topology = buildTopology(numSpouts, numBolts,
        spout_parallelism, bolt_parrallelism, spoutResource, boltResource);
    return topology;
  }

  public static StormTopology getOverCapacityTopology(ClusterSummary sum, ResoureAwareTestType type) {
    int numNodes = sum.get_supervisors().size();
    Map<String, Double> spoutResource = new HashMap<String, Double>();
    Map<String, Double> boltResource = new HashMap<String, Double>();
    int numSpouts=0;
    int numBolts=0;
    int spout_parallelism=0;
    int bolt_parrallelism=0;
    Double totalClusterMem = 0.0;
    Double totalClusterCPU = 0.0;
    Double memory_per_comp = 0.0;
    Double cpu_per_comp = 0.0;
    
    //get total resource capacity
    for (SupervisorSummary sup : sum.get_supervisors()) {
      totalClusterMem += 20480.0;// hack for now
      totalClusterCPU += 100.0;// hack for now
    }

    switch (type) {
      case MEMORY_AWARE:
        numSpouts = 5;
        numBolts = 6;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        memory_per_comp = totalClusterMem / 20.0;
        spoutResource.put("cpu", 0.0);
        spoutResource.put("memory", memory_per_comp);
        boltResource.put("cpu", 0.0);
        boltResource.put("memory", memory_per_comp);
        break;
      case CPU_AWARE:
        numSpouts = 5;
        numBolts = 6;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        cpu_per_comp = totalClusterCPU / 20.0;
        spoutResource = new HashMap<String, Double>();
        spoutResource.put("cpu", cpu_per_comp);
        spoutResource.put("memory", 0.0);
        boltResource = new HashMap<String, Double>();
        boltResource.put("cpu", cpu_per_comp);
        boltResource.put("memory", 0.0);
        break;
      case RESOURCE_AWARE:
        numSpouts = 5;
        numBolts = 6;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        memory_per_comp = totalClusterMem / 20.0;
        cpu_per_comp = totalClusterCPU / 20.0;
        spoutResource.put("cpu", cpu_per_comp);
        spoutResource.put("memory", memory_per_comp);
        boltResource.put("cpu", cpu_per_comp);
        boltResource.put("memory", memory_per_comp);
        break;
      default:
      return null;
    }
    StormTopology topology = buildTopology(numSpouts, numBolts,
        spout_parallelism, bolt_parrallelism, spoutResource, boltResource);
    return topology;
  }

  public static StormTopology getFaultToleranceTopology(ClusterSummary sum, ResoureAwareTestType type) {
    int numNodes = sum.get_supervisors().size();
    Map<String, Double> spoutResource = new HashMap<String, Double>();
    Map<String, Double> boltResource = new HashMap<String, Double>();
    int numSpouts=0;
    int numBolts=0;
    int spout_parallelism=0;
    int bolt_parrallelism=0;
    Double totalClusterMem = 0.0;
    Double totalClusterCPU = 0.0;
    Double memory_per_comp = 0.0;
    Double cpu_per_comp = 0.0;
    //get total resource capacity
    for (SupervisorSummary sup : sum.get_supervisors()) {
      totalClusterMem += 20480.0;// hack for now
      totalClusterCPU += 100.0;// hack for now
    }

    switch (type) {
      case MEMORY_AWARE:
        numSpouts = 10;
        numBolts = 8;
        spout_parallelism = 1;
        bolt_parrallelism = 1;
        memory_per_comp = totalClusterMem/20;
        spoutResource.put("cpu", 0.0);
        spoutResource.put("memory", memory_per_comp);
        boltResource.put("cpu", 0.0);
        boltResource.put("memory", memory_per_comp);
        break;
      case CPU_AWARE:
        numSpouts = ((totalClusterCPU).intValue()/10)/2;
        numBolts = ((totalClusterCPU).intValue()/10)/2;
        spout_parallelism = 1;
        bolt_parrallelism = 1;
        cpu_per_comp = 10.0;
        spoutResource = new HashMap<String, Double>();
        spoutResource.put("cpu", cpu_per_comp);
        spoutResource.put("memory", 0.0);
        boltResource = new HashMap<String, Double>();
        boltResource.put("cpu", cpu_per_comp);
        boltResource.put("memory", 0.0);
        break;
      case RESOURCE_AWARE:
        numSpouts = 5;
        numBolts = 4;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        memory_per_comp = totalClusterMem / 20.0;
        cpu_per_comp = totalClusterCPU / 20.0;
        spoutResource.put("cpu", cpu_per_comp);
        spoutResource.put("memory", memory_per_comp);
        boltResource.put("cpu", cpu_per_comp);
        boltResource.put("memory", memory_per_comp);
        break;
      default:
      return null;
    }
    StormTopology topology = buildTopology(numSpouts, numBolts,
        spout_parallelism, bolt_parrallelism, spoutResource, boltResource);
    return topology;
  }

  public static StormTopology getHardcoreFaultToleranceTopology(ClusterSummary sum, ResoureAwareTestType type) {
    int numNodes = sum.get_supervisors().size();
    Map<String, Double> spoutResource = new HashMap<String, Double>();
    Map<String, Double> boltResource = new HashMap<String, Double>();
    int numSpouts=0;
    int numBolts=0;
    int spout_parallelism=0;
    int bolt_parrallelism=0;
    Double totalClusterMem = 0.0;
    Double totalClusterCPU = 0.0;
    Double memory_per_comp = 0.0;
    Double cpu_per_comp = 0.0;
    //get total resource capacity
    for (SupervisorSummary sup : sum.get_supervisors()) {
      totalClusterMem += 20480.0;// hack for now
      totalClusterCPU += 100.0;// hack for now
    }

    switch (type) {
      case RESOURCE_AWARE:
        numSpouts = 5;
        numBolts = 4;
        spout_parallelism = 2;
        bolt_parrallelism = 2;
        memory_per_comp = totalClusterMem / 200.0;
        cpu_per_comp = totalClusterCPU / 200.0;
        spoutResource.put("cpu", cpu_per_comp);
        spoutResource.put("memory", memory_per_comp);
        boltResource.put("cpu", cpu_per_comp);
        boltResource.put("memory", memory_per_comp);
        break;
      default:
      return null;
    }
    StormTopology topology = buildTopology(numSpouts, numBolts,
        spout_parallelism, bolt_parrallelism, spoutResource, boltResource);
    return topology;
  }

  public static List<StormTopology> getSmallTopologies(ClusterSummary sum, ResoureAwareTestType type, int num) {
    int numNodes = sum.get_supervisors().size();
    Map<String, Double> spoutResource = new HashMap<String, Double>();
    Map<String, Double> boltResource = new HashMap<String, Double>();
    List<StormTopology> retList = new ArrayList<StormTopology>();
    int numSpouts=0;
    int numBolts=0;
    int spout_parallelism=0;
    int bolt_parrallelism=0;
    Double totalClusterMem = 0.0;
    Double totalClusterCPU = 0.0;
    Double memory_per_comp = 0.0;
    Double cpu_per_comp = 0.0;
    //get total resource capacity
    for (SupervisorSummary sup : sum.get_supervisors()) {
      totalClusterMem += 20480.0;// hack for now
      totalClusterCPU += 100.0;// hack for now
    }
    Double memory_per_topo = totalClusterMem/num;
    Double cpu_per_topo = totalClusterCPU/num;
    while (retList.size()<num) {
      switch (type) {
        case CPU_AWARE:
          numSpouts = 5;
          numBolts = 4;
          spout_parallelism = 2;
          bolt_parrallelism = 2;
          cpu_per_comp = cpu_per_topo / 20.0;
          spoutResource.put("cpu", cpu_per_comp);
          spoutResource.put("memory", 0.0);
          boltResource.put("cpu", cpu_per_comp);
          boltResource.put("memory", 0.0);
          break;
        case MEMORY_AWARE:
          numSpouts = 5;
          numBolts = 4;
          spout_parallelism = 2;
          bolt_parrallelism = 2;
          memory_per_comp = memory_per_topo / 20.0;
          spoutResource.put("cpu", 0.0);
          spoutResource.put("memory", memory_per_comp);
          boltResource.put("cpu", 0.0);
          boltResource.put("memory", memory_per_comp);
          break;
        case RESOURCE_AWARE:
          numSpouts = 5;
          numBolts = 4;
          spout_parallelism = 2;
          bolt_parrallelism = 2;
          memory_per_comp = memory_per_topo / 20.0;
          cpu_per_comp = cpu_per_topo / 20.0;
          spoutResource.put("cpu", cpu_per_comp);
          spoutResource.put("memory", memory_per_comp);
          boltResource.put("cpu", cpu_per_comp);
          boltResource.put("memory", memory_per_comp);
          break;
        default:
        return null;
      }
    
      StormTopology topology = buildTopology(numSpouts, numBolts,
          spout_parallelism, bolt_parrallelism, spoutResource, boltResource);
      retList.add(topology);
    }
    return retList;
  }

  public static StormTopology buildTopology(int numSpout, int numBolt,
      int spoutParallelism, int boltParallelism,
      Map<String, Double> spoutResourceReq, Map<String, Double> boltResourceReq) {
    TopologyBuilder builder = new TopologyBuilder();

    for (int i = 0; i < numSpout; i++) {
      SpoutDeclarer s1 = builder.setSpout("spout-" + i, new TestWordSpout(),
          spoutParallelism);
      s1.setCPULoad(spoutResourceReq.get("cpu"));
      s1.setMemoryLoad(spoutResourceReq.get("memory") / 2.0,
          spoutResourceReq.get("memory") / 2.0);
    }
    int j = 0;
    for (int i = 0; i < numBolt; i++) {
      if (j >= numSpout) {
        j = 0;
      }
      BoltDeclarer b1 = builder.setBolt("bolt-" + i, new TestBolt(),
          boltParallelism).shuffleGrouping("spout-" + j);
      b1.setCPULoad(boltResourceReq.get("cpu"));
      b1.setMemoryLoad(boltResourceReq.get("memory") / 2.0,
          boltResourceReq.get("memory") / 2.0);
    }

    return builder.createTopology();
  }

  public static void killAllTopologies(List<TopologySummary> topos, StormCluster cluster, Class clazz) throws Exception{
    Logger logger = Logger.getLogger(clazz);
    for (TopologySummary topo : topos) {
      logger.info("killing topology " + topo.get_name());
      KillOptions opt =new KillOptions();
      opt.set_wait_secs(0);
      cluster.killTopology(topo.get_name(), opt);
    }
    Utils.sleep(10000);
  }

  public static void killAllTopologies_multiTheaded(List<TopologySummary> topos, StormCluster cluster, Class clazz) throws Exception{
    for (TopologySummary topo : topos) {
      ResourceAwareSchedulerTestFuncs outer = new ResourceAwareSchedulerTestFuncs();
      KillTopologyThread t = outer.new KillTopologyThread(topo, cluster, clazz);
      t.start();
    }
  }

  public static class TestBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context,
        OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      //_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  class KillTopologyThread implements Runnable {
    private TopologySummary _topology;
    private StormCluster _cluster;
    private Logger logger;
    private Thread t;
    
    public KillTopologyThread(TopologySummary topology, StormCluster cluster, Class clazz) {
      _topology=topology;
      _cluster=cluster;
      logger = Logger.getLogger(clazz);
    }
    public void run(){
      logger.info("killing topology " + _topology.get_name());
      KillOptions opt =new KillOptions();
      opt.set_wait_secs(0);
      try {
        _cluster.killTopology(_topology.get_name(), opt);
      } catch (Exception ex) {
        logger.info(ex);
      }
    }

    public void start ()
    {
       if (t == null)
       {
          t = new Thread (this);
          t.start ();
       }
    }
  }
}
