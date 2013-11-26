package org.apache.hadoop.mapreduce;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

public class TestBackwardCompatibility extends ClusterMapReduceTestCase{

  private static final Log LOG = LogFactory.getLog(
      TestBackwardCompatibility.class.getName());

  private String twentyJobConfigValue;
  private String twentyTwoJobConfigValue;
  private String twentyTaskConfigValue;
  private String twentyTwoTaskConfigValue;
  private String twentyMapConfigValue;
  private String twentyTwoMapConfigValue;
  private String twentyReduceConfigValue;
  private String twentyTwoReduceConfigValue;
  private String twentyJobClientConfigValue;
  private String twentyTwoJobClientConfigValue;
 

  @Test
  public void testJobRelatedBC() throws Exception{
    Configuration conf = createJobConf();

    String [] twentyJobConfigKeys = {"mapred.map.tasks", "mapred.max.tracker.failures", "mapred.reduce.slowstart.completed.maps", "mapred.reduce.tasks", "job.local.dir", "mapreduce.inputformat.class", "mapreduce.map.class", "mapreduce.combine.class", "mapreduce.reduce.class", "mapreduce.outputformat.class", "mapreduce.partitioner.class", "mapred.job.classpath.archives", "mapred.job.classpath.files", "mapred.job.queue.name", "mapred.job.name"};

    String [] twentyTwoJobConfigKeys = {"mapreduce.job.maps", "mapreduce.job.maxtaskfailures.per.tracker", "mapreduce.job.reduce.slowstart.completedmaps", "mapreduce.job.reduces", "mapreduce.job.local.dir", "mapreduce.job.inputformat.class", "mapreduce.job.map.class", "mapreduce.job.combine.class", "mapreduce.job.reduce.class", "mapreduce.job.outputformat.class", "mapreduce.job.partitioner.class", "mapreduce.job.classpath.archives", "mapreduce.job.classpath.files", "mapreduce.job.queuename", "mapreduce.job.name"};

    String [] twentyTaskConfigKeys = {"io.sort.factor", "io.sort.mb", "mapred.child.tmp", "mapred.task.id", "mapred.task.is.map", "mapred.task.partition", "mapred.task.timeout", "mapred.tip.id", "mapred.work.output.dir"};

    String [] twentyTwoTaskConfigKeys = {"mapreduce.task.io.sort.factor", "mapreduce.task.io.sort.mb", "mapreduce.task.tmp.dir", "mapreduce.task.attempt.id", "mapreduce.task.ismap", "mapreduce.task.partition", "mapreduce.task.timeout", "mapreduce.task.id", "mapreduce.task.output.dir"};

    String [] twentyMapConfigKeys = {"io.sort.spill.percent", "map.input.file", "map.input.length", "map.input.start", "mapred.job.map.memory.mb", "mapred.map.max.attempts", "mapred.map.tasks.speculative.execution", "mapred.max.map.failures.percent", "mapred.compress.map.output", "mapred.map.output.compression.codec", "mapred.mapoutput.key.class", "mapred.mapoutput.value.class", "map.output.key.field.separator"};

    String [] twentyTwoMapConfigKeys = {"mapreduce.map.sort.spill.percent", "mapreduce.map.input.file", "mapreduce.map.input.length", "mapreduce.map.input.start", "mapreduce.map.memory.mb", "mapreduce.map.maxattempts", "mapreduce.map.speculative", "mapreduce.map.failures.maxpercent", "mapreduce.map.output.compress", "mapreduce.map.output.compress.codec", "mapreduce.map.output.key.class", "mapreduce.map.output.value.class", "mapreduce.map.output.key.field.separator"};

    String [] twentyReduceConfigKeys = {"mapred.job.reduce.memory.mb", "mapred.job.reduce.total.mem.bytes", "mapred.max.reduce.failures.percent", "mapred.reduce.max.attempts", "mapred.reduce.tasks.speculative.execution", "mapred.shuffle.connect.timeout", "mapred.shuffle.read.timeout"};

    String [] twentyTwoReduceConfigKeys = {"mapreduce.reduce.memory.mb", "mapreduce.reduce.memory.totalbytes", "mapreduce.reduce.failures.maxpercent", "mapreduce.reduce.maxattempts", "mapreduce.reduce.speculative", "mapreduce.reduce.shuffle.connect.timeout", "mapreduce.reduce.shuffle.read.timeout"};

    String [] twentyJobClientConfigKeys = {"jobclient.output.filter", "mapred.submit.replication", "user.name", "jobclient.completion.poll.interval", "jobclient.progress.monitor.poll.interval"};

    String [] twentyTwoJobClientConfigKeys = {"mapreduce.client.output.filter", "mapreduce.client.submit.file.replication", "mapreduce.client.username", "mapreduce.client.completion.poll-interval", "mapreduce.client.progerss-monitor.poll-interval"};
    
    assertEquals(twentyJobConfigKeys.length, twentyTwoJobConfigKeys.length);
    assertEquals(twentyTaskConfigKeys.length, twentyTwoTaskConfigKeys.length);
    assertEquals(twentyMapConfigKeys.length, twentyTwoMapConfigKeys.length);
    assertEquals(twentyReduceConfigKeys.length, twentyTwoReduceConfigKeys.length);
    assertEquals(twentyJobClientConfigKeys.length, twentyTwoJobClientConfigKeys.length);

    for(int i=0;i<twentyJobConfigKeys.length;i++){
      twentyJobConfigValue = conf.get(twentyJobConfigKeys[i]);
      twentyTwoJobConfigValue = conf.get(twentyTwoJobConfigKeys[i]);
      LOG.info("TWENTY JOB CONFIG KEY" + "\t" + twentyJobConfigKeys[i] + "\t" + twentyJobConfigValue); 
      LOG.info("TWENTY TWO JOB CONFIG KEY" + "\t" + twentyTwoJobConfigKeys[i] + "\t" + twentyTwoJobConfigValue); 
      if(twentyJobConfigValue == null || twentyTwoJobConfigValue == null){
        assertNull(twentyJobConfigValue);
        assertNull(twentyTwoJobConfigValue);
      }
      else{
         assertTrue(twentyJobConfigValue.equals(twentyTwoJobConfigValue));
      }
    } 

    for(int i=0;i<twentyTaskConfigKeys.length;i++){
      twentyTaskConfigValue = conf.get(twentyTaskConfigKeys[i]);
      twentyTwoTaskConfigValue = conf.get(twentyTwoTaskConfigKeys[i]);
      LOG.info("TWENTY TASK CONFIG KEY" + "\t" + twentyTaskConfigKeys[i] + "\t" + twentyTaskConfigValue); 
      LOG.info("TWENTY TWO TASK CONFIG KEY" + "\t" + twentyTwoTaskConfigKeys[i] + "\t" + twentyTwoTaskConfigValue); 
      if(twentyTaskConfigValue == null || twentyTwoTaskConfigValue == null){
         assertNull(twentyTaskConfigValue);
         assertNull(twentyTwoTaskConfigValue);
      }
      else{
         assertTrue(twentyTaskConfigValue.equals(twentyTwoTaskConfigValue));
      }
    } 

    for(int i=0;i<twentyMapConfigKeys.length;i++){
      twentyMapConfigValue = conf.get(twentyMapConfigKeys[i]);
      twentyTwoMapConfigValue = conf.get(twentyTwoMapConfigKeys[i]);
      LOG.info("TWENTY MAP CONFIG KEY" + "\t" + twentyMapConfigKeys[i] + "\t" + twentyMapConfigValue); 
      LOG.info("TWENTY TWO MAP CONFIG KEY" + "\t" + twentyTwoMapConfigKeys[i] + "\t" + twentyTwoMapConfigValue); 
      if(twentyMapConfigValue == null || twentyTwoMapConfigValue == null){
         assertNull(twentyMapConfigValue);
         assertNull(twentyTwoMapConfigValue);
      }
      else{
         assertTrue(twentyMapConfigValue.equals(twentyTwoMapConfigValue));
      }
    } 

    for(int i=0;i<twentyReduceConfigKeys.length;i++){
      twentyReduceConfigValue = conf.get(twentyReduceConfigKeys[i]);
      twentyTwoReduceConfigValue = conf.get(twentyTwoReduceConfigKeys[i]);
      LOG.info("TWENTY REDUCE CONFIG KEY" + "\t" + twentyReduceConfigKeys[i] + "\t" + twentyReduceConfigValue); 
      LOG.info("TWENTY TWO REDUCE CONFIG KEY" + "\t" + twentyTwoReduceConfigKeys[i] + "\t" + twentyTwoReduceConfigValue); 
      if(twentyReduceConfigValue == null || twentyTwoReduceConfigValue == null){
         assertNull(twentyReduceConfigValue);
         assertNull(twentyTwoReduceConfigValue);
      }
      else{
         assertTrue(twentyReduceConfigValue.equals(twentyTwoReduceConfigValue));
      }
    } 

    for(int i=0;i<twentyJobClientConfigKeys.length;i++){
      twentyJobClientConfigValue = conf.get(twentyJobClientConfigKeys[i]);
      twentyTwoJobClientConfigValue = conf.get(twentyTwoJobClientConfigKeys[i]);
      LOG.info("TWENTY MAP CONFIG KEY" + "\t" + twentyJobClientConfigKeys[i] + "\t" + twentyJobClientConfigValue); 
      LOG.info("TWENTY TWO MAP CONFIG KEY" + "\t" + twentyTwoJobClientConfigKeys[i] + "\t" + twentyTwoJobClientConfigValue); 
      if(twentyJobClientConfigValue == null || twentyTwoJobClientConfigValue == null){
         assertNull(twentyJobClientConfigValue);
         assertNull(twentyTwoJobClientConfigValue);
      }
      else{
         assertTrue(twentyJobClientConfigValue.equals(twentyTwoJobClientConfigValue));
      }
    } 
  }
}
