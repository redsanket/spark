package hadooptest.tez.ats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

import hadooptest.TestSession;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

public class CaptiveOrderedWordCount extends OrderedWordCountExtendedForHtf {

	public boolean run(String inputPath, String outputPath, Configuration conf,
		      int numPartitions, String mode, Session session, TimelineServer timelineServer, String testName, UserGroupInformation ugi)
			throws Exception {
	    TestSession.logger.info("Running OrderedWordCount");
	    TezConfiguration tezConf;
	    if (conf != null) {
	      tezConf = new TezConfiguration(conf);
	    } else {
	      tezConf = (TezConfiguration) HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName);
	    }
	    
	    UserGroupInformation.setConfiguration(tezConf);
	    UserGroupInformation.setLoginUser(ugi);
	    TezClient tezClient = TezClient.create("OrderedWordCount", tezConf);
	    tezClient.start();

	    try {
	        DAG dag = createDAG(tezConf, inputPath, outputPath, numPartitions, "OrderedWordCount");

	        tezClient.waitTillReady();
	        DAGClient dagClient = tezClient.submitDAG(dag);

	        // monitoring
	        DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          System.out.println("OrderedWordCount failed with diagnostics: " + dagStatus.getDiagnostics());
	          return false;
	        }
	        return true;
	    } finally {
	      tezClient.stop();
	    }

	}

}
