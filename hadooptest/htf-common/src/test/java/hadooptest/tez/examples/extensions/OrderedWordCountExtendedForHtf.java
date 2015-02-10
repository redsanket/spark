package hadooptest.tez.examples.extensions;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import hadooptest.TestSession;
import hadooptest.tez.ats.SeedData;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.examples.OrderedWordCount;

/**
 * These classes ending in *ExtendedForTezHTF are intermediate classes, that
 * live between the class that is distributed with original Tez JAR and the
 * actual test class (that has the @Test implementations) Since the Tez classes
 * sent out with the distribution 'extends Configured implements Tool' they are
 * designed to be invoked directly from the command line. When invoked from the
 * Command line, the run(String[] args) method [from the Tool class] gets
 * invoked. It parses the arguments and subsequently calls run(...) arguments,
 * that has the main body of the functionality. Since this 'run' method creates
 * Configuration objects (and that is where we toggle if this runs on local mode
 * or cluster mode) we need to overtide (copy/paste) that run method here and
 * override the getConf() method calls with {@code}
 * HtfTezUtils.setupConfForTez(conf, mode)
 * 
 * That would set up the local/cluster mode correctly.
 * 
 * NOTE: Changes to be done, should the file need to be refreshed
 * <p>
 * 1) Ensure that all references to getConf fetch a configuration instance from
 * {@link} HtfTezUtils
 * </p>
 */

public class OrderedWordCountExtendedForHtf extends OrderedWordCount {
	/**
	 * Copy and paste the the code from the parent class's run method here.
	 * Change all references to getConf() to HtfTezUtils.setupConfForTez(conf,
	 * mode) Note: Be careful, there could be several run methods there, for
	 * example those contained inside a Processor, or that overriding the method
	 * in the Tool class.
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */

	public boolean run(String inputPath, String outputPath, Configuration conf,
		      int numPartitions, String mode, Session session, TimelineServer timelineServer, String testName)
			throws Exception {
	    TestSession.logger.info("Running OrderedWordCount");
	    TezConfiguration tezConf;
	    if (conf != null) {
	      tezConf = new TezConfiguration(conf);
	    } else {
	      tezConf = (TezConfiguration) HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName);
	    }
	    
	    UserGroupInformation.setConfiguration(tezConf);
	    
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
	/**
	 * Needed by HTF.
	 * The following code is used for ATS testing, wher we need to pass the UGI as an additional parameter
	 * @param inputPath
	 * @param outputPath
	 * @param conf
	 * @param numPartitions
	 * @param mode
	 * @param session
	 * @param timelineServer
	 * @param testName
	 * @param ugi
	 * @return
	 * @throws Exception
	 */
	public boolean run(String inputPath, String outputPath, Configuration conf,
		      int numPartitions, String mode, Session session, TimelineServer timelineServer, String testName, UserGroupInformation ugi, 
		      SeedData seedData, String acls) throws Exception {
	    TestSession.logger.info("Running OrderedWordCount");
	    TezConfiguration tezConf;
	    if (conf != null) {
	      tezConf = new TezConfiguration(conf);
	    } else {
	      tezConf = (TezConfiguration) HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName);
	    }
	    /**
	     * HTF: set the seed data and ACLs
	     */
	    UserGroupInformation.setConfiguration(tezConf);
	    UserGroupInformation.setLoginUser(ugi);
	    tezConf.set("tez.am.dag.view-acls", acls);
	    
	    TezClient tezClient = TezClient.create("OrderedWordCount", tezConf);
	    tezClient.start();

	    try {
	        DAG dag = createDAG(tezConf, inputPath, outputPath, numPartitions, "DAG-" + testName +"-" + ugi.getUserName());
	        tezClient.waitTillReady();
	        DAGClient dagClient = tezClient.submitDAG(dag);	        
	        // monitoring
	        DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
	        populateSeedData(dag, seedData, tezClient);
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          System.out.println("OrderedWordCount failed with diagnostics: " + dagStatus.getDiagnostics());
	          return false;
	        }
	        return true;
	    } finally {
	      tezClient.stop();
	    }

	}
	
	/**
	 * HTF
	 * Seed data 
	 */
	void populateSeedData(DAG aDag, SeedData seedData, TezClient tezClient){
		seedData.appId = tezClient.getAppMasterApplicationId().toString();
		SeedData.DAG seedDag = new SeedData.DAG();
		seedDag.name = aDag.getName();
		for (Vertex aVertex:aDag.getVertices()){
			SeedData.DAG.Vertex seedVertex = new SeedData.DAG.Vertex();
			seedVertex.name = aVertex.getName();			
			seedDag.vertices.add(seedVertex);
		}
		seedData.dags.add(seedDag);
	}

}