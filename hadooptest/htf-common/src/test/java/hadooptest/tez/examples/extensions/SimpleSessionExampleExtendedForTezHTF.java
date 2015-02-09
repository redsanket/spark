package hadooptest.tez.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.ats.SeedData;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.examples.SimpleSessionExample;

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
 */
public class SimpleSessionExampleExtendedForTezHTF extends SimpleSessionExample {

	/**
	 * Copy and paste the the code from the parent class's run method here.
	 * Change all references to getConf() to HtfTezUtils.setupConfForTez(conf,
	 * mode) Note: Be careful, there could be several run methods there, for
	 * example those contained inside a Processor, or that overriding the method
	 * in the Tool class.
	 * 
	 * NOTE: In this case since the run method is accepting a conf object, it is
	 * in out control to pass it as an argument from our test. This class is
	 * provided for preserving the consistency of Test Design.
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */
	public boolean run(String[] inputPaths, String[] outputPaths,
			Configuration conf, int numPartitions) throws Exception {
		return super.run(inputPaths, outputPaths, conf, numPartitions);
	}

	/**
	 * HTF Needed to pass UGI, hence overloading the run method here
	 */
	private static final String enablePrewarmConfig = "simplesessionexample.prewarm";

	public boolean run(String[] inputPaths, String[] outputPaths,
			Configuration conf, int numPartitions, UserGroupInformation ugi,
			SeedData seedData, String acls) throws Exception {
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}
		/**
		 * HTF: Set the UGI and acls
		 */
		UserGroupInformation.setConfiguration(tezConf);
		UserGroupInformation.setLoginUser(ugi);
		// tezConf.set("tez.am.dag.view-acls", acls);
		tezConf.set("tez.am.view-acls", acls);

		// start TezClient in session mode. The same code run in session mode or
		// non-session mode. The
		// mode can be changed via configuration. However if the application
		// wants to run exclusively in
		// session mode then it can do so in code directly using the appropriate
		// constructor

		// tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true); //
		// via config OR via code
		TezClient tezClient = TezClient.create(
				"SimpleSessionExample-" + ugi.getUserName(), tezConf, true);
		tezClient.start();

		// Session pre-warming allows the user to hide initial startup, resource
		// acquisition latency etc.
		// by pre-allocating execution resources in the Tez session. They can
		// run initialization logic
		// in these pre-allocated resources (containers) to pre-warm the
		// containers.
		// In between DAG executions, the session can hold on to a minimum
		// number of containers.
		// Ideally, this would be enough to provide desired balance of
		// efficiency for the application
		// and sharing of resources with other applications. Typically, the
		// number of containers to be
		// pre-warmed equals the number of containers to be held between DAGs.

		if (tezConf.getBoolean(enablePrewarmConfig, false)) {
			// the above parameter is not a Tez parameter. Its only for this
			// example.
			// In this example we are pre-warming enough containers to run all
			// the sum tasks in parallel.
			// This means pre-warming numPartitions number of containers.
			// We are making the pre-warm and held containers to be the same and
			// using the helper API to
			// set up pre-warming. They can be made different and also custom
			// initialization logic can be
			// specified using other API's. We know that the OrderedWordCount
			// dag uses default files and
			// resources. Otherwise we would have to specify matching parameters
			// in the preWarm API too.
			tezConf.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS,
					numPartitions);
			tezClient.preWarm(PreWarmVertex.createConfigBuilder(tezConf)
					.build());
		}

		// the remaining code is the same as submitting any DAG.
		try {
			for (int i = 0; i < inputPaths.length; ++i) {
				DAG dag = OrderedWordCount.createDAG(tezConf, inputPaths[i],
				// The names of DAG must be unique in a session
						outputPaths[i], numPartitions, ("DAG-Iteration-" + i));
				tezClient.waitTillReady();
				System.out.println("Running dag number " + i);
				DAGClient dagClient = tezClient.submitDAG(dag);
				// wait to finish
				DAGStatus dagStatus = dagClient.waitForCompletion();
				populateSeedData(dag, seedData, tezClient);
				if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
					System.out.println("Iteration " + i
							+ " failed with diagnostics: "
							+ dagStatus.getDiagnostics());
					return false;
				}

			}
			return true;
		} finally {
			tezClient.stop();
		}
	}

	/**
	 * Seed data
	 */
	void populateSeedData(DAG aDag, SeedData seedData, TezClient tezClient) {
		seedData.appId = tezClient.getAppMasterApplicationId().toString();
		SeedData.DAG seedDag = new SeedData.DAG();
		seedDag.name = aDag.getName();
		for (Vertex aVertex : aDag.getVertices()) {
			SeedData.DAG.Vertex seedVertex = new SeedData.DAG.Vertex();
			seedVertex.name = aVertex.getName();
			seedDag.vertices.add(seedVertex);
		}
		seedData.dags.add(seedDag);
	}
}