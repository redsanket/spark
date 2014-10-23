package hadooptest.tez.mapreduce.examples.extensions;

import java.util.LinkedHashSet;
import java.util.Set;

import hadooptest.TestSession;
import hadooptest.tez.ats.SeedData;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.examples.MRRSleepJob;

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

public class MRRSleepJobExtendedForTezHTF extends MRRSleepJob {
	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	private Credentials credentials = new Credentials();

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

	public int run(String[] args, String mode, Session session, TimelineServer timelineServer, String testName) throws Exception {

	    if(args.length < 1) {
	      System.err.println("MRRSleepJob [-m numMapper] [-r numReducer]" +
	          " [-ir numIntermediateReducer]" +
	          " [-irs numIntermediateReducerStages]" +
	          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]" +
	          " [-irt intermediateReduceSleepTime]" +
	          " [-recordt recordSleepTime (msec)]" +
	          " [-generateSplitsInAM (false)/true]" +
	          " [-writeSplitsToDfs (false)/true]");
	      ToolRunner.printGenericCommandUsage(System.err);
	      return 2;
	    }

	    int numMapper = 1, numReducer = 1, numIReducer = 1;
	    long mapSleepTime = 100, reduceSleepTime = 100, recSleepTime = 100,
	        iReduceSleepTime=1;
	    int mapSleepCount = 1, reduceSleepCount = 1, iReduceSleepCount = 1;
	    int iReduceStagesCount = 1;
	    boolean writeSplitsToDfs = false;
	    boolean generateSplitsInAM = false;
	    boolean splitsOptionFound = false;

	    for(int i=0; i < args.length; i++ ) {
	      if(args[i].equals("-m")) {
	        numMapper = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-r")) {
	        numReducer = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-ir")) {
	        numIReducer = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-mt")) {
	        mapSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-rt")) {
	        reduceSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-irt")) {
	        iReduceSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-irs")) {
	        iReduceStagesCount = Integer.parseInt(args[++i]);
	      }
	      else if (args[i].equals("-recordt")) {
	        recSleepTime = Long.parseLong(args[++i]);
	      }
	      else if (args[i].equals("-generateSplitsInAM")) {
	        if (splitsOptionFound) {
	          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
	        }
	        splitsOptionFound = true;
	        generateSplitsInAM = Boolean.parseBoolean(args[++i]);
	        
	      }
	      else if (args[i].equals("-writeSplitsToDfs")) {
	        if (splitsOptionFound) {
	          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
	        }
	        splitsOptionFound = true;
	        writeSplitsToDfs = Boolean.parseBoolean(args[++i]);
	      }
	    }

	    if (numIReducer > 0 && numReducer <= 0) {
	      throw new RuntimeException("Cannot have intermediate reduces without"
	          + " a final reduce");
	    }

	    // sleep for *SleepTime duration in Task by recSleepTime per record
	    mapSleepCount = (int)Math.ceil(mapSleepTime / ((double)recSleepTime));
	    reduceSleepCount = (int)Math.ceil(reduceSleepTime / ((double)recSleepTime));
	    iReduceSleepCount = (int)Math.ceil(iReduceSleepTime / ((double)recSleepTime));

	    TezConfiguration conf = new TezConfiguration(HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName));
	    FileSystem remoteFs = FileSystem.get(conf);

	    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
	        conf.get(
	            TezConfiguration.TEZ_AM_STAGING_DIR,
	            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT));
	    
	    Path remoteStagingDir =
	        remoteFs.makeQualified(new Path(conf.get(
	            TezConfiguration.TEZ_AM_STAGING_DIR,
	            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT),
	            Long.toString(System.currentTimeMillis())));
	    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

	    DAG dag = createDAG(remoteFs, conf, remoteStagingDir,
	        numMapper, numReducer, iReduceStagesCount, numIReducer,
	        mapSleepTime, mapSleepCount, reduceSleepTime, reduceSleepCount,
	        iReduceSleepTime, iReduceSleepCount, writeSplitsToDfs, generateSplitsInAM);

	    TezClient tezSession = TezClient.create("MRRSleep", conf, false, null, credentials);
	    tezSession.start();
	    DAGClient dagClient = tezSession.submitDAG(dag);
	    dagClient.waitForCompletion();
	    tezSession.stop();

	    return dagClient.getDAGStatus(null).getState().equals(DAGStatus.State.SUCCEEDED) ? 0 : 1;
	}

	/**
	 * HTF-ATS (Timelineserver related change, to pass ugi)
	 * @param args
	 * @param mode
	 * @param session
	 * @param timelineServer
	 * @param testName
	 * @param ugi
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args, String mode, Session session, TimelineServer timelineServer, String testName,
			UserGroupInformation ugi, SeedData seedData) throws Exception {

	    if(args.length < 1) {
	      System.err.println("MRRSleepJob [-m numMapper] [-r numReducer]" +
	          " [-ir numIntermediateReducer]" +
	          " [-irs numIntermediateReducerStages]" +
	          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]" +
	          " [-irt intermediateReduceSleepTime]" +
	          " [-recordt recordSleepTime (msec)]" +
	          " [-generateSplitsInAM (false)/true]" +
	          " [-writeSplitsToDfs (false)/true]");
	      ToolRunner.printGenericCommandUsage(System.err);
	      return 2;
	    }
	    

	    int numMapper = 1, numReducer = 1, numIReducer = 1;
	    long mapSleepTime = 100, reduceSleepTime = 100, recSleepTime = 100,
	        iReduceSleepTime=1;
	    int mapSleepCount = 1, reduceSleepCount = 1, iReduceSleepCount = 1;
	    int iReduceStagesCount = 1;
	    boolean writeSplitsToDfs = false;
	    boolean generateSplitsInAM = false;
	    boolean splitsOptionFound = false;

	    for(int i=0; i < args.length; i++ ) {
	      if(args[i].equals("-m")) {
	        numMapper = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-r")) {
	        numReducer = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-ir")) {
	        numIReducer = Integer.parseInt(args[++i]);
	      }
	      else if(args[i].equals("-mt")) {
	        mapSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-rt")) {
	        reduceSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-irt")) {
	        iReduceSleepTime = Long.parseLong(args[++i]);
	      }
	      else if(args[i].equals("-irs")) {
	        iReduceStagesCount = Integer.parseInt(args[++i]);
	      }
	      else if (args[i].equals("-recordt")) {
	        recSleepTime = Long.parseLong(args[++i]);
	      }
	      else if (args[i].equals("-generateSplitsInAM")) {
	        if (splitsOptionFound) {
	          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
	        }
	        splitsOptionFound = true;
	        generateSplitsInAM = Boolean.parseBoolean(args[++i]);
	        
	      }
	      else if (args[i].equals("-writeSplitsToDfs")) {
	        if (splitsOptionFound) {
	          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
	        }
	        splitsOptionFound = true;
	        writeSplitsToDfs = Boolean.parseBoolean(args[++i]);
	      }
	    }

	    if (numIReducer > 0 && numReducer <= 0) {
	      throw new RuntimeException("Cannot have intermediate reduces without"
	          + " a final reduce");
	    }

	    // sleep for *SleepTime duration in Task by recSleepTime per record
	    mapSleepCount = (int)Math.ceil(mapSleepTime / ((double)recSleepTime));
	    reduceSleepCount = (int)Math.ceil(reduceSleepTime / ((double)recSleepTime));
	    iReduceSleepCount = (int)Math.ceil(iReduceSleepTime / ((double)recSleepTime));

	    TezConfiguration conf = new TezConfiguration(HtfTezUtils.setupConfForTez(TestSession.cluster.getConf(), mode, session, timelineServer, testName));

	    FileSystem remoteFs = FileSystem.get(conf);
	    
	    /**
	     * HTF
	     * Set the UGI, as needed by ATS
	     */
	    UserGroupInformation.setConfiguration(conf);
	    UserGroupInformation.setLoginUser(ugi);
	    

	    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
	        conf.get(
	            TezConfiguration.TEZ_AM_STAGING_DIR,
	            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT));
	    
	    Path remoteStagingDir =
	        remoteFs.makeQualified(new Path(conf.get(
	            TezConfiguration.TEZ_AM_STAGING_DIR,
	            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT),
	            Long.toString(System.currentTimeMillis())));
	    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

	    DAG dag = createDAG(remoteFs, conf, remoteStagingDir,
	        numMapper, numReducer, iReduceStagesCount, numIReducer,
	        mapSleepTime, mapSleepCount, reduceSleepTime, reduceSleepCount,
	        iReduceSleepTime, iReduceSleepCount, writeSplitsToDfs, generateSplitsInAM);
	    
	    TezClient tezClient = TezClient.create("MRRSleep", conf, false, null, ugi.getCredentials());
	    tezClient.start();
	    DAGClient dagClient = tezClient.submitDAG(dag);
	    dagClient.waitForCompletion();
	    populateSeedData(dag, seedData, tezClient);
	    tezClient.stop();

	    return dagClient.getDAGStatus(null).getState().equals(DAGStatus.State.SUCCEEDED) ? 0 : 1;
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
