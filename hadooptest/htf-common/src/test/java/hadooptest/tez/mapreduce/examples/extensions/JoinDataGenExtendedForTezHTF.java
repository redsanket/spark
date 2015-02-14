package hadooptest.tez.mapreduce.examples.extensions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.JoinDataGen.GenDataProcessor;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.apache.tez.mapreduce.output.MROutput;

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
public class JoinDataGenExtendedForTezHTF extends JoinDataGen {
	private static final String STREAM_OUTPUT_NAME = "streamoutput";
	private static final String HASH_OUTPUT_NAME = "hashoutput";
	private static final String EXPECTED_OUTPUT_NAME = "expectedoutput";

	protected static final String OUTPUT_DIR = "/tmp/joindata/expectedResults/";
	protected static final String TEMP_OUT_1 = "/tmp/joindata/out1/";
	protected static final String TEMP_OUT_2 = "/tmp/joindata/out2/";

	/**
	 * Copy and paste the the code from the parent class's run method here.
	 * Change all references to getConf() to HtfTezUtils.setupConfForTez(conf,
	 * mode) Note: Be careful, there could be several run methods there, for
	 * example those contained inside a Processor, or that overriding the method
	 * in the Tool class.
	 * 
	 * Usage: joindatagen <outPath1> <path1Size> <outPath2> <path2Size>
	 * <expectedResultPath> <numTasks>
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */
	public int run(TezConfiguration tezConf, String[] args, TezClient tezClient)
			throws Exception {
	    TestSession.logger.info("Running JoinDataGen");

	    UserGroupInformation.setConfiguration(tezConf);

	    String outDir1 = args[0];
	    long outDir1Size = Long.parseLong(args[1]);
	    String outDir2 = args[2];
	    long outDir2Size = Long.parseLong(args[3]);
	    String expectedOutputDir = args[4];
	    int numTasks = Integer.parseInt(args[5]);

	    Path largeOutPath = null;
	    Path smallOutPath = null;
	    long largeOutSize = 0;
	    long smallOutSize = 0;

	    if (outDir1Size >= outDir2Size) {
	      largeOutPath = new Path(outDir1);
	      largeOutSize = outDir1Size;
	      smallOutPath = new Path(outDir2);
	      smallOutSize = outDir2Size;
	    } else {
	      largeOutPath = new Path(outDir2);
	      largeOutSize = outDir2Size;
	      smallOutPath = new Path(outDir1);
	      smallOutSize = outDir1Size;
	    }

	    Path expectedOutputPath = new Path(expectedOutputDir);

	    // Verify output path existence
	    FileSystem fs = FileSystem.get(tezConf);
	    int res = 0;
	    res = checkOutputDirectory(fs, largeOutPath) + checkOutputDirectory(fs, smallOutPath)
	        + checkOutputDirectory(fs, expectedOutputPath);
	    if (res != 0) {
	      return 3;
	    }

	    if (numTasks <= 0) {
	      System.err.println("NumTasks must be > 0");
	      return 4;
	    }

	    DAG dag = createDag(tezConf, largeOutPath, smallOutPath, expectedOutputPath, numTasks,
	        largeOutSize, smallOutSize);

	    tezClient.waitTillReady();
	    DAGClient dagClient = tezClient.submitDAG(dag);
	    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
	    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	      TestSession.logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
	      return -1;
	    }
	    return 0;
		
	}

	private DAG createDag(TezConfiguration tezConf, Path largeOutPath,
			Path smallOutPath, Path expectedOutputPath, int numTasks,
			long largeOutSize, long smallOutSize) throws IOException {

	    long largeOutSizePerTask = largeOutSize / numTasks;
	    long smallOutSizePerTask = smallOutSize / numTasks;
	    Random randomGenerator = new Random();
	    int randomInt = randomGenerator.nextInt(100000);

	    DAG dag = DAG.create("JoinDataGen" + randomInt);

	    Vertex genDataVertex = Vertex.create("datagen", ProcessorDescriptor.create(
	        GenDataProcessor.class.getName()).setUserPayload(
	        UserPayload.create(ByteBuffer.wrap(GenDataProcessor.createConfiguration(largeOutSizePerTask,
	            smallOutSizePerTask)))), numTasks);
	    genDataVertex.addDataSink(STREAM_OUTPUT_NAME, 
	        MROutput.createConfigBuilder(new Configuration(tezConf),
	            TextOutputFormat.class, largeOutPath.toUri().toString()).build());
	    genDataVertex.addDataSink(HASH_OUTPUT_NAME, 
	        MROutput.createConfigBuilder(new Configuration(tezConf),
	            TextOutputFormat.class, smallOutPath.toUri().toString()).build());
	    genDataVertex.addDataSink(EXPECTED_OUTPUT_NAME, 
	        MROutput.createConfigBuilder(new Configuration(tezConf),
	            TextOutputFormat.class, expectedOutputPath.toUri().toString()).build());

	    dag.addVertex(genDataVertex);

	    return dag;
	}

	private int checkOutputDirectory(FileSystem fs, Path path)
			throws IOException {
		if (fs.exists(path)) {
			System.err.println("Output directory: " + path + " already exists");
			return 2;
		}
		return 0;
	}

}
