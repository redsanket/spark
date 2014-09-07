package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;

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
public class BroadcastAndOneToOneExampleExtendedForTezHTF extends
		BroadcastAndOneToOneExample {
	protected static String skipLocalityCheck = "-skipLocalityCheck";

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
	public int run(String[] args, String mode, boolean session, String testName) throws Exception {
		boolean doLocalityCheck = true;
		if (args.length == 1) {
			if (args[0].equals(skipLocalityCheck)) {
				doLocalityCheck = false;
			} else {
				printUsage();
				throw new TezException("Invalid command line");
			}
		} else if (args.length > 1) {
			printUsage();
			throw new TezException("Invalid command line");
		}

		Configuration conf = TestSession.cluster.getConf();
		conf = HtfTezUtils.setupConfForTez(conf, mode, session, testName);
		boolean status = run(conf, doLocalityCheck);
		return status ? 0 : 1;
	}

	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	private static void printUsage() {
		System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
		ToolRunner.printGenericCommandUsage(System.err);
	}

}
