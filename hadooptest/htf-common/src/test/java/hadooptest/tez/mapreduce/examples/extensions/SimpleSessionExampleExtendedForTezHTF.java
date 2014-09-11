package hadooptest.tez.mapreduce.examples.extensions;

import org.apache.hadoop.conf.Configuration;
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
public class SimpleSessionExampleExtendedForTezHTF extends
		SimpleSessionExample {

	/**
	 * Copy and paste the the code from the parent class's run method here.
	 * Change all references to getConf() to HtfTezUtils.setupConfForTez(conf,
	 * mode) Note: Be careful, there could be several run methods there, for
	 * example those contained inside a Processor, or that overriding the method
	 * in the Tool class.
	 * 
	 * NOTE: In this case since the run method is accepting a conf object, it is in
	 * out control to pass it as an argument from our test. This class is provided
	 * for preserving the consistency of Test Design. 
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */
	  public boolean run(String[] inputPaths, String[] outputPaths, Configuration conf,
		      int numPartitions) throws Exception {
		  return super.run(inputPaths, outputPaths,conf, numPartitions);
	  }
	/**
	 * Re-Provided here, because the corresponding method in the base class is
	 * marked private.
	 */
	  private static void printUsage() {
		    System.err.println("Usage: " + " simplesessionexample <in1,in2> <out1, out2> [numPartitions]");
		  }

}
