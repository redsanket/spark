package hadooptest.tez.mapreduce.examples.extensions;

import java.io.IOException;

import javax.annotation.Nullable;

import hadooptest.TestSession;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.Session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.apache.tez.mapreduce.examples.RPCLoadGen;

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
public class RPCLoadGenExtendedForTezHTF extends RPCLoadGen {
	private TezClient tezClientInternal;

	/**
	 * Copy and paste the the code from the parent class's run
	 * method here. Change all references to getConf() to
	 * HtfTezUtils.setupConfForTez(conf, mode) Note: Be careful, there could be
	 * several run methods there, for example those contained inside a
	 * Processor, or that overriding the method in the Tool class.
	 * 
	 * @param args
	 * @param mode
	 * @return
	 * @throws Exception
	 */
	public int run(TezConfiguration conf, String[] args,
			@Nullable TezClient tezClient, String mode, Session session, String testName) throws IOException,
			TezException, InterruptedException {
		setConf(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		return _execute(otherArgs, conf, tezClient, mode, session, testName);
	}

	/**
	 * This has been overridden from TezExampleBase. Because it was marked as
	 * private, I could not override it but had to copy it in its entirety here.
	 * 
	 * @param otherArgs
	 * @param tezConf
	 * @param tezClient
	 * @param mode
	 * @return
	 * @throws IOException
	 * @throws TezException
	 * @throws InterruptedException
	 */
	private int _execute(String[] otherArgs, TezConfiguration tezConf,
			TezClient tezClient, String mode, Session session, String testName) throws IOException, TezException,
			InterruptedException {

		int result = _validateArgs(otherArgs);
		if (result != 0) {
			return result;
		}

		if (tezConf == null) {
			tezConf = new TezConfiguration(HtfTezUtils.setupConfForTez(
					TestSession.cluster.getConf(), mode, session, testName));
		}
		UserGroupInformation.setConfiguration(tezConf);
		boolean ownTezClient = false;
		if (tezClient == null) {
			ownTezClient = true;
			tezClientInternal = createTezClient(tezConf);
		}
		try {
			return runJob(otherArgs, tezConf, tezClientInternal);
		} finally {
			if (ownTezClient && tezClientInternal != null) {
				tezClientInternal.stop();
			}
		}
	}

	/**
	 * Was marked private in the base class, hence have to copy it in its
	 * entirety here.
	 * 
	 * @param tezConf
	 * @return
	 * @throws IOException
	 * @throws TezException
	 */
	private TezClient createTezClient(TezConfiguration tezConf)
			throws IOException, TezException {
		TezClient tezClient = TezClient.create(getClass().getSimpleName(),
				tezConf);
		tezClient.start();
		return tezClient;
	}

	/**
	 * Was marked private in the base class, hence have to copy it in its
	 * entirety here.
	 * 
	 * @param args
	 * @return
	 */
	private int _validateArgs(String[] args) {
		int res = validateArgs(args);
		if (res != 0) {
			printUsage();
			return res;
		}
		return 0;
	}
}
