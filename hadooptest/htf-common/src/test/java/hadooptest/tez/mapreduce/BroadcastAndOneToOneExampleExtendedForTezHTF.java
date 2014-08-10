package hadooptest.tez.mapreduce;

import hadooptest.TestSession;
import hadooptest.tez.TezUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.junit.BeforeClass;
import org.junit.Test;

public class BroadcastAndOneToOneExampleExtendedForTezHTF extends BroadcastAndOneToOneExample {


	public int run(String[] args, TezUtils.LocalMode localModeFlag) throws Exception {
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
		conf = TezUtils.setupConfForTez(conf, localModeFlag);
		boolean status = run(conf, doLocalityCheck);
		return status ? 0 : 1;
	}

	private static void printUsage() {
		System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
		ToolRunner.printGenericCommandUsage(System.err);
	}

	static String skipLocalityCheck = "-skipLocalityCheck";

}
