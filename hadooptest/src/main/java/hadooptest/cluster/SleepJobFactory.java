package hadooptest.cluster;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.fullydistributed.FullyDistributedSleepJob;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.pseudodistributed.PseudoDistributedSleepJob;

/*
 * Class factory selector for sleep jobs.
 */
public final class SleepJobFactory {
	public static Job getSleepJob() {
		if (TestSession.getCluster() instanceof FullyDistributedCluster) {
			return new FullyDistributedSleepJob();
		}
		else if (TestSession.getCluster() instanceof PseudoDistributedCluster) {
			return new PseudoDistributedSleepJob();
		}
		
		TestSession.logger.error("There is no sleep job implementation for the specified cluster type.");
		return null;
	}
}
