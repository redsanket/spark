package hadooptest.cluster;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.fullydistributed.FullyDistributedFailJob;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.pseudodistributed.PseudoDistributedFailJob;

/*
 * Class factory selector for fail jobs.
 */
public final class FailJobFactory {
	public static Job getFailJob() {
		if (TestSession.getCluster() instanceof FullyDistributedCluster) {
			return new FullyDistributedFailJob();
		}
		else if (TestSession.getCluster() instanceof PseudoDistributedCluster) {
			return new PseudoDistributedFailJob();
		}
		
		TestSession.logger.error("There is no fail job implementation for the specified cluster type.");
		return null;
	}
}