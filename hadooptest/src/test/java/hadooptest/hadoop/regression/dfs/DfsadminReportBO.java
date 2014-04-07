package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;

import java.util.ArrayList;

import org.apache.log4j.Logger;

public class DfsadminReportBO {
	static Logger logger = Logger.getLogger(DfsadminReportBO.class);

	private static final String CONFIGURED_CAPACITY_COLON = "Configured Capacity:";
	private static final String PRESENT_CAPACITY_COLON = "Present Capacity:";
	private static final String DFS_REMAINING_COLON = "DFS Remaining:";
	private static final String DFS_USED_COLON = "DFS Used:";
	private static final String DFS_USED_PERCENTAGE_COLON = "DFS Used%:";
	private static final String UNDER_REPLICATED_BLOCKS_COLON = "Under replicated blocks:";
	private static final String BLOCKS_WITH_CORRUPT_REPLICAS_COLON = "Blocks with corrupt replicas:";
	private static final String MISSING_BLOCKS_COLON = "Missing blocks:";
	private static final String DATANODES_AVAILABLE_COLON = "Datanodes available:";
	private static final String LIVE_DATANODES_COLON = "Live datanodes:";
	private static final String DEAD_DATANODES_COLON = "Dead datanodes:";
	private static final String NAME_COLON = "Name:";
	private static final String HOSTNAME_COLON = "Hostname:";
	private static final String RACK_COLON = "Rack:";
	private static final String DECOMMISSION_STATUS_COLON = "Decommission Status :";
	private static final String NON_DFS_USED_COLON = "Non DFS Used:";
	private static final String DFS_REMAINING_PERCENTAGE_COLON = "DFS Remaining%:";
	private static final String CONFIGURED_CACHE_CAPACITY_COLON = "Configured Cache Capacity:";
	private static final String CACHE_USED_COLON = "Cache Used:";
	private static final String CACHE_REMAINING_COLON = "Cache Remaining:";
	private static final String CACHE_USED_PERCENTAGE_COLON = "Cache Used%:";
	private static final String CACHE_REMAINING_PERCENTAGE_COLON = "Cache Remaining%:";
	private static final String LAST_CONTACT_COLON = "Last contact:";

	class DatanodeBO {
		String name;
		String hostname;
		String rack;
		String decommissionStatus;
		String configuredCapacity;
		String dfsUsed;
		String nonDfsUsed;
		String dfsRemaining;
		String dfsUsedPercentage;
		String dfsRemainingPercentage;
		String configuredCacheCapacity;
		String cacheUsed;
		String cacheRemaining;
		String cacheUsedPercentage;
		String cacheRemainingPercentage;
		String lastContact;

	}

	String configuredCapacity;
	String presentCapacity;
	String dfsRemaining;
	String dfsUsed;
	String dfsUsedPercentage;
	String underReplicatedBlocks;
	String blocksWithCorruptReplicas;
	String missingBlocks;
	String datanodesAvailable;
	ArrayList<DatanodeBO> liveDatanodes = new ArrayList<DatanodeBO>();
	ArrayList<DatanodeBO> deadDatanodes = new ArrayList<DatanodeBO>();

	public DfsadminReportBO(String blurb) {
		boolean processingLiveDatanodes = false;
		boolean processingDeadDatanodes = false;
		DatanodeBO aDatanodeBO = null;
		for (String aLineBeingProcessed : blurb.split("\n")) {
			if (!processingLiveDatanodes && !processingDeadDatanodes) {
				if (aLineBeingProcessed.contains(LIVE_DATANODES_COLON)) {
					processingLiveDatanodes = true;
					continue;
				}
				if (aLineBeingProcessed.contains(DEAD_DATANODES_COLON)) {
					processingDeadDatanodes = true;
					continue;
				}
				if (aLineBeingProcessed.contains(CONFIGURED_CAPACITY_COLON)) {
					this.configuredCapacity = aLineBeingProcessed.replace(
							CONFIGURED_CAPACITY_COLON, "").trim();
				}
				if (aLineBeingProcessed.contains(PRESENT_CAPACITY_COLON)) {
					this.presentCapacity = aLineBeingProcessed.replace(
							PRESENT_CAPACITY_COLON, "").trim();
				}
				if (aLineBeingProcessed.contains(DFS_REMAINING_COLON)) {
					this.dfsRemaining = aLineBeingProcessed.replace(
							DFS_REMAINING_COLON, "").trim();
				}
				if (aLineBeingProcessed.contains(DFS_USED_COLON)) {
					this.dfsUsed = aLineBeingProcessed.replace(DFS_USED_COLON,
							"").trim();
				}
				if (aLineBeingProcessed.contains(DFS_USED_PERCENTAGE_COLON)) {
					this.dfsUsedPercentage = aLineBeingProcessed.replace(
							DFS_USED_PERCENTAGE_COLON, "").trim();
				}
				if (aLineBeingProcessed.contains(UNDER_REPLICATED_BLOCKS_COLON)) {
					this.underReplicatedBlocks = aLineBeingProcessed.replace(
							UNDER_REPLICATED_BLOCKS_COLON, "".trim());
				}
				if (aLineBeingProcessed
						.contains(BLOCKS_WITH_CORRUPT_REPLICAS_COLON)) {
					this.blocksWithCorruptReplicas = aLineBeingProcessed
							.replace(BLOCKS_WITH_CORRUPT_REPLICAS_COLON, "")
							.trim();
				}
				if (aLineBeingProcessed.contains(MISSING_BLOCKS_COLON)) {
					this.missingBlocks = aLineBeingProcessed.replace(
							MISSING_BLOCKS_COLON, "").trim();
				}
				if (aLineBeingProcessed.contains(DATANODES_AVAILABLE_COLON)) {
					this.datanodesAvailable = aLineBeingProcessed.replace(
							DATANODES_AVAILABLE_COLON, "").trim();
				}
				continue;

			}
			// Process Live/Dead datanodes
			if (processingLiveDatanodes) {
				if (aLineBeingProcessed.contains(DEAD_DATANODES_COLON)) {
					processingDeadDatanodes = true;
					processingLiveDatanodes = false;
					continue;
				}
			}
			if (processingDeadDatanodes) {
				if (aLineBeingProcessed.contains(LIVE_DATANODES_COLON)) {
					processingDeadDatanodes = false;
					processingLiveDatanodes = true;
					continue;
				}
			}
			

			if (aLineBeingProcessed.contains(NAME_COLON)) {
				aDatanodeBO = new DatanodeBO();
				TestSession.logger.info("Allocated aDatanodeBO, after tripping on " + aLineBeingProcessed);
				aDatanodeBO.name = aLineBeingProcessed.replace(NAME_COLON, "")
						.trim();
				
				continue;
			}
			if (aLineBeingProcessed.contains(HOSTNAME_COLON)) {
				aDatanodeBO.hostname = aLineBeingProcessed.replace(
						HOSTNAME_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(RACK_COLON)) {
				aDatanodeBO.rack = aLineBeingProcessed.replace(RACK_COLON, "")
						.trim();
				continue;
			}
			if (aLineBeingProcessed.contains(DECOMMISSION_STATUS_COLON)) {
				aDatanodeBO.decommissionStatus = aLineBeingProcessed.replace(
						DECOMMISSION_STATUS_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CONFIGURED_CAPACITY_COLON)) {
				aDatanodeBO.configuredCapacity = aLineBeingProcessed.replace(
						CONFIGURED_CAPACITY_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(DFS_USED_COLON)) {
				aDatanodeBO.dfsUsed = aLineBeingProcessed.replace(
						DFS_USED_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(NON_DFS_USED_COLON)) {
				aDatanodeBO.nonDfsUsed = aLineBeingProcessed.replace(
						NON_DFS_USED_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(DFS_REMAINING_COLON)) {
				aDatanodeBO.dfsRemaining = aLineBeingProcessed.replace(
						DFS_REMAINING_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(DFS_USED_PERCENTAGE_COLON)) {
				aDatanodeBO.dfsUsedPercentage = aLineBeingProcessed.replace(
						DFS_USED_PERCENTAGE_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(DFS_REMAINING_PERCENTAGE_COLON)) {
				aDatanodeBO.dfsRemainingPercentage = aLineBeingProcessed
						.replace(DFS_REMAINING_PERCENTAGE_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CONFIGURED_CACHE_CAPACITY_COLON)) {
				aDatanodeBO.configuredCacheCapacity = aLineBeingProcessed
						.replace(CONFIGURED_CACHE_CAPACITY_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CACHE_USED_COLON)) {
				aDatanodeBO.cacheUsed = aLineBeingProcessed.replace(
						CACHE_USED_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CACHE_REMAINING_COLON)) {
				aDatanodeBO.cacheRemaining = aLineBeingProcessed.replace(
						CACHE_REMAINING_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CACHE_USED_PERCENTAGE_COLON)) {
				aDatanodeBO.cacheUsedPercentage = aLineBeingProcessed.replace(
						CACHE_USED_PERCENTAGE_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(CACHE_REMAINING_PERCENTAGE_COLON)) {
				aDatanodeBO.cacheRemainingPercentage = aLineBeingProcessed
						.replace(CACHE_REMAINING_PERCENTAGE_COLON, "").trim();
				continue;
			}
			if (aLineBeingProcessed.contains(LAST_CONTACT_COLON)) {
				aDatanodeBO.lastContact = aLineBeingProcessed.replace(
						LAST_CONTACT_COLON, "").trim();

				if (processingLiveDatanodes) {
					liveDatanodes.add(aDatanodeBO);
				} else if (processingDeadDatanodes) {
					deadDatanodes.add(aDatanodeBO);
				}
			}
		}
		logger.info("Dead nodes:" + deadDatanodes);
		logger.info("Live nodes:" + liveDatanodes);

	}
}
