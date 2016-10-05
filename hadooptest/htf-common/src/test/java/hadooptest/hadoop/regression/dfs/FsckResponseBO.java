package hadooptest.hadoop.regression.dfs;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
/**
 * The FSCK Response Business Object
 * @author tiwari
 *
 */
public class FsckResponseBO {
	public HashMap<FsckFileDetailsBO, ArrayList<FsckBlockDetailsBO>> fileAndBlockDetails;
	public FsckSummaryBO fsckSummaryBO;

	/*
	 * The consolidated Response
	 */
	public FsckResponseBO(Process process, boolean filesArgPassed,
			boolean blocksArgPassed, boolean racksArgPassed)
			throws InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		fsckSummaryBO = new FsckSummaryBO();

		String aLineFromFsckResponse;
		FsckFileDetailsBO fileDetailsBO = null;
		FsckBlockDetailsBO blockDetailsBO = null;

		fileAndBlockDetails = new HashMap<FsckFileDetailsBO, ArrayList<FsckBlockDetailsBO>>();
		try {
			aLineFromFsckResponse = reader.readLine();
			while (aLineFromFsckResponse != null) {
				aLineFromFsckResponse = reader.readLine();
				if (filesArgPassed) {
					if ((aLineFromFsckResponse != null)
							&& (!aLineFromFsckResponse.isEmpty() && aLineFromFsckResponse
									.matches(FsckFileDetailsBO.pattern))) {
						TestSession.logger.info("Processing line for context (filesArgPassed):"
								+ aLineFromFsckResponse);
						fileDetailsBO = new FsckFileDetailsBO(
								aLineFromFsckResponse);
						fileAndBlockDetails.put(fileDetailsBO,
								new ArrayList<FsckBlockDetailsBO>());
						continue;

					}
				}
				if (blocksArgPassed && racksArgPassed) {
					if ((aLineFromFsckResponse != null)
							&& (!aLineFromFsckResponse.isEmpty() && aLineFromFsckResponse
									.matches(FsckBlockDetailsBO.blocksAndRacksPattern))) {
						TestSession.logger.info("Processing line for context (blocksArgPassed && racksArgPassed):"
								+ aLineFromFsckResponse);
						blockDetailsBO = new FsckBlockDetailsBO(
								aLineFromFsckResponse, racksArgPassed);
						fileAndBlockDetails.get(fileDetailsBO).add(
								blockDetailsBO);
						continue;
					}
				} else if (blocksArgPassed && !racksArgPassed) {
					if ((aLineFromFsckResponse != null)
							&& (!aLineFromFsckResponse.isEmpty() && aLineFromFsckResponse
									.matches(FsckBlockDetailsBO.blocksAndNoRacksPattern))) {
						TestSession.logger.info("Processing line for context (blocksArgPassed && !racksArgPassed):"
								+ aLineFromFsckResponse);
						blockDetailsBO = new FsckBlockDetailsBO(
								aLineFromFsckResponse, racksArgPassed);
						fileAndBlockDetails.get(fileDetailsBO).add(
								blockDetailsBO);
						continue;
					}

				}
				if (aLineFromFsckResponse != null) {
					TestSession.logger.info("Processing line:"
							+ aLineFromFsckResponse);
					fsckSummaryBO.setAppropriateMember(aLineFromFsckResponse);
				}

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();

	}

	@Override
	public String toString() {
		return fileAndBlockDetails.toString() + fsckSummaryBO.toString();
	}

	/*
	 * Fsck File Details
	 */
	public class FsckFileDetailsBO {
		String fileName;
		double sizeInBytes;
		int numBlocks;
		String status;

		private static final String pattern = "([a-zA-Z0-9//_]+)\\s+([\\d]+)\\s+bytes,\\s+([\\d]+)\\s+block\\(s\\):\\s+(\\w+)";

		/*
		 * Data looks like this /HTF/testdata/dfs/_big_file_10dot7GB 11811160064
		 * bytes, 88 block(s): OK /HTF/testdata/dfs/_big_file_11GB 11811160064
		 * bytes, 88 block(s): OK /HTF/testdata/dfs/_file_128MB 1073741824
		 * bytes, 8 block(s): OK
		 */
		public FsckFileDetailsBO(String aLineFromFsckResponse) {
			this.fileName = aLineFromFsckResponse.replaceAll(pattern, "$1");
			this.sizeInBytes = Double.parseDouble(aLineFromFsckResponse
					.replaceAll(pattern, "$2"));
			this.numBlocks = Integer.parseInt(aLineFromFsckResponse.replaceAll(
					pattern, "$3"));
			this.status = aLineFromFsckResponse.replaceAll(pattern, "$4");

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("\n");
			sb.append("Filename:" + fileName);
			sb.append(" ");
			sb.append("sizeInBytes:" + sizeInBytes);
			sb.append(" ");
			sb.append("numBlocks:" + numBlocks);
			sb.append(" ");
			sb.append("status:" + status);
			sb.append("\n");
			return sb.toString();
		}

	}

	/*
	 * File block details
	 */
	public class FsckBlockDetailsBO {
		//
		// private static final String blocksAndRacksPattern =
		// "([\\d]+)\\.\\s+([\\w\\-\\.]+):([\\w\\_]+)\\s+len=([\\d]+)\\s+repl=([\\d]+)\\s+\\[\\/([\\d\\.]+)\\/([\\d\\.\\:]+),\\s+\\/([\\d\\.]+)\\/([\\d\\.\\:]+),\\s+\\/([\\d\\.]+)\\/([\\d\\.\\:]+)\\]";
		// gridci-1398, 2.8 tweaked the fsck header slightly, "repl" became "Live_repl"
		private static final String blocksAndRacksPattern = "([\\d]+)\\.\\s+"
				+ "([\\w\\-\\.]+)\\:"
				+ "([\\w\\_]+)\\s+"
				+ "len=([\\d]+)\\s+"
				+ "\\*repl=([\\d]+)\\s+"
				+ "\\[\\/([\\d\\.]+)\\/([\\d\\.]+)\\:\\d+,\\s+\\/([\\d\\.]+)\\/([\\d\\.]+):\\d+,\\s+\\/([\\d\\.]+)\\/([\\d\\.]+)\\:\\d+\\]";
		private static final String blocksAndNoRacksPattern = "([\\d]+)\\.\\s+([\\w\\-\\.]+):([\\w\\_]+)\\s+len=([\\d]+)\\s+\\*repl=([\\d]+)";
		int sequenceNumber;
		String blockPool;
		String blockName;
		int length;
		int replication;
		HashMap<String, ArrayList<String>> dataNodeDistributionAcrossRacks;

		/*
		 * Data looks like this 0.
		 * BP-253779783-98.137.99.117-1386796889549:blk_1073745007_4184
		 * len=134217728 repl=3 [/98.137.99.64/98.137.99.123:1004,
		 * /98.137.99.64/98.137.99.122:1004, /98.137.99.64/98.137.99.124:1004]
		 */
		FsckBlockDetailsBO(String aLineFromFsckResponse, boolean racksArgPassed) {
			if (racksArgPassed) {
				dataNodeDistributionAcrossRacks = new HashMap<String, ArrayList<String>>();
				this.sequenceNumber = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$1"));
	TestSession.logger.info("PHW sequence: " + this.sequenceNumber);
				this.blockPool = aLineFromFsckResponse.replaceAll(
						blocksAndRacksPattern, "$2");
	TestSession.logger.info("PHW blockpool: " + this.blockPool);
				this.blockName = aLineFromFsckResponse.replaceAll(
						blocksAndRacksPattern, "$3");
	TestSession.logger.info("PHW blockname: " + this.blockName);
				this.length = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$4"));
	TestSession.logger.info("PHW length: " + this.length);
				this.replication = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$5"));
	TestSession.logger.info("PHW repl: " + this.replication);
				for (int xx = 0, rackPos = 6, datanodePos = 7; xx < replication; xx++) {
					String rack = aLineFromFsckResponse.replaceAll(
							blocksAndRacksPattern, "$" + rackPos);
					String datanode = aLineFromFsckResponse.replaceAll(
							blocksAndRacksPattern, "$" + datanodePos);
					if (dataNodeDistributionAcrossRacks.containsKey(rack)) {
						dataNodeDistributionAcrossRacks.get(rack).add(datanode);
					} else {
						ArrayList<String> dataNodes = new ArrayList<String>();
						dataNodes.add(datanode);
						dataNodeDistributionAcrossRacks.put(rack, dataNodes);
					}
					rackPos += 2;
					datanodePos += 2;
				}

			} else {
				dataNodeDistributionAcrossRacks = null;
				this.sequenceNumber = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$1"));
	TestSession.logger.info("PHW sequence2: " + this.sequenceNumber);
				this.blockPool = aLineFromFsckResponse.replaceAll(
						blocksAndRacksPattern, "$2");
	TestSession.logger.info("PHW blockpool2: " + this.blockPool);
				this.blockName = aLineFromFsckResponse.replaceAll(
						blocksAndRacksPattern, "$3");
	TestSession.logger.info("PHW blockname2: " + this.blockName);
				this.length = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$4"));
	TestSession.logger.info("PHW length2: " + this.length);
				this.replication = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(blocksAndRacksPattern, "$5"));
	TestSession.logger.info("PHW repl2: " + this.replication);
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("sequenceNumber:" + sequenceNumber);
			sb.append(" ");
			sb.append("block-pool:" + blockPool);
			sb.append(" ");
			sb.append("block-name:" + blockName);
			sb.append(" ");
			sb.append("block-length:" + length);
			sb.append(" ");
			sb.append("block-replication:" + replication);
			sb.append(" ");

			if (dataNodeDistributionAcrossRacks != null) {
				for (String rack : dataNodeDistributionAcrossRacks.keySet()) {
					sb.append("Rack:" + rack);
					sb.append(" ");
					sb.append("Datanodes:"
							+ dataNodeDistributionAcrossRacks.get(rack));
					sb.append(" ");
				}
			}
			sb.append("\n");
			return sb.toString();
		}
	}

	/*
	 * Summary, that is a multi-line response, printed towards the end.
	 */
	public class FsckSummaryBO {
		public String status;
		public double totalSizeInBytes;
		public int totalDirs;
		public int totalFiles;
		public int totalSymlinks;
		public int totalBlocksValidated;
		public int averageBlockSize;
		public int minimallyReplicatedBlocks;
		public int overReplicatedBlocks;
		public int underReplicatedBlocks;
		public int misReplicatedBlocks;
		public int defaultReplicationFactor;
		public float averageBlockReplication;
		public int corruptBlocks;
		public int missingReplicas;
		public int numberOfDatanodes;
		public int numberOfRacks;

		public FsckSummaryBO() {

		}

		void setAppropriateMember(String aLineFromFsckResponse) {
			String pattern = "^\\.*Status:\\s+(\\w+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.status = aLineFromFsckResponse.replaceAll(pattern, "$1");
				return;
			}

			pattern = "\\s+Total size:\\s+(\\d+)\\s+B";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.totalSizeInBytes = Double
						.parseDouble(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}
			pattern = "\\s+Total dirs:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.totalDirs = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}

			pattern = "\\s+Total files:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.totalFiles = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
			pattern = "\\s+Total symlinks:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.totalSymlinks = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
			pattern = "\\s+Total blocks \\(validated\\):\\s+(\\d+)\\s+\\(avg\\.\\s+block\\s+size\\s+(\\d+)\\s+B\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.totalBlocksValidated = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				this.averageBlockSize = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$2"));
				return;
			}
			pattern = "\\s+Minimally replicated blocks:\\s+(\\d+)\\s+\\(\\d+\\.\\d\\s+%\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.minimallyReplicatedBlocks = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}

			pattern = "\\s+Over-replicated blocks:\\s+(\\d+)\\s+\\(\\d+\\.\\d\\s+%\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.overReplicatedBlocks = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}

			pattern = "\\s+Under-replicated blocks:\\s+(\\d+)\\s+\\(\\d+\\.\\d\\s+%\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.underReplicatedBlocks = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}

			pattern = "\\s+Mis-replicated blocks:\\s+(\\d+)\\s+\\(\\d+\\.\\d\\s+%\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.misReplicatedBlocks = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}
			pattern = "\\s+Default replication factor:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.defaultReplicationFactor = Integer
						.parseInt(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}
			pattern = "\\s+Average block replication:\\s+(\\d+\\.\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.averageBlockReplication = Float
						.parseFloat(aLineFromFsckResponse.replaceAll(pattern,
								"$1"));
				return;
			}
			pattern = "\\s+Corrupt blocks:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.corruptBlocks = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
			pattern = "\\s+Missing replicas:\\s+(\\d+)\\s+\\(\\d+\\.\\d\\s+%\\)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.missingReplicas = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
			pattern = "\\s+Number of data-nodes:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.numberOfDatanodes = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
			pattern = "\\s+Number of racks:\\s+(\\d+)";
			if (aLineFromFsckResponse.matches(pattern)) {
				this.numberOfRacks = Integer.parseInt(aLineFromFsckResponse
						.replaceAll(pattern, "$1"));
				return;
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Status:" + status);
			sb.append(" ");
			sb.append("totalSizeInBytes:" + totalSizeInBytes);
			sb.append(" ");
			sb.append("totalDirs:" + totalDirs);
			sb.append(" ");
			sb.append("totalFiles:" + totalFiles);
			sb.append(" ");
			sb.append("totalSymlinks:" + totalSymlinks);
			sb.append(" ");
			sb.append("totalBlocksValidated:" + totalBlocksValidated);
			sb.append(" ");
			sb.append("averageBlockSize:" + averageBlockSize);
			sb.append(" ");
			sb.append("minimallyReplicatedBlocks:" + minimallyReplicatedBlocks);
			sb.append(" ");
			sb.append("overReplicatedBlocks:" + overReplicatedBlocks);
			sb.append(" ");
			sb.append("underReplicatedBlocks:" + underReplicatedBlocks);
			sb.append(" ");
			sb.append("misReplicatedBlocks:" + misReplicatedBlocks);
			sb.append(" ");
			sb.append("defaultReplicationFactor:" + defaultReplicationFactor);
			sb.append(" ");
			sb.append("averageBlockReplication:" + averageBlockReplication);
			sb.append(" ");
			sb.append("corruptBlocks:" + corruptBlocks);
			sb.append(" ");
			sb.append("missingReplicas:" + missingReplicas);
			sb.append(" ");
			sb.append("numberOfDatanodes:" + numberOfDatanodes);
			sb.append(" ");
			sb.append("numberOfRacks:" + numberOfRacks);
			sb.append("\n");
			return sb.toString();
		}

	}

}
