package hadooptest.tez.mapreduce.examples.extensions;

import hadooptest.tez.utils.HtfTezUtils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.examples.UnionExample;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

/**
 * Changes to be done, should the file need to be refreshed. 1) Staging dir:
 * conf.setBoolean("tez.local.mode")
 * 
 */
public class UnionExampleExtendedForTezHTF extends UnionExample {
	
	public boolean run(String inputPath, String outputPath, Configuration conf,
			String mode, boolean session, String testName) throws Exception {	
		System.out.println("Running UnionExample");
																	
		// conf and UGI
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}
		UserGroupInformation.setConfiguration(tezConf);

		// staging dir
		FileSystem fs = FileSystem.get(tezConf);
		UserGroupInformation.setConfiguration(tezConf);
		Path stagingDir = fs.makeQualified(new Path(tezConf
				.get(TezConfiguration.TEZ_AM_STAGING_DIR)));

		// No need to add jar containing this class as assumed to be part of
		// the tez jars.

		// TEZ-674 Obtain tokens based on the Input / Output paths. For now
		// assuming staging dir
		// is the same filesystem as the one used for Input/Output.

		TezClient tezSession = TezClient.create("UnionExampleSession", tezConf);
		tezSession.start();

		DAGClient dagClient = null;

		try {
			if (fs.exists(new Path(outputPath))) {
				throw new FileAlreadyExistsException("Output directory "
						+ outputPath + " already exists");
			}

			Map<String, LocalResource> localResources = new TreeMap<String, LocalResource>();

			DAG dag = createDAG(fs, tezConf, localResources, stagingDir,
					inputPath, outputPath);

			tezSession.waitTillReady();
			dagClient = tezSession.submitDAG(dag);

			// monitoring
			DAGStatus dagStatus = dagClient
					.waitForCompletionWithStatusUpdates(EnumSet
							.of(StatusGetOpts.GET_COUNTERS));
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				System.out.println("DAG diagnostics: "
						+ dagStatus.getDiagnostics());
				return false;
			}
			return true;
		} finally {
			fs.delete(stagingDir, true);
			tezSession.stop();
		}
	}

	private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
			Map<String, LocalResource> localResources, Path stagingDir,
			String inputPath, String outputPath) throws IOException {
		DAG dag = DAG.create("UnionExample");

		int numMaps = -1;
		Configuration inputConf = new Configuration(tezConf);
		MRInput.MRInputConfigBuilder configurer = MRInput.createConfigBuilder(
				inputConf, TextInputFormat.class, inputPath);
		DataSourceDescriptor dataSource = configurer.generateSplitsInAM(false)
				.build();

		Vertex mapVertex1 = Vertex.create("map1",
				ProcessorDescriptor.create(TokenProcessor.class.getName()),
				numMaps).addDataSource("MRInput", dataSource);

		Vertex mapVertex2 = Vertex.create("map2",
				ProcessorDescriptor.create(TokenProcessor.class.getName()),
				numMaps).addDataSource("MRInput", dataSource);

		Vertex mapVertex3 = Vertex.create("map3",
				ProcessorDescriptor.create(TokenProcessor.class.getName()),
				numMaps).addDataSource("MRInput", dataSource);

		Vertex checkerVertex = Vertex.create("checker",
				ProcessorDescriptor.create(UnionProcessor.class.getName()), 1);

		Configuration outputConf = new Configuration(tezConf);
		DataSinkDescriptor od = MROutput.createConfigBuilder(outputConf,
				TextOutputFormat.class, outputPath).build();
		checkerVertex.addDataSink("union", od);

		Configuration allPartsConf = new Configuration(tezConf);
		DataSinkDescriptor od2 = MROutput.createConfigBuilder(allPartsConf,
				TextOutputFormat.class, outputPath + "-all-parts").build();
		checkerVertex.addDataSink("all-parts", od2);

		Configuration partsConf = new Configuration(tezConf);
		DataSinkDescriptor od1 = MROutput.createConfigBuilder(partsConf,
				TextOutputFormat.class, outputPath + "-parts").build();
		VertexGroup unionVertex = dag.createVertexGroup("union", mapVertex1,
				mapVertex2);
		unionVertex.addDataSink("parts", od1);

		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder(Text.class.getName(), IntWritable.class.getName(),
						HashPartitioner.class.getName()).build();

		dag.addVertex(mapVertex1)
				.addVertex(mapVertex2)
				.addVertex(mapVertex3)
				.addVertex(checkerVertex)
				.addEdge(
						Edge.create(mapVertex3, checkerVertex,
								edgeConf.createDefaultEdgeProperty()))
				.addEdge(
						GroupInputEdge.create(
								unionVertex,
								checkerVertex,
								edgeConf.createDefaultEdgeProperty(),
								InputDescriptor
										.create(ConcatenatedMergedKeyValuesInput.class
												.getName())));
		return dag;
	}

}
