package hadooptest.tez.examples.extensions;

import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

/**
 * Simple example to perform WordCount using Tez API's. WordCount is the
 * HelloWorld program of distributed data processing and counts the number of
 * occurrences of a word in a distributed text data set.
 */
public class WordCountSpeculativeExecutor extends Configured implements Tool {
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	static String INPUT = "Input";
	static String OUTPUT = "Output";
	static String TOKENIZER = "Tokenizer";
	static String SUMMATION = "Summation";
	protected static ApplicationId appId;

	/*
	 * Example code to write a processor in Tez. Processors typically apply the
	 * main application logic to the data. TokenProcessor tokenizes the input
	 * data. It uses an input that provide a Key-Value reader and writes output
	 * to a Key-Value writer. The processor inherits from SimpleProcessor since
	 * it does not need to handle any advanced constructs for Processors.
	 */
	public static class SpecExTokenProcessor extends SimpleProcessor {
		private final ProcessorContext myContext;
		IntWritable one = new IntWritable(1);
		Text word = new Text();

		public SpecExTokenProcessor(ProcessorContext context) {
			super(context);
			this.myContext = context;
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 1);
			Preconditions.checkArgument(getOutputs().size() == 1);
			// the recommended approach is to cast the reader/writer to a
			// specific type instead
			// of casting the input/output. This allows the actual input/output
			// type to be replaced
			// without affecting the semantic guarantees of the data type that
			// are represented by
			// the reader and writer.
			// The inputs/outputs are referenced via the names assigned in the
			// DAG.
			KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT)
					.getReader();
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(
					SUMMATION).getWriter();
			while (kvReader.next()) {
				StringTokenizer itr = new StringTokenizer(kvReader
						.getCurrentValue().toString());
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					// Count 1 every time a word is observed. Word is the key a
					// 1 is the value
//					System.out.println("Processing taskId " + myContext.getTaskIndex() +" now......");
					if (myContext.getTaskIndex() == 5) {						
						if (myContext.getTaskAttemptNumber() == 0) {
							System.out.println("Processing taskAttempt 0 now......."
									+ "going slow for " + myContext.getTaskIndex());
							Thread.sleep(1000);
							kvWriter.write(word, one);
						} else {
							kvWriter.write(word, one);
						}
					} else {
						kvWriter.write(word, one);
					}
				}
			}
		}

	}

	/*
	 * Example code to write a processor that commits final output to a data
	 * sink The SumProcessor aggregates the sum of individual word counts
	 * generated by the TokenProcessor. The SumProcessor is connected to a
	 * DataSink. In this case, its an Output that writes the data via an
	 * OutputFormat to a data sink (typically HDFS). Thats why it derives from
	 * SimpleMRProcessor that takes care of handling the necessary output commit
	 * operations that makes the final output available for consumers.
	 */
	public static class SumProcessor extends SimpleMRProcessor {
		public SumProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 1);
			Preconditions.checkArgument(getOutputs().size() == 1);
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT)
					.getWriter();
			// The KeyValues reader provides all values for a given key. The
			// aggregation of values per key
			// is done by the LogicalInput. Since the key is the word and the
			// values are its counts in
			// the different TokenProcessors, summing all values per key
			// provides the sum for that word.
			KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(
					TOKENIZER).getReader();
			while (kvReader.next()) {
				Text word = (Text) kvReader.getCurrentKey();
				int sum = 0;
				for (Object value : kvReader.getCurrentValues()) {
					sum += ((IntWritable) value).get();
				}
				kvWriter.write(word, new IntWritable(sum));
			}
			// deriving from SimpleMRProcessor takes care of committing the
			// output
			// It automatically invokes the commit logic for the OutputFormat if
			// necessary.
		}
	}

	private DAG createDAG(TezConfiguration tezConf, String inputPath,
			String outputPath, int numPartitions) throws IOException, URISyntaxException {

		// Create the descriptor that describes the input data to Tez. Using
		// MRInput to read text
		// data from the given input path. The TextInputFormat is used to read
		// the text data.
		
		DataSourceDescriptor dataSource = MRInput.createConfigBuilder(
				new Configuration(tezConf), TextInputFormat.class, inputPath)
				.build();

		// Create a descriptor that describes the output data to Tez. Using
		// MROoutput to write text
		// data to the given output path. The TextOutputFormat is used to write
		// the text data.
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(
				new Configuration(tezConf), TextOutputFormat.class, outputPath)
				.build();

		// Create a vertex that reads the data from the data source and
		// tokenizes it using the
		// TokenProcessor. The number of tasks that will do the work for this
		// vertex will be decided
		// using the information provided by the data source descriptor.
		Vertex tokenizerVertex = Vertex
				.create(TOKENIZER,
						ProcessorDescriptor.create(SpecExTokenProcessor.class
								.getName())).addDataSource(INPUT, dataSource);

		
		// Create the edge that represents the movement and semantics of data
		// between the producer
		// Tokenizer vertex and the consumer Summation vertex. In order to
		// perform the summation in
		// parallel the tokenized data will be partitioned by word such that a
		// given word goes to the
		// same partition. The counts for the words should be grouped together
		// per word. To achieve this
		// we can use an edge that contains an input/output pair that handles
		// partitioning and grouping
		// of key value data. We use the helper OrderedPartitionedKVEdgeConfig
		// to create such an
		// edge. Internally, it sets up matching Tez inputs and outputs that can
		// perform this logic.
		// We specify the key, value and partitioner type. Here the key type is
		// Text (for word), the
		// value type is IntWritable (for count) and we using a hash based
		// partitioner. This is a helper
		// object. The edge can be configured by configuring the input, output
		// etc individually without
		// using this helper.
		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder(Text.class.getName(), IntWritable.class.getName(),
						HashPartitioner.class.getName()).build();

		// Create a vertex that reads the tokenized data and calculates the sum
		// using the SumProcessor.
		// The number of tasks that do the work of this vertex depends on the
		// number of partitions used
		// to distribute the sum processing. In this case, its been made
		// configurable via the
		// numPartitions parameter.
		Vertex summationVertex = Vertex.create(SUMMATION,
				ProcessorDescriptor.create(SumProcessor.class.getName()),
				numPartitions).addDataSink(OUTPUT, dataSink);

		// No need to add jar containing this class as assumed to be part of the
		// Tez jars. Otherwise
		// we would have to add the jars for this code as local files to the
		// vertices.

		// Create DAG and add the vertices. Connect the producer and consumer
		// vertices via the edge
		DAG dag = DAG.create("TezSpecExecWordCount");
		dag.addVertex(tokenizerVertex)
				.addVertex(summationVertex)
				.addEdge(
						Edge.create(tokenizerVertex, summationVertex,
								edgeConf.createDefaultEdgeProperty()));
		dag.addTaskLocalFiles(getLocalResources(tezConf));
		return dag;
	}

	protected Map<String, LocalResource> getLocalResources(
			TezConfiguration tezConf) throws IOException, URISyntaxException {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		Path stagingDir = TezCommonUtils.getTezBaseStagingPath(tezConf);

		// staging dir
		FileSystem fs = FileSystem.get(tezConf);
		Path jobJar = new Path(stagingDir, "job.jar");
		if (fs.exists(jobJar)) {
			fs.delete(jobJar, true);
		}
		fs.copyFromLocalFile(getCurrentJarURL(), jobJar);

		localResources.put("job.jar", createLocalResource(fs, jobJar));
		return localResources;
	}

	protected LocalResource createLocalResource(FileSystem fs, Path file)
			throws IOException {
		final LocalResourceType type = LocalResourceType.FILE;
		final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
		FileStatus fstat = fs.getFileStatus(file);
		org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils
				.getYarnUrlFromPath(file);
		long resourceSize = fstat.getLen();
		long resourceModificationTime = fstat.getModificationTime();
		LocalResource lr = Records.newRecord(LocalResource.class);
		lr.setResource(resourceURL);
		lr.setType(type);
		lr.setSize(resourceSize);
		lr.setVisibility(visibility);
		lr.setTimestamp(resourceModificationTime);
		return lr;
	}

	public static Path getCurrentJarURL() throws URISyntaxException {
		return new Path(WordCountSpeculativeExecutor.class
				.getProtectionDomain().getCodeSource().getLocation().toURI());
	}

	private static void printUsage() {
		System.err.println("Usage: " + " wordcount in out [numPartitions]");
		ToolRunner.printGenericCommandUsage(System.err);
	}

	public boolean run(String inputPath, String outputPath, Configuration conf,
			int numPartitions, String mode, Session session,
			TimelineServer timelineServer, String testName) throws Exception {
		System.out.println("Running SpecExecWordCount");
		Configuration baseConf = new Configuration();
		baseConf.setBoolean("mapreduce.map.speculative", true);
		baseConf.setBoolean("tez.local.mode", false);
		baseConf.setBoolean("tez.use.cluster.hadoop-libs", true);
		baseConf.set("mapreduce.job.acl-view-job", "*");
		baseConf.set("mapreduce.framework.name", "yarn-tez");
		baseConf.setBoolean("tez.am.speculation.enabled", true);
		baseConf.setBoolean("tez.am.speculation.enabled.default", true);
		baseConf.set("tez.am.legacy.speculative.slowtask.threshold", "1");
		baseConf.set("tez.queue.name", "default");
		
//		baseConf.set("tez.am.resource.memory.mb", "2048");		
//		baseConf.set("tez.am.java.opts", "-Xmx4096m");
//		baseConf.set("tez.task.resource.memory.mb","3072");

		/**
		 * 	Int value. Time (in seconds) for which the Tez AM should wait for a
		 * DAG to be submitted before shutting down. Only relevant in session
		 * mode.
		 */

		baseConf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, 30);		
		
		TezConfiguration tezConf;
		tezConf = new TezConfiguration(baseConf);
		tezConf.addResource(new FileInputStream("/home/gs/conf/tez/current/tez-site.xml"));
		System.out.println("tez.task.resource.memory.mb==" + tezConf.get("tez.task.resource.memory.mb"));


		UserGroupInformation.setConfiguration(tezConf);

		// Create the TezClient to submit the DAG. Pass the tezConf that has all
		// necessary global and
		// dag specific configurations
		TezClient tezClient = TezClient.create("SpecExWordCount", tezConf);
		// TezClient must be started before it can be used
		tezClient.start();

		try {
			DAG dag = createDAG(tezConf, inputPath, outputPath, numPartitions);

			// check that the execution environment is ready
			tezClient.waitTillReady();
			// submit the dag and receive a dag client to monitor the progress
			DAGClient dagClient = tezClient.submitDAG(dag);
			
			appId = tezClient.getAppMasterApplicationId();
			// monitor the progress and wait for completion. This method blocks
			// until the dag is done.
			DAGStatus dagStatus = dagClient
					.waitForCompletionWithStatusUpdates(null);
			// check success or failure and print diagnostics
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				System.out.println("WordCount failed with diagnostics: "
						+ dagStatus.getDiagnostics());
				return false;
			}
			return true;
		} finally {
			// stop the client to perform cleanup
			tezClient.stop();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2 || otherArgs.length > 3) {
			printUsage();
			return 2;
		}
		WordCountSpeculativeExecutor job = new WordCountSpeculativeExecutor();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new WordCountSpeculativeExecutor(), args);
		System.exit(res);
	}
}
