package hadooptest.automation.constants;

public class HadooptestConstants {

	// Node Types
	public static class NodeTypes {
		public static String DATANODE = "datanode";
		public static String NAMENODE = "namenode";
		public static String SECONDARY_NAMENODE = "secondarynamenode";
		public static String RESOURCE_MANAGER = "resourcemanager";
		public static String HISTORY_SERVER = "historyserver";
		public static String NODE_MANAGER = "nodemanager";
	}

	// Config file names
	public static class ConfFileNames {
		public static String CORE_SITE_XML = "core-site.xml";
		public static String HDFS_SITE_XML = "hdfs-site.xml";
		public static String YARN_SITE_XML = "yarn-site.xml";
		public static String MAPRED_SITE_XML = "mapred-site.xml";
	}

	// User names
	public static class UserNames {
		public static String HADOOPQA = "hadoopqa";
		public static String MAPREDQA = "mapredqa";
		public static String HDFSQA = "hdfsqa";
		public static String DFSLOAD = "dfsload";
		public static String HDFS = "hdfs";
	}

	// Log
	public static class Log {
		public static String DOT_LOG = ".log";
		public static String LOG_LOCATION = "/home/gs/var/log/";
	}

	// Misc
	public static class Miscellaneous {
		public static String HADOOP = "hadoop";
		public static String YARN = "yarn";
		public static String MEMORY = "MEMORY";
		public static String CPU = "CPU";
		public static String FILES = "FILES";
		public static String USER_DIR = "user.dir";
		public static String SSH_AGENT_FILE = "SSH_AGENT_FILE";
		public static String EXCEPTIONS = "EXCEPTIONS";
		public static String TEST_STATUS = "TEST_STATUS";
		public static String TEST_DURATION = "TEST_DURATION";
		public static String JOB_STATUS = "JOB_STATUS";
		public static String MAP_TASK_STATUS = "MAP_TASK_STATUS";
		public static String REDUCE_TASK_STATUS = "REDUCE_TASK_STATUS";
	}

	// Commands
	public static class ShellCommand {
		public static String PDSH = "/home/y/bin/pdsh";
		public static String GREP = "/bin/grep";
		public static String TAC = "/usr/bin/tac";
		public static String CAT = "/bin/cat";
		public static String SED = "/bin/sed";
		public static String PIPE = "|";
		
	}

	// Locations
	public static class Location {
		public static String CORE_SITE_XML = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/conf/hadoop/core-site.xml";
		public static String HDFS_SITE_XML = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/conf/hadoop/hdfs-site.xml";
		public static String YARN_SITE_XML = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/conf/hadoop/yarn-site.xml";
		public static String MAPRED_SITE_XML = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/conf/hadoop/mapred-site.xml";

		public static class Keytab {
			public static String HADOOPQA = "/homes/hadoopqa/hadoopqa.dev.headless.keytab";
			public static String DFSLOAD = "/homes/dfsload/dfsload.dev.headless.keytab";
		}

		public static class TestProperties {
			public static String CrossClusterProperties = System
					.getProperty(HadooptestConstants.Miscellaneous.USER_DIR)
					+ "/conf/CrossCluster/Resource.properties";

		}
		public static class Binary {
			public static String HADOOP = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME") + "/share/hadoop/bin/hadoop";
			public static String HDFS = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME") + "/share/hadoop/bin/hdfs";
			public static String YARN = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME") + "/share/hadoop/bin/yarn";
			public static String MAPRED = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME") + "/share/hadoop/bin/mapred";
			public static String PERL = "/usr/local/bin/perl";
		}
		public static class Conf {
			public static String DIRECTORY = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME") + "/conf/hadoop/";
		}

	}

	// Schemas
	public static class Schema {
		public static String HTTP = "http://";
		public static String HDFS = "hdfs://";
		public static String WEBHDFS = "webhdfs://";
		public static String HFTP = "hftp://";
		public static String NONE = "";
	}
	// Ports
	public static class Ports {
		public static String HDFS = "8020";

	}

}
