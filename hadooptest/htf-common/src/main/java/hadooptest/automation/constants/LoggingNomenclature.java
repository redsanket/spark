package hadooptest.automation.constants;

import hadooptest.automation.constants.HadooptestConstants;

import java.util.HashMap;

public class LoggingNomenclature {
	private static HashMap<String, String> historyserver;
	private static HashMap<String, String> resourcemanager;
	private static HashMap<String, String> namenode;
	private static HashMap<String, String> secondarynamenode;
	private static HashMap<String, String> datanode;
	private static final String USER = "user";
	private static final String PREFIX = "prefix";
	static {
		historyserver = new HashMap<String, String>();
		historyserver.put("prefix", HadooptestConstants.Miscellaneous.YARN);
		historyserver.put("user", HadooptestConstants.UserNames.MAPREDQA);

		resourcemanager = new HashMap<String, String>();
		resourcemanager.put("prefix", HadooptestConstants.Miscellaneous.YARN);
		resourcemanager.put("user", HadooptestConstants.UserNames.MAPREDQA);

		namenode = new HashMap<String, String>();
		namenode.put("prefix", HadooptestConstants.Miscellaneous.HADOOP);
		namenode.put("user", HadooptestConstants.UserNames.HDFSQA);

		secondarynamenode = new HashMap<String, String>();
		secondarynamenode.put("prefix",
				HadooptestConstants.Miscellaneous.HADOOP);
		secondarynamenode.put("user", HadooptestConstants.UserNames.HDFSQA);

		datanode = new HashMap<String, String>();
		datanode.put("prefix", HadooptestConstants.Miscellaneous.HADOOP);
		datanode.put("user", HadooptestConstants.UserNames.HDFSQA);

	}

	static public String getUser(String component) {
		if (component.equals(HadooptestConstants.NodeTypes.HISTORY_SERVER)) {
			return historyserver.get(USER);
		} else if (component
				.equals(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)) {
			return resourcemanager.get(USER);
		} else if (component.equals(HadooptestConstants.NodeTypes.NAMENODE)) {
			return namenode.get(USER);
		} else if (component.equals(HadooptestConstants.NodeTypes.DATANODE)) {
			return datanode.get(USER);
		} else {
			return null;
		}
	}

	static public String getLogPrefix(String component) {
		if (component.equals(HadooptestConstants.NodeTypes.HISTORY_SERVER)) {
			return historyserver.get(PREFIX);
		} else if (component
				.equals(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)) {
			return resourcemanager.get(PREFIX);
		} else if (component.equals(HadooptestConstants.NodeTypes.NAMENODE)) {
			return namenode.get(PREFIX);
		} else if (component.equals(HadooptestConstants.NodeTypes.DATANODE)) {
			return datanode.get(PREFIX);
		} else {
			return null;
		}
	}

}
