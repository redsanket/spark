package hadooptest.automation.utils.http;

import hadooptest.automation.constants.HadooptestConstants;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.log4j.Logger;

public class ResourceManagerHttpUtils {
	Logger logger = Logger.getLogger(ResourceManagerHttpUtils.class);
	Properties crossClusterProperties;

	public ResourceManagerHttpUtils() {
		String workingDir = System
				.getProperty(HadooptestConstants.Miscellaneous.USER_DIR);
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties.load(new FileInputStream(workingDir
					+ "/../conf/CrossCluster/Resource.properties"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	public String about(String clusterName) {
		HTTPHandle httpHandle = new HTTPHandle();
		String resource = "/cluster/cluster";
		logger.info("Resource Manager URL read as"
				+ crossClusterProperties.getProperty(clusterName.toLowerCase() + "."
						+ HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		String resourceManager = crossClusterProperties.getProperty(clusterName
				+ "." + HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		HttpMethod getMethod = httpHandle.makeGET(resourceManager, resource,
				null);
		Response response = new Response(getMethod, false);
		return response.getResponseBodyAsString();
	}

	public String getHadoopVersion(String clusterName) {
		String aboutResponse = this.about(clusterName);
		boolean trippedOnHadoopVersion = false;
		boolean trippedOnTd = false;
		String lineContainingVersion = null;

		for (String aLine : aboutResponse.split("\n")) {

			if (aLine.contains("Hadoop version:")) {
				trippedOnHadoopVersion = true;
				continue;
			}
			if (trippedOnHadoopVersion) {
				if (aLine.contains("<td>")) {
					trippedOnTd = true;
					continue;
				}
			}
			if (trippedOnHadoopVersion && trippedOnTd) {
				lineContainingVersion = aLine;
				break;
			}
		}
		logger.info("Returning version line as[" + lineContainingVersion.trim() + "]");
		lineContainingVersion = lineContainingVersion.trim();
		return lineContainingVersion.split("\\s+")[0];
	}

}
