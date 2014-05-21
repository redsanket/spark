package hadooptest.automation.factories.yarnClientImpl;

import hadooptest.automation.utils.http.ResourceManagerHttpUtils;

public class YarnClientImplFactory {
	static String version = null;

	static public IYarnClientFunctionality get() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		if (version == null) {
			ResourceManagerHttpUtils rmUtils = new ResourceManagerHttpUtils();
			version = rmUtils.getHadoopVersion(System.getProperty("CLUSTER_NAME"));
		
		}
		if (version.startsWith("0.23")) {
			return new YarnClientImplVdot23();
		} else {
			return new YarnClientImplV2();
		}
	}	

}
