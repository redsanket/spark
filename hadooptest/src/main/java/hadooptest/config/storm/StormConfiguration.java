package hadooptest.config.storm;

import hadooptest.cluster.storm.StormCluster;

import backtype.storm.Config;

public class StormConfiguration extends Config {
	public static String TEST_OUTPUT_LOCATION = "test.output.location";
	
	public StormConfiguration(StormCluster cluster){
		super.putAll(cluster.getConfig());
	}
	
	public StormConfiguration(){
		
	}
	
	public void setTestOuputLoc(String loc) {
		this.put(TEST_OUTPUT_LOCATION, loc);
	}
	
	public String getTestOuputLoc() {
		return (String) this.get(TEST_OUTPUT_LOCATION);
	}
}

