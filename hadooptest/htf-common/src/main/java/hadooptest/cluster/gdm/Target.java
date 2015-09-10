package hadooptest.cluster.gdm;

import static org.junit.Assert.fail;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Target {
	private String targetName;
	private JSONArray targetPaths;
	private int targetPathsCount ;
	private String targetRetentionDays;
	private String targetNumMaps;
	private JSONObject targetJsonObject ;
	
	public Target() {
		this.targetPaths = new JSONArray();
		this.targetPathsCount = 0;
		this.targetJsonObject = new JSONObject();
	}
	
/*	public Target(Target targetObject) {
		targetName = targetObject.targetName;
		targetPaths = targetObject.targetPaths;
		targetJsonObject = new JSONObject();
	}*/
	
	public Target targetName(String targetName) {
		this.targetName = targetName;
		this.targetJsonObject.put("TargetCluster" , this.targetName);
		return this;
	}
	
	public Target addPath(String pathValue) {
		JSONObject pathJSONObject = new JSONObject();
		if (this.targetPaths == null) {
			this.targetPaths = new JSONArray();
			this.targetPathsCount = 0;	
		}
		pathJSONObject.put("Path", pathValue);
		this.targetPaths.add(this.targetPathsCount, pathJSONObject);
		this.targetPathsCount++;
		return this;
	}
	
	public JSONObject getTarget() throws Exception {
		targetJsonObject.put("TargetPaths", this.targetPaths.toString());
		return targetJsonObject;
	}
	
	public Target retentionDays(String targetRetentionDays) {
		this.targetRetentionDays = targetRetentionDays;
		this.targetJsonObject.put("RetentionDays", this.targetRetentionDays);
		return this;
	}
	
	public Target numMaps(String targetNumMaps) {
		this.targetNumMaps = targetNumMaps;
		this.targetJsonObject.put("NumMaps", this.targetNumMaps);
		return this;
	}
	
	public String toString(){ 
		String returnValue = null;
		try {
			returnValue = this.getTarget().toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnValue;
	}
}
