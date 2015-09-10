package hadooptest.cluster.gdm;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SourcePath {
	private JSONArray sourcePaths;
	private int sourcePathsCount ;
	
	public SourcePath() {
		this.sourcePaths = new JSONArray();
		this.sourcePathsCount = 0;
	}
	
/*	public SourcePath(SourcePath sourceObject) {
		sourcePaths = sourceObject.sourcePaths;
	}*/
	
	public SourcePath addSourcePath(String pathValue) {
		JSONObject pathJSONObject = new JSONObject();
		if (this.sourcePaths == null) {
			this.sourcePaths = new JSONArray();
			this.sourcePathsCount = 0;
		}
		pathJSONObject.put("Path", pathValue);
		this.sourcePaths.add(this.sourcePathsCount, pathJSONObject);
		this.sourcePathsCount++;
		return this;
	}

	public JSONArray getSourcePath() throws Exception {
		if (this.sourcePaths == null) {
			throw new Exception("There is no any path specified.");
		}
		return sourcePaths;
	}
	
	public String toString() {
		String returnValue = null;
		try {
			returnValue = getSourcePath().toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnValue;
	}
}
