// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import static org.junit.Assert.fail;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Target {
    private String targetName;
    private JSONArray targetPaths;
    private JSONObject targetJsonObject ;
    private int targetPathsCount = 0;
    
    public Target() {
        this.targetPaths = new JSONArray();
        this.targetJsonObject = new JSONObject();
    }

    public Target targetName(String targetName) {
        this.targetName = targetName;
        this.targetJsonObject.put("TargetCluster" , this.targetName);
        return this;
    }
    
    public Target addPath(String pathValue) {
        JSONObject pathJSONObject = new JSONObject();
        if (this.targetPaths == null) {
            this.targetPaths = new JSONArray();
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
    
    public Target retentionNumber(String number) {
        this.targetJsonObject.put("RetentionNumber", number);
        return this;
    }
    
    public Target retentionPolicy(String policy) {
        this.targetJsonObject.put("RetentionPolicy", policy);
        return this;
    }
    
    /**
     * Adds TargetLatency to the target
     * 
     * @param latency
     * @return the modified Target
     */
    public Target latency(String latency) {
        this.targetJsonObject.put("TargetLatency", latency);
        return this;
    }
    
    public Target numMaps(String targetNumMaps) {
        this.targetJsonObject.put("NumMaps", targetNumMaps);
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
