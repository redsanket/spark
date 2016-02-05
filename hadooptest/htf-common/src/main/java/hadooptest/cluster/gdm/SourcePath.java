// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SourcePath {
    private JSONArray sourcePaths;
    
    public SourcePath() {
        this.sourcePaths = new JSONArray();
    }
    
    public SourcePath addSourcePath(String pathValue) {
        if (this.sourcePaths == null) {
            this.sourcePaths = new JSONArray();
        }
        this.sourcePaths.add(pathValue);
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
