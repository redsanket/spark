// Copyright 2018, Yahoo Inc.
package hadooptest.gdm.regression;

public class JsonObjectCreator {
    private JSONArray urls;
    private HadoopFileSystemHelper sourceHelper;

    public JsonObjectCreator(HadoopFileSystemHelper sourceHelper) {
        this.sourceHelper = sourceHelper;
    }

    public JsonObjectCreator createFile(JsonObjectCreator newjo, String path){
        newjo.sourceHelper.createFile(path);
        return this;
    }
}