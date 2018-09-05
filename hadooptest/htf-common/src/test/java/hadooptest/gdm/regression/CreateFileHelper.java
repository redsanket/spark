// Copyright 2018, Yahoo Inc.
package hadooptest.gdm.regression;

import java.io.IOException;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class CreateFileHelper {
    private HadoopFileSystemHelper sourceHelper;
    JSONObject fileContent = new JSONObject();
    JSONArray urls = new JSONArray();
    JSONObject jsonObject = new JSONObject();

    public CreateFileHelper(HadoopFileSystemHelper sourceHelper) {
        this.sourceHelper = sourceHelper;
    }

    public CreateFileHelper createFile(String path) throws IOException, InterruptedException {
        this.sourceHelper.createFile(path);
        return this;
    }

    public CreateFileHelper createFile(String path, String fileContent) throws IOException, InterruptedException {
        this.sourceHelper.createFile(path, fileContent);
        return this;
    }

    public CreateFileHelper addJsonObject(String path) {
        this.jsonObject.put("url", path);
        urls.add(jsonObject);
        return this;
    }

    public String generateFileContent() {
        this.fileContent.put("entries", urls);
        this.urls.clear();
        return fileContent.toString();
    }

}