// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.integration.metrics;

import static com.jayway.restassured.RestAssured.given;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;

public class NameNodeDFSMemoryInfo {
    private String nameNodeName;
    private String hadoopVersion;
    private String timeStamp; 
    private String totalMemoryCapacity; 
    private String usedMemoryCapacity; 
    private String remainingMemoryCapacity;
    private String missingBlocks;
    private String cookie;
    private ConsoleHandle consoleHandle;
    private static final String NAME_NODE_NAME = "gsbl90101.blue.ygrid.yahoo.com";
    
    public NameNodeDFSMemoryInfo() {
        this.consoleHandle = new ConsoleHandle();
        HTTPHandle httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
    }
    
    private void setNameNodeName(String nameNodeName) {
        this.nameNodeName = nameNodeName;
    }
    
    public String getNameNodeName() {
        return this.nameNodeName;
    }
    
    private void setHadoopVersion(String hadoopVersion) {
        this.hadoopVersion = hadoopVersion;
    }

    public String getHadoopVersion() {
        return this.hadoopVersion;
    }

    private void setCurrentTimeStamp(String currentTimeStamp) {
        this.timeStamp = currentTimeStamp;
    }
    
    public String getCurrentTimeStamp(){
        return this.timeStamp;
    }
    
    private void setTotalMemoryCapacity(String totalMemoryCapacity) {
        this.totalMemoryCapacity = totalMemoryCapacity;
    }
    
    public String getTotalMemoryCapacity() {
        return this.totalMemoryCapacity;
    }
    
    private void setUsedMemoryCapacity(String usedMemoryCapacity) {
        this.usedMemoryCapacity = usedMemoryCapacity;
    }
    
    public String getUsedMemoryCapacity() {
        return this.usedMemoryCapacity;
    }
    
    private void setRemainingMemoryCapacity(String remainingMemoryCapacity) {
        this.remainingMemoryCapacity = remainingMemoryCapacity;
    }
    
    public String getRemainingMemoryCapacity( ) {
        return this.remainingMemoryCapacity ;
    }
    
    private void setMissingBlocks(String missingBlocks) {
        this.missingBlocks = missingBlocks;
    }
    
    public String getMissingBlocks() {
        return this.missingBlocks;
    }
    
    public void getNameNodeDFSMemoryInfo() {
        Calendar initialCal = Calendar.getInstance();
        SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String currentiMin = feed_sdf.format(initialCal.getTime());


        String url = "http://" + NAME_NODE_NAME + ":" + MetricFields.NAME_NAME_METRIC_PORT + "/" + MetricFields.JMX_METRIC;
        TestSession.logger.info("url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
        String responseString = response.getBody().asString();

        JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
        TestSession.logger.info("obj = " + obj.toString());
        JSONArray jsonArray = obj.getJSONArray("beans");
        TestSession.logger.info("size = " + jsonArray.size());

        for ( int i=0; i<jsonArray.size() - 1 ; i++) {
            JSONObject theadJsonObject = jsonArray.getJSONObject(i);
            TestSession.logger.info(i + "   theadJsonObject = " + theadJsonObject.getString("name"));
            if (theadJsonObject.getString("name").equals("Hadoop:service=NameNode,name=FSNamesystem")) {
                TestSession.logger.info("Name = " + theadJsonObject.getString("name"));
                String nameNodeName = theadJsonObject.getString("tag.Hostname");
                String capacityTotalGB = theadJsonObject.getString("CapacityTotalGB");
                String capacityUsedGB = theadJsonObject.getString("CapacityUsedGB");
                String capacityRemainingGB = theadJsonObject.getString("CapacityRemainingGB");
                String missingBlocks = theadJsonObject.getString("MissingBlocks");
                TestSession.logger.info("nameNodeName -  " + nameNodeName );
                TestSession.logger.info("capacityTotalGB - " + capacityTotalGB);
                TestSession.logger.info("capacityUsedGB - " + capacityUsedGB);
                TestSession.logger.info("capacityRemainingGB - " + capacityRemainingGB);
                TestSession.logger.info("missingBlocks - " + missingBlocks);

                this.setNameNodeName(nameNodeName);
                this.setHadoopVersion("2.6.0.12.1504130654");
                this.setCurrentTimeStamp(currentiMin);
                this.setTotalMemoryCapacity(capacityTotalGB);
                this.setUsedMemoryCapacity(capacityUsedGB);
                this.setRemainingMemoryCapacity(capacityRemainingGB);
                this.setMissingBlocks(missingBlocks);
            }
        }
    }
    
}
