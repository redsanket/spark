package gdm.regression;

import hadooptest.TestSession;
import org.junit.BeforeClass;
import org.junit.Test;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestHcatDataHandle extends TestSession{
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Test
    public void runTest() throws Exception{
        TestSession.logger.info("mmukhi- this works");
        Date date = new Date();
        String tableSuffix = String.valueOf(date.getTime());
        String tableName = "HTFTest_" + tableSuffix;
        String result = HCatDataHandle.createTable("qe6blue",tableName);
        if(result == null){
            TestSession.logger.info("error creating table");
        }
        if(HCatDataHandle.doesTableExist("qe6blue", tableName)){
            TestSession.logger.info(tableName + " exists on qe6blue");
        }else{
            TestSession.logger.info("Uh oh..");
        }
        String bogusTableName="abogustable";
        if(!HCatDataHandle.doesTableExist("qe6blue", bogusTableName)){
            TestSession.logger.info(bogusTableName + " doesn't exist on qe6blue");
        }else{
            TestSession.logger.info("Uh oh..");
        }
        
        boolean status = HCatDataHandle.addPartition("qe6blue", tableName, "201604010101");
        if(status){
            TestSession.logger.info("Partition added successfully");
        }else{
            TestSession.logger.info("An error occured while adding partition");
        }
        
        status = HCatDataHandle.doesPartitionExist("qe6blue", tableName, "201604010101");
        if(status){
            TestSession.logger.info("partition exists");
        }
        else{
            TestSession.logger.info("error...");
        }
        status = HCatDataHandle.doesPartitionExist("qe6blue", tableName, "20160401010");
        if(status){
            TestSession.logger.info("partition rightly doesnt exist");
        }else{
            TestSession.logger.info("error...");
        }
    }

}
