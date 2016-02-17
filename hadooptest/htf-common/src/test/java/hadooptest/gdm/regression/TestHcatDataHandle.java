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
            System.out.println("error creating table");
        }
        if(HCatDataHandle.doesTableExist("qe6blue", tableName)){
            System.out.println(tableName + " exists on qe6blue");
        }else{
            System.out.println("Uh oh..");
        }
        tableName="abogustable";
        if(!HCatDataHandle.doesTableExist("qe6blue", tableName)){
            System.out.println(tableName + " doesn't exist on qe6blue");
        }else{
            System.out.println("Uh oh..");
        }
        
        boolean status = HCatDataHandle.addPartition("qe6blue", tableName, "201604010101");
        if(status){
            System.out.println("Partition added successfully");
        }else{
            
        }
    }

}
