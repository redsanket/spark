package gdm.regression;

import hadooptest.TestSession;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHcatDataHandle extends TestSession{
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Test
    public void runTest() throws Exception{
        TestSession.logger.info("mmukhi- this works");
        String tableName = HCatDataHandle.createTable("qe6blue");
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
    }

}
