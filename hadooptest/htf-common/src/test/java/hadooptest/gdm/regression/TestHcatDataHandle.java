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
        HCatDataHandle.createTable("qe6blue");
    }

}
