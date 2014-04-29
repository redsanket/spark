package hadooptest.hadoop.regression.dfs;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(ZParallelized.class)
public class ZTestParallelizedClass
{
    // This is the parameter for each instance of the test.
    private String feature;

    public ZTestParallelizedClass(String feature)
    {
         this.feature = feature;
    }

    @Parameters
    public static Collection<Object[]> getParameters()     
    {       
    	
    	return Arrays.asList(new Object[][] { { "wilma" }, { "betty" }, { "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" },{ "betty" }});
    }
 
    @Test
    public void testOneFeature() throws InterruptedException     
    {
    	int timeout = 1 + (int)(Math.random() * ((10 - 1) + 1));
    	
        System.out.println("Invoked with :" + feature);
        Thread.sleep(timeout*1000);
    }
} 