package hadooptest.tez.pig.localmode;


import hadooptest.tez.HtfPigBaseClass;
import hadooptest.tez.HtfTezUtils;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestPigTutorialScript1 extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "script1-local.pig";
	private static String OUTPUT_NAME = "/tmp/pigout/" + SCRIPT_NAME.replace(".pig", "");

	@Test
	public void testWithTez() throws Exception {
		int returnCode = runPigScriptLocally( SCRIPT_NAME,
				OUTPUT_NAME);
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirInHdfs() throws Exception {
		HtfTezUtils.delete(new File(OUTPUT_NAME));
		
	}
}
