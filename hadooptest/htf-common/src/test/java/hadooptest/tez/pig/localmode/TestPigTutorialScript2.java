package hadooptest.tez.pig.localmode;


import hadooptest.tez.utils.HtfPigBaseClass;
import hadooptest.tez.utils.HtfTezUtils;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestPigTutorialScript2 extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "script2-local.pig";
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
