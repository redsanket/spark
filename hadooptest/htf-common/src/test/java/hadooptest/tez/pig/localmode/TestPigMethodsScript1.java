package hadooptest.tez.pig.localmode;

import hadooptest.tez.utils.HtfPigBaseClass;
import hadooptest.tez.utils.HtfTezUtils;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestPigMethodsScript1 extends HtfPigBaseClass {
	private static String SCRIPT_NAME = "pig_methods_script1.pig";
	private static String OUTPUT_NAME = "/tmp/pigout/"
			+ SCRIPT_NAME.replace(".pig", "");

	@Before
	public void ensureOutputDirIsDeleted() throws Exception {
		FileUtils.deleteQuietly(new File(OUTPUT_NAME));

	}
	
	@Test
	public void testWithTez() throws Exception {
		int returnCode = runPigScriptLocally(SCRIPT_NAME, OUTPUT_NAME);
		Assert.assertTrue(returnCode == 0);
	}

	@After
	public void deleteOutputDirInHdfs() throws Exception {
		FileUtils.deleteQuietly(new File(OUTPUT_NAME));

	}
}
