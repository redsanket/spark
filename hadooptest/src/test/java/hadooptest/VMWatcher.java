/*
 * YAHOO!
 * 
 * A set of JUnit tests that act as wrappers to run the hadooptest
 * programmatic debugger.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest;

import static org.junit.Assert.*;

import hadooptest.cluster.VMViewer;

import org.junit.Test;

public class VMWatcher {

	@Test
	public void attachToExistingVMAndWatch() throws Exception {
		VMViewer.watchVariable();
	}
	
	@Test
	public void attachToExistingVMAndBreakpoint() throws Exception {
		VMViewer.watchBreakpoint();
	}

}
