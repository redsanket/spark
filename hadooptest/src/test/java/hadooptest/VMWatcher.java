/*
 * YAHOO!
 * 
 * A set of JUnit tests that act as wrappers to run the hadooptest
 * programmatic debugger.
 * 
 * Rick Bernotas (rbernota)
 */

package hadooptest;

import static org.junit.Assert.*;

import hadooptest.cluster.VMViewer;

import org.junit.Test;

public class VMWatcher {

	@Test
	public void attachToExistingVMAndWatch() throws Exception {
		VMViewer debugger = new VMViewer();
		debugger.watchVariable("org.apache.hadoop.mapreduce.Job", "state");
	}
	
	@Test
	public void attachToExistingVMAndBreakpoint() throws Exception {
		VMViewer debugger = new VMViewer();
		debugger.breakpointMethod("org.apache.hadoop.mapreduce.Reducer", "run");
	}
	
	@Test
	public void listenForVMAndWatch() throws Exception {
		VMViewer debugger = new VMViewer("WATCH", "org.apache.hadoop.mapreduce.Job", "state");
	}

	@Test
	public void listenForVMAndBreakpoint() throws Exception {
		new VMViewer("BREAK", "org.apache.hadoop.mapreduce.Reducer", "run");
	}
}
