/*
 * YAHOO!
 * 
 * A class which allows for programmatic debugging of a debuggable instance
 * of a Hadoop cluster.  Currently can set a watch variable and method
 * breakpoints.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster;

import java.io.IOException;
import java.util.List;

import com.sun.jdi.AbsentInformationException;
import com.sun.jdi.IncompatibleThreadStateException;
import com.sun.jdi.Field;
import com.sun.jdi.LocalVariable;
import com.sun.jdi.Location;
import com.sun.jdi.Method;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StackFrame;
import com.sun.jdi.StringReference;
import com.sun.jdi.ThreadReference;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventIterator;
import com.sun.jdi.event.EventQueue;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.ModificationWatchpointEvent;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequest;
import com.sun.jdi.request.EventRequestManager;
import com.sun.jdi.request.ModificationWatchpointRequest;

import org.apache.hadoop.mapreduce.Reducer.Context;

public class VMViewer {

	public static final String CLASS_NAME_WATCH = "org.apache.hadoop.mapreduce.Job";
	public static final String CLASS_NAME_BREAKPOINT = "org.apache.hadoop.mapreduce.Reducer";
	public static final String FIELD_NAME_WATCH = "state";
	public static final String METHOD_NAME_WATCH = "updateStatus";
	public static final String METHOD_NAME_BREAKPOINT = "run";

	public static void watchVariable() 
			throws IOException, 
			InterruptedException 
			{
		
		VirtualMachine vm = new VMAttach().connect(8008);

		List<ReferenceType> referenceTypes = vm.classesByName(CLASS_NAME_WATCH);
		for (ReferenceType refType : referenceTypes) {
			addFieldViewer(vm, refType);
		}
		
		addClassViewer(vm, CLASS_NAME_WATCH);

		vm.resume();

		EventQueue eventQueue = vm.eventQueue();
		while (true) {
			EventSet eventSet = eventQueue.remove();
			for (Event event : eventSet) {
				if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
					return;
				} else if (event instanceof ClassPrepareEvent) {
					ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
					ReferenceType refType = classPrepEvent.referenceType();
					addFieldViewer(vm, refType);
				} else if (event instanceof ModificationWatchpointEvent) {
					ModificationWatchpointEvent modWatchEvent = (ModificationWatchpointEvent) event;
					System.out.println("OLD VALUE=" + modWatchEvent.valueCurrent());
					System.out.println("NEW VALUE=" + modWatchEvent.valueToBe());
					System.out.println();
				} 
			}
			eventSet.resume();
		}
	}
	
	public static void watchBreakpoint() 
			throws IOException, 
			InterruptedException, 
			AbsentInformationException, 
			IncompatibleThreadStateException {

		VirtualMachine vm = new VMAttach().connect(8008);

		//addBreakpointMethod(vm, "org.apache.hadoop.mapreduce.Job", "getStartTime");

		List<ReferenceType> referenceTypes = vm.classesByName(CLASS_NAME_BREAKPOINT);
		for (ReferenceType refType : referenceTypes) {
			EventRequestManager evtReqManager = vm.eventRequestManager();
			
			List<Method> methods = refType.methodsByName(METHOD_NAME_BREAKPOINT);
			
			for (Method methodRefType : methods) {
				BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
				breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
				breakpointRequest.setEnabled(true);
			}
			
		}

		addClassViewer(vm, CLASS_NAME_BREAKPOINT);
		
		vm.resume();

		EventQueue eventQueue = vm.eventQueue();
		while (true) {
			EventSet eventSet = eventQueue.remove();
			for (Event event : eventSet) {
				if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
					return;
				} else if (event instanceof ClassPrepareEvent) {
					vm.suspend();
					
					ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
					ReferenceType refType = classPrepEvent.referenceType();

					EventRequestManager evtReqManager = vm.eventRequestManager();
					List<Method> methods = refType.methodsByName(METHOD_NAME_BREAKPOINT);
					
					for (Method methodRefType : methods) {
						BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
						breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
						breakpointRequest.setEnabled(true);
					}
					
					vm.resume();
				} else if (event instanceof BreakpointEvent) {
					vm.suspend();
					
					BreakpointEvent breakpointEvent = (BreakpointEvent) event;
					System.out.println("BREAKPOINT: " + breakpointEvent.toString());
					System.out.println("BP LINE NUMBER: " + breakpointEvent.location().lineNumber());
					System.out.println("BP SOURCE NAME: " + breakpointEvent.location().sourceName());
					System.out.println();
					
					ThreadReference threadRef = breakpointEvent.thread();
					threadRef.suspend();
					StackFrame stackFrame = threadRef.frame(0);
					
					List<LocalVariable> visibleVars = stackFrame.visibleVariables();
					for (LocalVariable visibleVar: visibleVars) {
						System.out.println("VARIABLE NAME: " + visibleVar.name());
					}
					
					threadRef.resume();
					vm.resume();
				}
			}
			eventSet.resume();
		}
	}
	
	public static void watchBreakpointTest()
			throws IOException,
			AbsentInformationException,
			InterruptedException {

		int lineNumber = 329;
		String varName = "status";
		
		VirtualMachine vm = new VMAttach().connect(8008);
		
		
		List<ReferenceType> refTypes = vm.allClasses();
		Location breakpointLocation = null;
		for (ReferenceType refType: refTypes)
		{
			if (breakpointLocation != null)
			{
				break;
			}
			List<Location> locs = refType.allLineLocations();
			for (Location loc: locs)
			{
				if (loc.lineNumber() == lineNumber)
				{
					breakpointLocation = loc;
					break;
				}
			}
		}

		if (breakpointLocation != null)
		{
			EventRequestManager evtReqMgr = vm.eventRequestManager();
			//BreakpointRequest bReq = evtReqMgr.createBreakpointRequest(breakpointLocation);
			//bReq.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
			//bReq.enable();
			
			String className = "org.apache.hadoop.mapreduce.Job";
			String methodName = "setStatus";
			ReferenceType classReference = vm.classesByName(className).get(0);
			Method method = classReference.methodsByName(methodName).get(0);
			
			BreakpointRequest breakpointRequest = evtReqMgr.createBreakpointRequest(method.location());
			breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
			breakpointRequest.setEnabled(true);
			
			
			
			EventQueue evtQueue = vm.eventQueue();
			while(true)
			{
				EventSet evtSet = evtQueue.remove();
				EventIterator evtIter = evtSet.eventIterator();
				while (evtIter.hasNext())
				{
					try
					{
						Event evt = evtIter.next();
						EventRequest evtReq = evt.request();
						if (evtReq instanceof BreakpointRequest)
						{
							BreakpointRequest bpReq = (BreakpointRequest)evtReq;
							if (bpReq.location().lineNumber() == lineNumber)
							{
								System.out.println("Breakpoint at line " + lineNumber + ": ");
								BreakpointEvent brEvt = (BreakpointEvent)evt;
								ThreadReference threadRef = brEvt.thread();
								StackFrame stackFrame = threadRef.frame(0);
								List<LocalVariable> visVars = stackFrame.visibleVariables();
								System.out.println("Got the visible variables without AbsentInformationException.");
								for (LocalVariable visibleVar: visVars)
								{
									if (visibleVar.name().equals(varName))
									{
										Value val = stackFrame.getValue(visibleVar);
										if (val instanceof StringReference)
										{
											String varNameValue = ((StringReference)val).value();
											System.out.println(varName + " = '" + varNameValue + "'");
										}
									}
								}
							}
						}
					}
					catch (AbsentInformationException aie)
					{
						System.out.println("AbsentInformationException: did you compile your target application with -g option?");
					}
					catch (Exception exc)
					{
						System.out.println(exc.getClass().getName() + ": " + exc.getMessage());
					}
					finally
					{
						evtSet.resume();
					}
				}
			}
		}

	}

	private static void addClassViewer(VirtualMachine vm, String className) {
		EventRequestManager evtReqManager = vm.eventRequestManager();
		ClassPrepareRequest classPrepareRequest = evtReqManager.createClassPrepareRequest();
		classPrepareRequest.addClassFilter(className);
		classPrepareRequest.setEnabled(true);
	}

	private static void addFieldViewer(VirtualMachine vm, ReferenceType refType) {
		EventRequestManager evtReqManager = vm.eventRequestManager();
		Field field = refType.fieldByName(FIELD_NAME_WATCH);
		ModificationWatchpointRequest modificationWatchpointRequest = evtReqManager.createModificationWatchpointRequest(field);
		modificationWatchpointRequest.setEnabled(true);
	}
	
	private static void addBreakpointMethod(VirtualMachine vm, String className, String methodName) {
		EventRequestManager evtReqManager = vm.eventRequestManager();
		
		ReferenceType classReference = vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.location());
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
	private static void addBreakpointMethodLine(VirtualMachine vm, String className, String methodName, long methodLine) {
		EventRequestManager evtReqManager = vm.eventRequestManager();
		
		ReferenceType classReference = vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.locationOfCodeIndex(methodLine));
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
}
