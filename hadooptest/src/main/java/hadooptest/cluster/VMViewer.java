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
import com.sun.jdi.Method;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StackFrame;
import com.sun.jdi.ThreadReference;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventQueue;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.ModificationWatchpointEvent;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.ClassPrepareRequest;
import com.sun.jdi.request.EventRequestManager;
import com.sun.jdi.request.ModificationWatchpointRequest;

import com.sun.jdi.Value;
import com.sun.jdi.StringReference;
import com.sun.jdi.ObjectReference;

import com.sun.jdi.BooleanValue;
import com.sun.jdi.ByteValue;
import com.sun.jdi.CharValue;
import com.sun.jdi.DoubleValue;
import com.sun.jdi.FloatValue;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LongValue;
import com.sun.jdi.ShortValue;
import com.sun.jdi.VoidValue;

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
						Value val = stackFrame.getValue(visibleVar);
						String value = "";
						if (val instanceof StringReference) {
							value = ((StringReference)val).value();
							System.out.println("VARIABLE VALUE: " + value);
						}
						else if (val instanceof ObjectReference) {
							value = ((ObjectReference)val).toString();
							System.out.println("OBJECT VALUE: " + value);
							List<Field> childFields = ((ObjectReference)val).referenceType().allFields();
							for (Field childField: childFields) {
								System.out.println("CHILD FIELD NAME: " + childField.name() + " - " + childField.toString());
								Value fieldValue = ((ObjectReference)val).getValue(childField);
								//System.out.println("value = " + fieldValue.toString());
								if (fieldValue instanceof BooleanValue) {
									System.out.println("\tVALUE = " + ((BooleanValue)fieldValue).toString());
								}
								else if (fieldValue instanceof ByteValue) {
									System.out.println("\tVALUE = " + ((ByteValue)fieldValue).toString());
								}
								else if (fieldValue instanceof CharValue) {
									System.out.println("\tVALUE = " + ((CharValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof DoubleValue) {
									System.out.println("\tVALUE = " + ((DoubleValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof FloatValue) {
									System.out.println("\tVALUE = " + ((FloatValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof IntegerValue) {
									System.out.println("\tVALUE = " + ((IntegerValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof LongValue) {
									System.out.println("\tVALUE = " + ((LongValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof ShortValue) {
									System.out.println("\tVALUE = " + ((ShortValue)fieldValue).toString());
									
								}
								else if (fieldValue instanceof VoidValue) {
									System.out.println("\tVALUE = " + ((VoidValue)fieldValue).toString());
								}
							}
						}
					}
					
					threadRef.resume();
					vm.resume();
				}
			}
			eventSet.resume();
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
