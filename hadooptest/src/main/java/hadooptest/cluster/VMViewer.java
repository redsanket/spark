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
import com.sun.jdi.ArrayReference;
import com.sun.jdi.BooleanValue;
import com.sun.jdi.ByteValue;
import com.sun.jdi.CharValue;
import com.sun.jdi.DoubleValue;
import com.sun.jdi.Field;
import com.sun.jdi.FloatValue;
import com.sun.jdi.IncompatibleThreadStateException;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LocalVariable;
import com.sun.jdi.LongValue;
import com.sun.jdi.Method;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.ShortValue;
import com.sun.jdi.StackFrame;
import com.sun.jdi.StringReference;
import com.sun.jdi.ThreadReference;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.VoidValue;
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

public class VMViewer {
	
	public static final int DEFAULT_DEBUG_ATTACH_PORT = 8008;
	public static final int RECURSION_DEPTH_DEFAULT = 2;

	private VirtualMachine vm;
	
	public VMViewer() throws IOException {
		this.vm = new VMAttach(DEFAULT_DEBUG_ATTACH_PORT).connect();
	}
	
	public VMViewer(int port) throws IOException {
		this.vm = new VMAttach(port).connect();
	}
	
	public void watchVariable(String className, String variable) 
			throws IOException, 
			InterruptedException 
			{

		List<ReferenceType> referenceTypes = this.vm.classesByName(className);
		for (ReferenceType refType : referenceTypes) {
			addFieldViewer(refType, variable);
		}
		
		this.addClassViewer(className);

		this.vm.resume();

		EventQueue eventQueue = this.vm.eventQueue();
		while (true) {
			EventSet eventSet = eventQueue.remove();
			for (Event event : eventSet) {
				if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
					return;
				} else if (event instanceof ClassPrepareEvent) {
					ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
					ReferenceType refType = classPrepEvent.referenceType();
					addFieldViewer(refType, variable);
				} else if (event instanceof ModificationWatchpointEvent) {
					ModificationWatchpointEvent modWatchEvent = (ModificationWatchpointEvent) event;
					System.out.println("OLD VALUE=" + modWatchEvent.valueCurrent());
					System.out.println("NEW VALUE=" + modWatchEvent.valueToBe());
					System.out.println();
					
					this.objectRefRecurse(modWatchEvent.valueToBe(), "", RECURSION_DEPTH_DEFAULT);
				} 
			}
			eventSet.resume();
		}
	}
	
	public void breakpointMethod(String className, String methodName) 
			throws IOException, 
			InterruptedException, 
			AbsentInformationException, 
			IncompatibleThreadStateException {

		List<ReferenceType> referenceTypes = this.vm.classesByName(className);
		for (ReferenceType refType : referenceTypes) {
			EventRequestManager evtReqManager = this.vm.eventRequestManager();
			
			List<Method> methods = refType.methodsByName(methodName);
			
			for (Method methodRefType : methods) {
				BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
				breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
				breakpointRequest.setEnabled(true);
			}
			
		}

		this.addClassViewer(className);
		
		this.vm.resume();

		EventQueue eventQueue = this.vm.eventQueue();
		while (true) {
			EventSet eventSet = eventQueue.remove();
			for (Event event : eventSet) {
				if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
					return;
				} else if (event instanceof ClassPrepareEvent) {
					this.vm.suspend();
					
					ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
					ReferenceType refType = classPrepEvent.referenceType();

					EventRequestManager evtReqManager = this.vm.eventRequestManager();
					List<Method> methods = refType.methodsByName(methodName);
					
					for (Method methodRefType : methods) {
						BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
						breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
						breakpointRequest.setEnabled(true);
					}
					
					this.vm.resume();
				} else if (event instanceof BreakpointEvent) {
					this.vm.suspend();
					
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
						
						this.objectRefRecurse(val, "", RECURSION_DEPTH_DEFAULT);
					}
					
					threadRef.resume();
					this.vm.resume();
				}
			}
			eventSet.resume();
		}
	}
	
	private void objectRefRecurse(Value val, String tabIndent, int recursionDepth) {
		String value = "";
		

		if (val instanceof BooleanValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((BooleanValue)val).toString());
		}
		else if (val instanceof ByteValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((ByteValue)val).toString());
		}
		else if (val instanceof CharValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((CharValue)val).toString());
		}
		else if (val instanceof DoubleValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((DoubleValue)val).toString());
		}
		else if (val instanceof FloatValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((FloatValue)val).toString());
		}
		else if (val instanceof IntegerValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((IntegerValue)val).toString());
		}
		else if (val instanceof LongValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((LongValue)val).toString());
		}
		else if (val instanceof ShortValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((ShortValue)val).toString());
		}
		else if (val instanceof VoidValue) {
			System.out.println(tabIndent + "\tVALUE = " + ((VoidValue)val).toString());
		}
		else if (val instanceof StringReference) {
			value = ((StringReference)val).value();
			System.out.println(tabIndent + "StringReference VALUE: " + value);
		}
		else if (val instanceof ArrayReference) {
			value = ((ArrayReference)val).toString();
			System.out.println(tabIndent + "ArrayReference VALUE: " + value);
		}
		else if (val instanceof ObjectReference) {
			value = ((ObjectReference)val).toString();
			System.out.println(tabIndent + "OBJECT VALUE: " + value);
			List<Field> childFields = ((ObjectReference)val).referenceType().allFields();
			for (Field childField: childFields) {
				System.out.println(tabIndent + "CHILD FIELD NAME: " + childField.name() + " - " + childField.toString());
				Value fieldValue = ((ObjectReference)val).getValue(childField);

				if (fieldValue instanceof BooleanValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((BooleanValue)fieldValue).toString());
				}
				else if (fieldValue instanceof ByteValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((ByteValue)fieldValue).toString());
				}
				else if (fieldValue instanceof CharValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((CharValue)fieldValue).toString());
				}
				else if (fieldValue instanceof DoubleValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((DoubleValue)fieldValue).toString());
				}
				else if (fieldValue instanceof FloatValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((FloatValue)fieldValue).toString());
				}
				else if (fieldValue instanceof IntegerValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((IntegerValue)fieldValue).toString());
				}
				else if (fieldValue instanceof LongValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((LongValue)fieldValue).toString());
				}
				else if (fieldValue instanceof ShortValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((ShortValue)fieldValue).toString());
				}
				else if (fieldValue instanceof VoidValue) {
					System.out.println(tabIndent + "\tVALUE = " + ((VoidValue)fieldValue).toString());
				}
				else if (fieldValue instanceof ObjectReference) {
					if (recursionDepth > 0) {
						tabIndent = tabIndent + "\t";
						recursionDepth = recursionDepth - 1;
						objectRefRecurse(fieldValue, tabIndent, recursionDepth);
					}
					else {
						value = ((ObjectReference)val).toString();
						System.out.println(tabIndent + "OBJECT VALUE: " + value);
					}
				}
			}
		}
	}
	
	private void addClassViewer(String className) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		ClassPrepareRequest classPrepareRequest = evtReqManager.createClassPrepareRequest();
		classPrepareRequest.addClassFilter(className);
		classPrepareRequest.setEnabled(true);
	}

	private void addFieldViewer(ReferenceType refType, String variable) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		Field field = refType.fieldByName(variable);
		ModificationWatchpointRequest modificationWatchpointRequest = evtReqManager.createModificationWatchpointRequest(field);
		modificationWatchpointRequest.setEnabled(true);
	}
	
	private void addBreakpointMethod(String className, String methodName) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		
		ReferenceType classReference = this.vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.location());
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
	private void addBreakpointMethodLine(String className, String methodName, long methodLine) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		
		ReferenceType classReference = this.vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.locationOfCodeIndex(methodLine));
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
}
