/*
 * YAHOO!
 * 
 *  Rick Bernotas (rbernota)
 */

package hadooptest.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

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

/**
 * A class which allows for programmatic debugging of a debuggable instance
 * of a Hadoop cluster.  Currently can set a watch variable and method
 * breakpoints.
 * 
 * This is currently an experimental class.
 */
public class VMViewer {
	
	/** Hadoop debug port to attach to. */
	public static final int DEFAULT_DEBUG_ATTACH_PORT = 8008;
	
	/** Hadoop default debug port to listen on. */
	public static final int DEFAULT_DEBUG_LISTEN_PORT = 8002;
	
	/** Depth of recursion to prevent chasing circular references in the stack. */
	public static final int RECURSION_DEPTH_DEFAULT = 2;

	/** The attaching VM for the debug session. */
	private VirtualMachine vm;
	
	/** The name of the class to debug */
	private String listenerClassName;
	
	/** The name of the variable to debug */
	private String listenerVariable;
	
	/** The name of the method to debug */
	private String listenerMethod;
	
	/** The Vector of VMs we are listening to */
	public Vector<VirtualMachine> listenerVMs;
	
	/**
	 * Class constructor.
	 * 
	 * Attaches to the default debug attach port of the suspended Hadoop VM.
	 */
	public VMViewer() throws IOException {
		this.vm = new VMAttach(DEFAULT_DEBUG_ATTACH_PORT).connect();
	}
	
	/**
	 * Class constructor
	 * 
	 * Attaches to a specified debug port of the suspended Hadoop VM.
	 * 
	 * @param isListener whether the debugger is listening for VMs or not.
	 * @param port the debug port to attach to.
	 */
	public VMViewer(boolean isListener, int port) throws IOException {
		this.vm = new VMAttach(port).connect();
	}

	/**
	 * Class constructor.
	 * 
	 * Listens for a VM to process a specific method or watch variable in a class.
	 * 
	 * @param actionType specifies whether to "WATCH" (watch variable) or "BREAK" (breakpoint)
	 * @param className the class containing the specified debug target
	 * @param debugTarget the method name or watch variable to debug
	 * @throws IOException
	 */
	public VMViewer(String actionType, String className, String debugTarget) throws IOException {
		this.listenerClassName = className;
		
		if (actionType == "WATCH") {
			this.listenerVariable = debugTarget;
		}
		else if (actionType == "BREAK") {
			this.listenerMethod = debugTarget;
		}
		
		VMAttach vmAttacher = new VMAttach(DEFAULT_DEBUG_LISTEN_PORT);
		VMListenerVectorWatcher watcher = new VMListenerVectorWatcher(vmAttacher, this);
		
		vmAttacher.listen();
		watcher.start();
		
		while(true) {

		}
	}
	
	/**
	 * Class constructor.
	 * 
	 * Attaches to a VM to process a specific method or watch variable in a class.
	 * 
	 * @param actionType specifies whether to "WATCH" (watch variable) or "BREAK" (breakpoint)
	 * @param port the port of the VM to connect to.
	 * @param className the class containing the specified debug target
	 * @param debugTarget the method name or watch variable to debug
	 * @throws IOException
	 */
	public VMViewer(String actionType, int port, String className, String debugTarget) throws IOException {
		this.listenerClassName = className;

		if (actionType == "WATCH") {
			this.listenerVariable = debugTarget;
		}
		else if (actionType == "BREAK") {
			this.listenerMethod = debugTarget;
		}
		
		VMAttach vmAttacher = new VMAttach(port);
		VMListenerVectorWatcher watcher = new VMListenerVectorWatcher(vmAttacher, this);
		
		vmAttacher.listen();
		watcher.start();
	}
	
	/**
	 * Sets a watch point on a variable name in a given class name, 
	 * and dumps out the stack contents of the variable to stdout each
	 * time it encounters the watch point.
	 * 
	 * @param className the name of the class the watch variable resides in
	 * 						(must include the package name).
	 * @param variable the variable to set the watch point on.
	 */
	public void watchVariable(String className, String variable) throws IOException, InterruptedException {

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
	
	/**
	 * Sets a breakpoint on a method name in a given class name, 
	 * and dumps out the stack contents each time it encounters the breakpoint.
	 * 
	 * @param className the name of the class the watch variable resides in
	 * 						(must include the package name).
	 * @param variable the method to set the breakpoint on.
	 */
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
	
	/**
	 * A recursive method that takes a stack value and prints some information
	 * about it to stdout.
	 * 
	 * @param val the stack value to explore.
	 * @param tabIndent the accumulated indentation for the std output
	 * @param recursionDepth the remaining depth of the stack to recurse.
	 */
	private void objectRefRecurse(Value val, String tabIndent, int recursionDepth) {
		String value = "";
		
		this.printValue(val, tabIndent);

		if (val instanceof StringReference) {
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

				this.printValue(fieldValue, tabIndent);

				if (fieldValue instanceof ObjectReference) {
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
	
	/**
	 * Prints the value of a stack type.
	 * 
	 * @param val the stack value to process
	 * @param tabIndent the string indentation to assign to the string prefix for
	 * 						printing out the stack value.
	 */
	private void printValue(Value val, String tabIndent) {
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
	}
	
	/**
	 * Adds a class viewer to the debug session.
	 * 
	 * @param the name of the class to view.
	 */
	private void addClassViewer(String className) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		ClassPrepareRequest classPrepareRequest = evtReqManager.createClassPrepareRequest();
		classPrepareRequest.addClassFilter(className);
		classPrepareRequest.setEnabled(true);
	}
	
	/**
	 * Adds a variable field viewer to the debug session
	 * 
	 * @param refType the reference type of the watch variable
	 * @param variable the field variable to set a watch point for.
	 */
	private void addFieldViewer(ReferenceType refType, String variable) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		Field field = refType.fieldByName(variable);
		ModificationWatchpointRequest modificationWatchpointRequest = evtReqManager.createModificationWatchpointRequest(field);
		modificationWatchpointRequest.setEnabled(true);
	}
	
	/**
	 * Adds a method breakpoint to the debug session
	 * 
	 * @param className the name of the class in which to set the breakpoint
	 * @param methodName the name of the method for which to set the breakpoint
	 */
	private void addBreakpointMethod(String className, String methodName) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		
		ReferenceType classReference = this.vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.location());
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
	/**
	 * Adds a method breakpoint at a given line in the method for the debug session
	 * 
	 * @param className the name of the class in which to set the breakpoint
	 * @param methodName the name of the method for which to set the breakpoint
	 * @param methodLine the line in the method for which to set the breakpoint
	 */
	private void addBreakpointMethodLine(String className, String methodName, long methodLine) {
		EventRequestManager evtReqManager = this.vm.eventRequestManager();
		
		ReferenceType classReference = this.vm.classesByName(className).get(0);
		Method method = classReference.methodsByName(methodName).get(0);
		
		BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(method.locationOfCodeIndex(methodLine));
		breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
		breakpointRequest.setEnabled(true);
	}
	
	/**
	 * A thread for processing a VM as it attaches itself to the debugger.
	 */
	private class VMListenerVectorWatcher extends Thread {
		
		private VMAttach attacher;
		
		private String className;
		private String variable;
		private String method;
		
		public VMListenerVectorWatcher(VMAttach VMAttacher, VMViewer viewer){
			
			attacher = VMAttacher;
			
			this.className = viewer.listenerClassName;
			this.variable = viewer.listenerVariable;
			this.method = viewer.listenerMethod;
		}
		
		@Override
		public void run() {
			while(true) {
				//System.out.println("PROCESS VM VECTOR SIZE: " + attacher.listenerVMs.size());
				if (attacher.listenerVMs.size() > 0) {
					try {
						processVM();
					} catch (IOException ioe) {

					} catch (InterruptedException ie) {

					}

					attacher.listenerVMs.remove(0);
				}
			}
		}
		
		private void processVM() throws IOException, InterruptedException {

			System.out.println("*** PROCESSING OLDEST UN-PROCESSED VM ***");

			try {
				if (variable == null) {
					this.processBreakpointMethod();
				}
				else if (method == null) {
					this.processWatchVariable();
				} 
			} catch (Exception e) {}
		}
		
		private void processWatchVariable() throws InterruptedException {

			VirtualMachine vmProcess = attacher.listenerVMs.get(0);
			
			List<ReferenceType> referenceTypes = vmProcess.classesByName(this.className);
			for (ReferenceType refType : referenceTypes) {
				addFieldViewerMultiVM(refType, this.variable, vmProcess);
			}

			this.addClassViewerMultiVM(this.className, vmProcess);

			vmProcess.resume();

			EventQueue eventQueue = vmProcess.eventQueue();
			while (true) {
				EventSet eventSet = eventQueue.remove();
				for (Event event : eventSet) {
					if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
						return;
					} else if (event instanceof ClassPrepareEvent) {
						ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
						ReferenceType refType = classPrepEvent.referenceType();
						addFieldViewerMultiVM(refType, this.variable, vmProcess);
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
		
		private void processBreakpointMethod() 
				throws IOException, 
				InterruptedException, 
				AbsentInformationException, 
				IncompatibleThreadStateException {

			VirtualMachine vmProcess = attacher.listenerVMs.get(0);
			
			List<ReferenceType> referenceTypes = vmProcess.classesByName(this.className);
			for (ReferenceType refType : referenceTypes) {
				EventRequestManager evtReqManager = vmProcess.eventRequestManager();
				
				List<Method> methods = refType.methodsByName(this.method);
				
				for (Method methodRefType : methods) {
					BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
					breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
					breakpointRequest.setEnabled(true);
				}
				
			}

			this.addClassViewerMultiVM(this.className, vmProcess);
			
			vmProcess.resume();

			EventQueue eventQueue = vmProcess.eventQueue();
			while (true) {
				EventSet eventSet = eventQueue.remove();
				for (Event event : eventSet) {
					if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
						return;
					} else if (event instanceof ClassPrepareEvent) {
						vmProcess.suspend();
						
						ClassPrepareEvent classPrepEvent = (ClassPrepareEvent) event;
						ReferenceType refType = classPrepEvent.referenceType();

						EventRequestManager evtReqManager = vmProcess.eventRequestManager();
						List<Method> methods = refType.methodsByName(this.method);
						
						for (Method methodRefType : methods) {
							BreakpointRequest breakpointRequest = evtReqManager.createBreakpointRequest(methodRefType.location());
							breakpointRequest.setSuspendPolicy(BreakpointRequest.SUSPEND_ALL);
							breakpointRequest.setEnabled(true);
						}
						
						vmProcess.resume();
					} else if (event instanceof BreakpointEvent) {
						vmProcess.suspend();
						
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
						vmProcess.resume();
					}
				}
				eventSet.resume();
			}
		}
		
		private void addFieldViewerMultiVM(ReferenceType refType, String variable, VirtualMachine vmInstance) {
			EventRequestManager evtReqManager = vmInstance.eventRequestManager();
			Field field = refType.fieldByName(variable);
			ModificationWatchpointRequest modificationWatchpointRequest = evtReqManager.createModificationWatchpointRequest(field);
			modificationWatchpointRequest.setEnabled(true);
		}

		private void addClassViewerMultiVM(String className, VirtualMachine vmInstance) {
			EventRequestManager evtReqManager = vmInstance.eventRequestManager();
			ClassPrepareRequest classPrepareRequest = evtReqManager.createClassPrepareRequest();
			classPrepareRequest.addClassFilter(className);
			classPrepareRequest.setEnabled(true);
		}
		
		/**
		 * A recursive method that takes a stack value and prints some information
		 * about it to stdout.
		 * 
		 * @param val the stack value to explore.
		 * @param tabIndent the accumulated indentation for the std output
		 * @param recursionDepth the remaining depth of the stack to recurse.
		 */
		private void objectRefRecurse(Value val, String tabIndent, int recursionDepth) {
			String value = "";
			
			this.printValue(val, tabIndent);

			if (val instanceof StringReference) {
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

					this.printValue(fieldValue, tabIndent);

					if (fieldValue instanceof ObjectReference) {
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
		
		private void printValue(Value val, String tabIndent) {
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
		}
		
	}
	
}
