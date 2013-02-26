/*
 * YAHOO!
 * 
 * Rick Bernotas (rbernota)
 */

package hadooptest.cluster;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.VirtualMachineManager;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.IllegalConnectorArgumentsException;
import com.sun.jdi.connect.Connector.Argument;
import com.sun.jdi.connect.Connector.IntegerArgument;

import java.io.IOException;
import java.util.Map;
import java.util.Vector;

/**
 * A class which allows the framework to connect to a debuggable instance of
 * a Hadoop cluster.
 * 
 * This is currently an experimental class.
 */
public class VMAttach {
	
	// The debug port we are going to attach to.
	private int debug_port;
	
	public Vector<VirtualMachine> listenerVMs;
	
	/**
	 * Class constructor.
	 * 
	 * Sets the debug port to attach to.
	 * 
	 * @param port the debug port to attach to.
	 */
	public VMAttach(int port) {
		debug_port = port;
		
		listenerVMs = new Vector<VirtualMachine>(1);
	}
	
	/**
	 * Connects to a JVM debug port and returns a VM.
	 * 
	 * @return VirtualMachine the VM hosting the debug port.
	 */
	public VirtualMachine connect() throws IOException {
		String port_str = Integer.toString(this.debug_port);
		AttachingConnector connector = getAttachingConnector();
		
		try {
			VirtualMachine vm = connectToVM(connector, port_str);
			return vm;
		} catch (IllegalConnectorArgumentsException e) {
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Listens to a host JVM debug port for a client to attach
	 */
	public void listen() throws IOException {
		ListeningConnector connector = getListeningConnector();
		
		Map<String, Argument> map = connector.defaultArguments();
		
		IntegerArgument listenPort = (IntegerArgument) map.get("port");
		listenPort.setValue(this.debug_port);
		
		IntegerArgument listenTimeout = (IntegerArgument) map.get("timeout");
		listenTimeout.setValue(0);
		
		/*
		try {
			connector.startListening(map);
			return connector.accept(map); 
		} catch (IllegalConnectorArgumentsException e) {
			throw new IllegalStateException(e);
		}
		*/

		//VirtualMachine vmList[] = new VirtualMachine[2];
		
		/*
		try {
			for (int i = 0; i < 2; i++) {
				vmList[i] = connector.accept(map);
			}
		}
		catch (IllegalConnectorArgumentsException e) {
			throw new IllegalStateException(e);
		}
		*/
		
		VMListenerThread listener = new VMListenerThread(map, connector);
		listener.start();
		
		//return vmList;
	}

	/**
	 * Gets the attaching connector for the debug session, which is
	 * the JDI SocketAttach.
	 * 
	 * @return AttachingConnector the AttachingConnector for the debug session.
	 */
	private AttachingConnector getAttachingConnector() {
		VirtualMachineManager VMManager = Bootstrap.virtualMachineManager();
		
		for (Connector connector : VMManager.attachingConnectors()) {
			System.out.println("Attaching Connector is: " + connector.name());
			
			if ("com.sun.jdi.SocketAttach".equals(connector.name())) {
				return (AttachingConnector) connector;
			}
		}
		
		throw new IllegalStateException();
	}


	/**
	 * Gets the listening connector for the debug session, which is
	 * the JDI SocketListen.
	 * 
	 * @return ListeningConnector the AttachingConnector for the debug session.
	 */
	private ListeningConnector getListeningConnector() {
		VirtualMachineManager VMManager = Bootstrap.virtualMachineManager();
		
		for (Connector connector : VMManager.listeningConnectors()) {
			System.out.println("Listening Connector is: " + connector.name());
			
			if ("com.sun.jdi.SocketListen".equals(connector.name())) {
				return (ListeningConnector) connector;
			}
		}
		
		throw new IllegalStateException();
	}
	
	/**
	 * Connects the debug session with the given attachingconnector and debug port.
	 * 
	 * @param connector the AttachingConnector for the debug session.
	 * @param port the port to attach to.
	 * 
	 * @return VirtualMachine the VM we've connected to.
	 */
	private VirtualMachine connectToVM(AttachingConnector connector, String port) 
			throws IllegalConnectorArgumentsException, IOException {
		
		Map<String, Argument> arguments = connector.defaultArguments();
		Argument portArgument = arguments.get("port");
		
		if (portArgument == null) {
			throw new IllegalStateException();
		}
		
		portArgument.setValue(port);

		return connector.attach(arguments);
	}

	/**
	 * A listener thread for picking up new VMs that try to attach to the debugger.
	 */
	private class VMListenerThread extends Thread {

		/** The Map of listeners attached to the debugger */
		private Map<String, Argument> listenerMap;
		
		/** The connector for a new listener thread */
		ListeningConnector VMConnector;
		
		/** 
		 * Initializes the listener Map and attaching connector.
		 */
		public VMListenerThread(Map<String, Argument> map, ListeningConnector connector) {
			listenerMap = map;
			VMConnector = connector;
		}
		
		/**
		 * The thread runner.  Accepts new VMs and attaches them to the debugger.
		 */
		@Override
		public void run() {
			synchronized(this) {
				try {
					while (true) {
						listenerVMs.add(VMConnector.accept(listenerMap));
						System.out.println("*** NEW VM ATTACHED ***");
						System.out.println("--- VM Vector size: " + listenerVMs.size());
					}
				}
				catch (IllegalConnectorArgumentsException e) {
					throw new IllegalStateException(e);
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}

}

