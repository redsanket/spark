/*
 * YAHOO!
 * 
 * A class which allows the framework to connect to a debuggable instance of
 * a Hadoop cluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.VirtualMachineManager;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.IllegalConnectorArgumentsException;

import java.io.IOException;
import java.util.Map;

public class VMAttach {
	
	private int debug_port;
	
	public VMAttach(int port) {
		debug_port = port;
	}
	
	public VirtualMachine connect() throws IOException {
		String port_str = Integer.toString(this.debug_port);
		AttachingConnector connector = getAttachingConnector();
		
		try {
			VirtualMachine vm = connect(connector, port_str);
			return vm;
		} catch (IllegalConnectorArgumentsException e) {
			throw new IllegalStateException(e);
		}
	}

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

	private VirtualMachine connect(AttachingConnector connector, String port) 
			throws IllegalConnectorArgumentsException, IOException {
		
		Map<String, Connector.Argument> arguments = connector.defaultArguments();
		Connector.Argument portArgument = arguments.get("port");
		
		if (portArgument == null) {
			throw new IllegalStateException();
		}
		
		portArgument.setValue(port);

		return connector.attach(arguments);
	}

	
}
