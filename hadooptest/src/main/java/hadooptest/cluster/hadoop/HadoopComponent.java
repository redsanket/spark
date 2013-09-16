/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster.State;
import hadooptest.node.hadoop.HadoopNode;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Class representing the base capability of a cluster component in the 
 * framework.
 */
public class HadoopComponent {

    /**
     * Container for storing the component nodes
     */
    protected Hashtable<String, HadoopNode> componentNodes =
            new Hashtable<String, HadoopNode>();
    protected State componentState = State.UNKNOWN;    
    protected String componentName;

    public HadoopComponent(String componentName,
            Hashtable<String, HadoopNode> nodes) {
        this.componentName= componentName;
        this.componentNodes = nodes;
    }
    
    public Hashtable<String, HadoopNode> getNodes() {
        return this.componentNodes;
    }

    public void setState(State state) {
        this.componentState = state;
    }

    public State getState() {
        return this.componentState;
    }

    public void printNodes() {
        TestSession.logger.debug("-- listing nodes for component " + 
                this.componentName + "--");        
        HadoopNode node;
        Enumeration<String> hostnameKeys = componentNodes.keys(); 
        while (hostnameKeys.hasMoreElements()) { 
            String hostname = (String) hostnameKeys.nextElement();            
            node = componentNodes.get(hostname);
            TestSession.logger.info(node.getHostname());
        }   
    }

    public String[] getNode(State state) {        
        ArrayList<String> nodes = new ArrayList<String>();        
        HadoopNode node;
        Enumeration<String> hostnameKeys = componentNodes.keys(); 
        while (hostnameKeys.hasMoreElements()) { 
            String hostname = (String) hostnameKeys.nextElement();            
            node = componentNodes.get(hostname);
            if (node.getState() == state) {
                nodes.add(hostname);
            }
        }   
        return nodes.toArray(new String[nodes.size()]);
    }

    public HadoopNode getNode(String hostname) {
        return this.componentNodes.get(hostname);
    }
}
