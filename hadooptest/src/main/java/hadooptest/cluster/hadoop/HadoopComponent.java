/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
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
    protected int numExpDaemonPerHost;
    protected int numActualTotalDaemon;    

    public HadoopComponent(String componentName,
            Hashtable<String, HadoopNode> nodes) {
        this.componentName= componentName;
        this.componentNodes = nodes;
        numExpDaemonPerHost = (componentName.equals(HadoopCluster.DATANODE)) ?
                2 : 1;
    }
    
    public Hashtable<String, HadoopNode> getNodes() {
        return this.componentNodes;
    }

    public int getNumHosts() {
        return this.componentNodes.size();
    }
    
    public int getNumExpDaemonPerHost() {
        return this.numExpDaemonPerHost;
    }

    public int getNumExpTotalDaemon() {
        return this.componentNodes.size() * this.numExpDaemonPerHost;
    }

    public void setNumActualTotalDaemon(int numDaemon) {
        this.numActualTotalDaemon = numDaemon;
    }

    public int getNumActualTotalDaemonUp() {
        return this.numActualTotalDaemon;
    }

    public int getNumActualTotalDaemonDown() {
        return (this.getNumExpTotalDaemon()-this.numActualTotalDaemon);
    }
    
    public String getNumActualTotalDaemonUpRatio() {
        return this.numActualTotalDaemon + "/" + this.getNumExpTotalDaemon();
    }

    public String getNumActualTotalDaemonDownRatio() {
        return (this.getNumExpTotalDaemon()-this.numActualTotalDaemon) + "/" +
                this.getNumExpTotalDaemon();
    }

    public int getNumExpDaemonPerHost(Action action) {
        return (action.equals(Action.START)) ?
                this.numExpDaemonPerHost : 0;
    }
    
    public int getNumExpTotalDaemon(Action action) {
        return (action.equals(Action.START)) ?
                getNumExpTotalDaemon() : 0;
    }
    
    public void setState(State state) {
        this.componentState = state;
    }

    public State getState() {
        return this.componentState;
    }

    /**
     * Get the daemon process status for the component. 
     * I.e. how many processes are up vs. down.
     * 
     * If action is 'start', then it returns the total number of daemons up. 
     * (total number of actual daemons up) / (total number of daemons)
     * 
     * If action is 'stop', then it returns the total number of daemons down.
     * (total number of daemons - total number of actual daemons up) / 
     * (total number of daemons)
     */
    public String getStatus(Action action) {
        return (action.equals(Action.START)) ? 
                this.getNumActualTotalDaemonUpRatio() :
                this.getNumActualTotalDaemonDownRatio();                
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
