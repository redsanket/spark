package hadooptest.cluster;

public class MultiClusterProtocol {
	 
    public String processInput(String theInput) {
        String theOutput = null;
        
        if (theInput != null) {
        	if (theInput.equals("Hi, this is a multi cluster client.")) {
        		theOutput = "Hello there.  I am the multi cluster server.";
        	}
        }
        
        return theOutput;
    }

}
