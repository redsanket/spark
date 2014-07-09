package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.automation.utils.http.JSONUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;

import java.util.Map;

@Category(SerialTests.class)
public class TestStormCli extends TestSessionStorm {

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    public TopologySummary getTS(String name) throws Exception {
        for (TopologySummary ts: cluster.getClusterInfo().get_topologies()) {
            if (name.equals(ts.get_name())) {
                return ts;
            }
        }
        throw new IllegalArgumentException("Topology "+name+" does not appear to be up yet");
    }

    public String getId(String name) throws Exception {
        TopologySummary ts = getTS( name );

        return ts.get_id();
    }
 
    public int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
    }

    public void LaunchExclamationTopology() throws Exception {
        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", "/home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar", "storm.starter.ExclamationTopology",  "exclaim", "-c", "ui.users=[\"user1\", \"user2\", \"user3\"]" }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );
    }

    @Test(timeout=600000)
    public void JSONTest() throws Exception {
        JSONUtil json = new JSONUtil();
        LaunchExclamationTopology();
        logger.info("Sleeping 30 seconds to let topology submission happen.");  
        Util.sleep(30);
        logger.info("Now let's get cluster conf.");  
        String topConfig = cluster.getTopologyConf(getId("exclaim"));
        logger.info("Returned cluster info is " + topConfig);  
        json.setContent(topConfig);
        String users = json.getElement("ui.users").toString();
        logger.info("Returned ui users is " + users);  
        assertTrue("value was not [user1, user2, user3]", users.equals("[user1, user2, user3]"));
        cluster.killTopology("exclaim");
    }
}
