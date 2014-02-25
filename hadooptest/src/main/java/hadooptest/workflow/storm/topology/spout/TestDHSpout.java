package hadooptest.workflow.storm.topology.spout;
import com.yahoo.spout.http.rainbow.RainbowSpout;

import java.net.URI;

public class TestDHSpout extends RainbowSpout {

	public TestDHSpout(URI serviceURI) {
		super(serviceURI, false, null);
	}
}
