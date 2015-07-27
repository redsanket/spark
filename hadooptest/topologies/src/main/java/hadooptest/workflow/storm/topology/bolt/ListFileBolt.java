package hadooptest.workflow.storm.topology.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.generated.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListFileBolt extends BaseBasicBolt {

    private final static Logger LOG = LoggerFactory.getLogger(ListFileBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        LOG.info("Called ListFileBolt prepare");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String distFile = tuple.getString(0);
        String output = null;
        String returnInfo = tuple.getStringByField("return-info");
        LOG.info("Received file name " + distFile);
	try {
        output = getBlobContent(distFile);
	} catch (IOException io) {
        output = "Got IO Exception";
        LOG.error("Got IO exception");
	}
        collector.emit(new Values(output, returnInfo));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }

    private String getBlobContent(String filename) throws IOException {
        // Let's get the owner, and then the created file permissions
        Path path = Paths.get(filename);
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
        FileOwnerAttributeView view = Files.getFileAttributeView(path, FileOwnerAttributeView.class);
        UserPrincipal userPrincipal = view.getOwner();
        String returnValue = userPrincipal.getName() + ":" + PosixFilePermissions.toString(perms);

        return returnValue;
    }
}
