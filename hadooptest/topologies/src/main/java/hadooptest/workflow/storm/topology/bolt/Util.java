package hadooptest.workflow.storm.topology.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.hbase.security.User;

import java.io.*;
import org.apache.log4j.Logger;

class Util {
    private static Logger LOG = Logger.getLogger(Util.class);

    /**
     * Serialize Configuration object into a byte array
     */
    static byte[] ToBytes(Configuration conf) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outputStream);
        conf.write(out);
        return outputStream.toByteArray();
    }

    /**
     * Deserialize Configuration object from a byte array
     */
    static Configuration FromBytes(byte[] buf) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
        DataInputStream in = new DataInputStream(inputStream);
        Configuration conf = new Configuration(false);
        conf.readFields(in);
        return conf;
    }
}
