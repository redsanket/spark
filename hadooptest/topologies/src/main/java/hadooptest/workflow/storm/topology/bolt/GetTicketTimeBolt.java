package hadooptest.workflow.storm.topology.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.security.auth.kerberos.AutoTGT;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Set;

public class GetTicketTimeBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GetTicketTimeBolt.class);

    public static long getTGTEndTime() {
        long endTime = 0;
        try {
            AccessControlContext context = AccessController.getContext();
            Subject subject = Subject.getSubject(context);
            KerberosTicket tgt = getTGT(subject);
            if (tgt != null) {
                endTime = tgt.getEndTime().getTime();
            }
        } catch (RuntimeException e) {
            LOG.info("Got runtime exception: ", e);
        }
        return endTime;
    }

    private static KerberosTicket getTGT(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                tickets = null;
                return ticket;
            }
        }
        tickets = null;
        return null;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String input = tuple.getString(1);
        collector.emit(new Values(tuple.getValue(0), Long.toString(getTGTEndTime())));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "endTime"));
    }
}
