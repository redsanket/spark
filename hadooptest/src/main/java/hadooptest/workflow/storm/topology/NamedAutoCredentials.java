package hadooptest.workflow.storm.topology;

import backtype.storm.security.auth.IAutoCredentials;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatically take a users TGT, and push it, and renew it in Nimbus.
 */
public class NamedAutoCredentials implements IAutoCredentials {
    private static final Logger LOG = LoggerFactory.getLogger(NamedAutoCredentials.class);
    private String _name;

    public static class Named implements Principal {
        private String _name;

        public Named(String name) {
            if (name == null) {
                throw new NullPointerException("NAME cannot be null");
            }
            _name = name;
        }

        public boolean equals(Object other) {
            if (!(other instanceof Named)) return false;
            if (this == other) return true;
            Named oth = (Named)other;
            return _name.equals(oth._name);
        }

        public String getName() {
            return _name;
        }

        public int hashCode() {
            return _name.hashCode();
        }

	public String toString() {
           return "[NAMED: "+_name+"]";
        }
    }

    @Override
    public void prepare(Map conf) {
        _name = (String)conf.get("name.to.use");
        if (_name == null) _name = "DEFAULT";
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        LOG.warn("Putting "+_name+" in credentials");
        credentials.put("NAMED",_name);
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        Named n = new Named(credentials.get("NAMED"));
        Set<Principal> p = subject.getPrincipals();
        if (!p.contains(n)) {
            Set<Named> old = subject.getPrincipals(Named.class);
            p.add(n);
            p.removeAll(old);
        }
        LOG.warn("Update subject to "+subject);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        Named n = new Named(credentials.get("NAMED"));
        subject.getPrincipals().add(n);
        LOG.warn("Populated subject "+subject);
    }
}
