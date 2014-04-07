package hadooptest.cluster.hadoop.dfs;

import hadooptest.cluster.hadoop.dfs.PrivilegedExceptionActionImpl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class DoAs {
	UserGroupInformation ugi;
	String action;
	Configuration configuration;
	String oneFile;
	String otherFile;

	public DoAs(UserGroupInformation ugi, String action,
			Configuration configuration, String firstFile, String secondFile)
					throws IOException {
		this.ugi = ugi;
		this.action = action;
		this.configuration = configuration;
		this.oneFile = firstFile;
		this.otherFile = secondFile;
	}

	public String doAction() throws AccessControlException, IOException,
	InterruptedException {
		PrivilegedExceptionActionImpl privilegedExceptionActor = new PrivilegedExceptionActionImpl(
				ugi, action, configuration, oneFile, otherFile);
		ugi.doAs(privilegedExceptionActor);
		return privilegedExceptionActor.getReturnString();
	}
}