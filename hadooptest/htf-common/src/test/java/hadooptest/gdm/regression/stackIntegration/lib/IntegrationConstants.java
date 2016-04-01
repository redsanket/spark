package hadooptest.gdm.regression.stackIntegration.lib;

public interface IntegrationConstants {
	String HADOOP_HOME="export HADOOP_HOME=/home/gs/hadoop/current";
	String JAVA_HOME="export JAVA_HOME=/home/gs/java/jdk64/current/";
	String HADOOP_CONF_DIR="export HADOOP_CONF_DIR=/home/gs/conf/current";
	String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	String INTEGRATION_JAR="/tmp/integration_test_files/lib/*.jar";
	String PIG_HOME = "/home/y/share/pig";
	String TEZ_HOME = "/home/gs/tez/current/";
	String TEZ_CONF_DIR ="/home/gs/conf/tez/current";
	String TEZ_HEALTH_CHECKUP = "tez_health";
	String TEZ_RESULT = "tez_result";
}
