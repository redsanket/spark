package org.hw.webhdfs;

import java.util.Properties;

import org.hw.wsfrm.WSFRMUtils;

public interface WebHdfsKeys {
  public static final String OPERATOR = "op";
  
  //the different operations available
  public static final String LISTSTATUS="LISTSTATUS";
  public static final String GETFILESTATUS="GETFILESTATUS";
  public static final String OPEN="OPEN";
  public static final String CREATE="CREATE";
  public static final String MKDIRS="MKDIRS";
  public static final String DELETE="DELETE";
  public static final String RENAME="RENAME";
  public static final String SETPERMISSION="SETPERMISSION";
  public static final String SETOWNER="SETOWNER";
  public static final String SETREPLICATION="SETREPLICATION";
  public static final String SETTIMES="SETTIMES";
  public static final String APPEND="APPEND";
  public static final String GETFILECHECKSUM="GETFILECHECKSUM";
  public static final String GETCONTENTSUMMARY="GETCONTENTSUMMARY";
  public static final String GET_BLOCK_LOCATIONS="GET_BLOCK_LOCATIONS";
  public static final String GETDELEGATIONTOKEN="GETDELEGATIONTOKEN";
  public static final String RENEWDELEGATIONTOKEN="RENEWDELEGATIONTOKEN";
  public static final String CANCELDELEGATIONTOKEN="CANCELDELEGATIONTOKEN";
  
  //different parameters available
  public static final String PARAM_MODIFICATION_TIME="modificationTime";
  public static final String PARAM_ACCESS_TIME="accessTime";
  public static final String PARAM_REPLICATION="replication";
  public static final String PARAM_OWNER="owner";
  public static final String PARAM_GROUP="group";
  public static final String PARAM_PERMISSION="permission";
  public static final String PARAM_DESTINATION="destination";
  public static final String PARAM_RECURSIVE="recursive";
  public static final String PARAM_APPEND="APPEND";
  public static final String PARAM_BUFFER_SIZE="buffersize";
  public static final String PARAM_BLOCK_SIZE="blockSize";
  public static final String PARAM_OVERWRITE="overwrite";
  public static final String PARAM_OFFSET="offset";
  public static final String PARAM_LENGTH="length";
  public static final String PARAM_USER_NAME="user.name";
  public static final String PARAM_DELEGATION="delegation";
  public static final String PARAM_RENEWER="renewer";
  public static final String PARAM_TOKEN="token";
  
  //default permission for webhdfs
  public static final String DEFAULT_PERMISSION_DIR="rwxr-xr-x";
  public static final String DEFAULT_PERMISSION_FILE="rw-r--r--";
  
  //load the properties file and read the data in
  public static final Properties properties=WSFRMUtils.loadProperties("webhdfs.properties");
  public static final String SCHEME=properties.getProperty("SCHEME");
  public static final String WEBHDFS=properties.getProperty("WEBHDFS");
  public static final String PATH=properties.getProperty("PATH");
  public static final String LOG4J_FILE=properties.getProperty("LOG4J_FILE");
  public static final String LOCAL_DATA_DIR=properties.getProperty("LOCAL_DATA_DIR");
  public static final String HDFS_DATA_DIR=properties.getProperty("HDFS_DATA_DIR");
  
  //user parameters
  public static final String USER=properties.getProperty("USER");
  public static final String USER_KEYTAB_FILE=properties.getProperty("USER_KEYTAB_FILE");
  
  public static final String USER2=properties.getProperty("USER2");
  public static final String USER2_KEYTAB_FILE=properties.getProperty("USER2_KEYTAB_FILE");
  
  public static final String HADOOP_SUPER_USER=properties.getProperty("HADOOP_SUPER_USER");
  public static final String HADOOP_SUPER_USER_KEYTAB_FILE=properties.getProperty("HADOOP_SUPER_USER_KEYTAB_FILE");
  
  
  //xpath for failed messages
  public static final String XPATH_FAIL_EXCEPTION="/RemoteException/exception";
  public static final String XPATH_FAIL_JAVA_CLASSNAME="/RemoteException/javaClassName";
  public static final String XPATH_FAIL_MSG="/RemoteException/message";
  
  //kerberos commands
  public static final String KINIT_CMD=properties.getProperty("KINIT_CMD");
  public static final String KDESTROY_CMD=properties.getProperty("KDESTROY_CMD");
  public static final String KLIST_CMD=properties.getProperty("KLIST_CMD");
  
  //get the property that determines the security
  public static final String HADOOP_SECURITY_PROPERTY=properties.getProperty("HADOOP_SECURITY_PROPERTY");
}
