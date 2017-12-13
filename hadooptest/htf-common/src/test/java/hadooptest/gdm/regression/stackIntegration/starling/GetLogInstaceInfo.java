package hadooptest.gdm.regression.stackIntegration.starling;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

public class GetLogInstaceInfo implements Callable<String> {
    
    private static final String PROCOTOL = "hdfs://";
    private static final String COMMON_LOG_PATH = "/projects/starling/hadoopqa/logs/";
    private static final String TEZ_LOG_PATH = "/projects/starling/hadoopqa/logs/tez/hourly/";
    private String nameNodeName;
    private String logType;
    private String instance;
    
    public GetLogInstaceInfo(String nameNodeName , String logType) {
	this.setNameNodeName(nameNodeName);
	this.setLogType(logType);
    }
    
    private String getNameNodeName() {
        return nameNodeName;
    }

    private void setNameNodeName(String nameNodeName) {
        this.nameNodeName = nameNodeName;
    }

    private String getLogType() {
        return logType;
    }

    private void setLogType(String logType) {
        this.logType = logType;
    }
    
    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    /**
     * Returns the remote cluster configuration object.
     * @param aUser  - user
     * @param nameNode - name of the cluster namenode.
     * @return
     * @throws IOException
     */
    public Configuration getConfForRemoteFS() throws IOException {
        Configuration conf = new Configuration(true);
        String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.getNameNodeName();
        TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
        conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
        TestSession.logger.info(conf);
        return conf;
    }
    
    private void findLatestLog(List<FileStatus> fStatusList) {
	TestSession.logger.info("~~~~~~~~~~~~~~~~~~~ findLatestLog ~~~~~~~~~~~~~~~~~~~");
	String fileName = "";
	long fileCreateTime = 0L;
	for ( FileStatus fStatus : fStatusList) {
	    long mTime = fStatus.getModificationTime();
	    if( mTime > fileCreateTime) {
		fileCreateTime = mTime;
		fileName = fStatus.getPath().getName();
	    }
	}
	TestSession.logger.info("Latest log - " + fileName);
	TestSession.logger.info("~~~~~~~~~~~~~~~~~~~ findLatestLog ~~~~~~~~~~~~~~~~~~~");
    }
    
    private void getLatestInstance(String logType) throws IOException {
	Configuration conf = this.getConfForRemoteFS();
	FileSystem hFileSystem = FileSystem.get(conf);
	Path lPath = logType.equals("tez") ?  new Path(TEZ_LOG_PATH) : new Path(COMMON_LOG_PATH + logType);
	TestSession.logger.info("** lPath -  " + lPath.toString());
	
	List<FileStatus> fileStatusList = hFileSystem.listStatus(lPath).length > 0 ? Arrays.asList(hFileSystem.listStatus(lPath)) : null;
	Map<Long, String> unSortedInstanceMap = new HashMap<Long,String>();
	if (fileStatusList != null && fileStatusList.size() > 0) {
	    findLatestLog(fileStatusList);
	    for ( FileStatus fStatus : fileStatusList) {
		String fName = fStatus.getPath().getName();
		long mTime = fStatus.getModificationTime();
		unSortedInstanceMap.put(mTime, fName);
		System.out.println("name - " + fStatus.getPath() + "  mTime " + mTime);
	    }
	    
	    if (unSortedInstanceMap.size() > 0) {
		unSortedInstanceMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
		.collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		
		boolean flag = false;
		java.util.Iterator<Entry<Long, String>> it = unSortedInstanceMap.entrySet().iterator();
		while ( it.hasNext()) {
		    Map.Entry pair = (Map.Entry)it.next();
		    System.out.println(pair.getKey() + " = " + pair.getValue());
		    if (!flag) {
			this.setInstance( (String)pair.getValue());
			flag = true;
		    }
		}
	    }
	} else {
	    System.out.println("There is no files for " + logType );
	}
    }

    @Override
    public String call() throws Exception {
	getLatestInstance(this.getLogType().trim());
	return "GetLogInstaceInfo~" + this.getLogType().trim() + "~" + this.getInstance().trim();
    }
}