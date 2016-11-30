package hadooptest.gdm.regression.integration.clusterHealth;

import static com.jayway.restassured.RestAssured.given;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.integration.SystemCommand;
import net.sf.ezmorph.bean.MorphDynaBean;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.util.Map;

/**
 * Check whether given cluster is in SAFE_MODE and delete the given path on the cluster.
 */
public class CheckClusterHealth {
	private String clusterName;
	private String clusterNameNodeName;
	private boolean clusterMode;
	private String clusterSpace;
	private JSONArray liveNodeJsonArray;
	private Configuration configuration;
	private List<String> pathsList;
	private boolean cleanUpFlag = false;
	private int noOfDataNode = 0;
	private int dataNodeFullCount = 0;
	private static final double BLOCK_USED_LIMIT = 80.0; // increase from 50% 
	
	private static final String NAMENODE_PORT_NUMBER = "50070";
	private static final String PROCOTOL = "hdfs://";
	private static final String INTEGRATION_DATA_INSTANCE_PATH = "/data/daqdev/abf/";
	private static final String INTEGRATION_SUPPORTING_FILE_PATH = "/tmp/test_stackint/Pipeline/bidded_clicks";

	public CheckClusterHealth() {
		pathsList = new ArrayList<String>();
		this.setPathsList();
	}
	
	public void setPathsList() {
		pathsList.add(INTEGRATION_DATA_INSTANCE_PATH);
		pathsList.add(INTEGRATION_SUPPORTING_FILE_PATH);
	}
	
	public List<String> getPathsList() {
		return this.pathsList;
	}

	public void setClusterName(String clusterName) { 
		this.clusterName = clusterName;
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public void setClustertMode(boolean mode) {
		this.clusterMode = mode;
	}

	public boolean getClusterMode() {
		return this.clusterMode;
	}
	
	public void setClusterNameNode(String clusterNameNode) {
		this.clusterNameNodeName = clusterNameNode;
	}
	
	public String getClusterNameNode() {
		return this.clusterNameNodeName;
	}	
	
	/**
	 * Get the cluster mode and check whether data needs to be deleted even if one of the datanode is 50% of data.
	 * @return
	 */
	public String checkClusterMode() {
		String cMode = null;
		String nameNodeHostNameCommand =  "yinst range -ir \"(@grid_re.clusters." + this.clusterName + ".namenode)\"";
		String nameNodeHostName =  this.executeCommand(nameNodeHostNameCommand).trim();
		this.setClusterNameNode(nameNodeHostName);
		String url = "http://" + this.getClusterNameNode() + ":" + CheckClusterHealth.NAMENODE_PORT_NUMBER + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
		TestSession.logger.info("Cluster mode url = " + url);
		ConsoleHandle consoleHandle = new ConsoleHandle();
		String cookie  = consoleHandle.httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
		String responseString = response.getBody().asString();
		TestSession.logger.info("responseString = "  +  responseString);

		JSONObject responseJsonObj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray beanJsonArray = responseJsonObj.getJSONArray("beans");
		String beanStr = beanJsonArray.getString(0);
		JSONObject beanJsonObject =  (JSONObject) JSONSerializer.toJSON(beanStr.toString());
		if ( beanJsonObject.containsKey("Safemode") ){
			String currentClusterMode = beanJsonObject.getString("Safemode");
			if (currentClusterMode.equals("") && currentClusterMode.length() == 0) {
				this.setClustertMode(false);
			} else {
				this.setClustertMode(true);
			}
		}

		if (beanJsonObject.containsKey("LiveNodes")) {
			String alivenodeinfo = beanJsonObject.getString("LiveNodes");
			JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON( alivenodeinfo );
			TestSession.logger.info("jsonObject  - " + jsonObject.toString());
			Map<String, Object> myMap = (Map<String, Object>) JSONObject.toBean(jsonObject, Map.class);
			for (String key : myMap.keySet()) {
				noOfDataNode++;
				TestSession.logger.info("------------------------------------------------");
				TestSession.logger.info("dataNode name : " + key);
				MorphDynaBean MorphDynaBeanObject = (MorphDynaBean)myMap.get(key);
				TestSession.logger.info("MorphDynaBeanObject  = " + MorphDynaBeanObject.get("blockPoolUsedPercent"));
				Double temp = (Double)MorphDynaBeanObject.get("blockPoolUsedPercent");
				TestSession.logger.info("temp =  " + temp.doubleValue());
				
				// if datanode's blockPoolUsedPercent is greater than 50% then we need to delete the folders
				if (temp > BLOCK_USED_LIMIT) {
					// clean hdfs
					this.setCleanUpFlag(true);
					this.setDataNodeFullCount();
					
					// increase the datanode that is full by 50% of space
					
					
					TestSession.logger.info("DataNode " + key + "  of " + this.getClusterName()  + "  is filled with data of " + temp + "  on HDFS.  cleanUpFlag =  "  +  this.getCleanUpFlag());
					if (this.getCleanUpFlag() == true) {
						TestSession.logger.info("Clean up required ");
					}
				}
				TestSession.logger.info("------------------------------------------------");
			}
		}
		return cMode;
	}
	
	public boolean getCleanUpFlag() {
		TestSession.logger.info("Clean up required " + this.cleanUpFlag);
		return cleanUpFlag;
	}

	public void setCleanUpFlag(boolean cleanUpFlag) {
		this.cleanUpFlag = cleanUpFlag;
	}

	public int getDataNodeFullCount() {
		return dataNodeFullCount;
	}

	public void setDataNodeFullCount() {
		this.dataNodeFullCount++;
	}

	/**
	 * tells whether data should be deleted.
	 * @return
	 */
	public boolean isDataRequiredToClean() {
		return (cleanUpFlag == true && dataNodeFullCount > 1); 
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
		String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.getClusterNameNode() ;
		TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
		conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
		TestSession.logger.info(conf);
		return conf;
	}
	
	/**
	 * Delete the given path
	 * @param path path to be deleted
	 * @return
	 * @throws IOException
	 */
	public boolean deletePath(String dataPath) throws IOException {
	
		boolean isPathDeleted = false;
		Configuration configuration = this.getConfForRemoteFS();
		FileSystem hdfsFileSystem = FileSystem.get(configuration);
		if (hdfsFileSystem != null) {
			if (isPathExists(dataPath)) {
				Path path = new Path(dataPath);
				isPathDeleted =  hdfsFileSystem.delete(path, true);
				if (isPathDeleted == true) {
					TestSession.logger.info(dataPath + " is deleted successfully");
				} else {
					TestSession.logger.info("Failed to delete " + dataPath);
				}
			}
		} else {
			TestSession.logger.error("Failed to instance of FileSystem ");
		}
		return isPathDeleted;
	}
	/**
	 * Check whether the given path exists.
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public  boolean isPathExists(String dataPath) throws IOException {
		boolean flag = false;
		Configuration configuration = this.getConfForRemoteFS();
		FileSystem hdfsFileSystem = FileSystem.get(configuration);
		if (hdfsFileSystem != null) {
			Path path = new Path(dataPath);
			if (hdfsFileSystem.exists(path)) {
				flag = true;
			} else {
				TestSession.logger.info(path.toString() + " path does not exists.");
			}
		} else {
			TestSession.logger.error("Failed to create an instance of FileSystem.");
		}
		return flag;
	}
	
	/**
	 * Execute a given command and return the output of the command.
	 * @param command
	 * @return
	 */
	public String executeCommand(String command) {
		String output = null;
		TestSession.logger.info("command - " + command);
		ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
		if ((result == null) || (result.getLeft() != 0)) {
			if (result != null) { 
				// save script output to log
				TestSession.logger.info("Command exit value: " + result.getLeft());
				TestSession.logger.info(result.getRight());
			} else {
				TestSession.logger.info("Failed to run the command - " + command);
				throw new RuntimeException("Exception" );
			}
		} else {
			output = result.getRight();
			TestSession.logger.info("log = " + output);
		}
		return output;
	}
}
