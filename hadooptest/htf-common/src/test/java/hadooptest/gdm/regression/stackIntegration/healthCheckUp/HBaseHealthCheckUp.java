package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import static com.jayway.restassured.RestAssured.given;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class HBaseHealthCheckUp implements Callable<StackComponent>{
	
	private String hostName;
	private String hbaseMasterHostName;
	private String hbaseMasterPortNo;
	private String cookie;
	private ConsoleHandle consoleHandle ;
	private CommonFunctions commonFunctionsObj;
	private final String COMPONENT_NAME = "hbase";
	private final String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private StackComponent stackComponent;
	
	public HBaseHealthCheckUp() {
		this.commonFunctionsObj = new CommonFunctions();
		this.stackComponent = new StackComponent();
		this.setHbaseMasterHostName(GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim());
		this.setHostName(this.getHbaseMasterHostName());
		this.setHbaseMasterPortNo(GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterPort").trim());
		this.consoleHandle = new ConsoleHandle();
		this.setCookie(consoleHandle.httpHandle.getBouncerCookie());
	}
	
	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public String getHbaseMasterHostName() {
		return hbaseMasterHostName;
	}

	public void setHbaseMasterHostName(String hbaseMasterHostName) {
		this.hbaseMasterHostName = hbaseMasterHostName;
	}

	public String getHbaseMasterPortNo() {
		return hbaseMasterPortNo;
	}

	public void setHbaseMasterPortNo(String hbaseMasterPortNo) {
		this.hbaseMasterPortNo = hbaseMasterPortNo;
	}

	@Override
	public StackComponent call() throws Exception {
		this.stackComponent.setStackComponentName(COMPONENT_NAME);
		this.stackComponent.setHostName(this.getHostName());
		this.stackComponent.setDataSetName(this.commonFunctionsObj.getDataSetName());
		String currentDataSet = this.stackComponent.getDataSetName();
		this.getHBaseHealthCheck();
		if (getHBaseRegionalServerHealthCheckup() == 0) {
			this.stackComponent.setHealth(false);
			this.commonFunctionsObj.updateDB(currentDataSet, "hbaseResult", "FAIL");
			this.commonFunctionsObj.updateDB(currentDataSet, "hbaseCurrentState", "COMPLETED");
			this.commonFunctionsObj.updateDB(currentDataSet, "hbaseComments", "Regional servers are down, but HBase master is up.");
			this.stackComponent.setErrorString("Regional servers are down, but HBase master is up.");
		}
		return this.stackComponent;
	}
	
	/**
	 * Check whethe HBase Master is up or down. If the hbase master is up it returns the value as "active~" + hbase version.
	 * @return
	 */
	private void getHBaseHealthCheck() {
		String hbaseVersion = null;
		boolean flag= false;
		try {
			String hbaseJmxUrl = "http://" + this.getHbaseMasterHostName() + ":" + this.getHbaseMasterPortNo() + "/jmx?qry=java.lang:type=Runtime"; 
			com.jayway.restassured.response.Response response = given().cookie(this.getCookie()).get(hbaseJmxUrl);
			String reponseString = response.getBody().asString();
			JSONObject obj =  (JSONObject) JSONSerializer.toJSON(reponseString.toString());
			JSONArray beanJsonArray = obj.getJSONArray("beans");
			String str = beanJsonArray.getString(0);
			JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(str.toString());
			TestSession.logger.info("name  = " +obj1.getString("name") );
			JSONArray SystemPropertiesJsonArray = obj1.getJSONArray("SystemProperties");
			if ( SystemPropertiesJsonArray.size() > 0 ) {
				Iterator iterator = SystemPropertiesJsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String key = jsonObject.getString("key");
					if (key.equals("java.class.path")) {
						List<String> paths = Arrays.asList(jsonObject.getString("value").split(":"));
						for ( String value : paths) {
							if (value.startsWith("/home/y/libexec/hbase/bin/../lib/hbase-client-")) {
								hbaseVersion = Arrays.asList(value.split("-")).get(2).replaceAll(".jar", "");
								TestSession.logger.info("hbaseVersion  = " + hbaseVersion);
								this.stackComponent.setHealth(true);
								this.stackComponent.setStackComponentVersion(hbaseVersion);
								flag = true;
								break;
							}
						}
						if (flag == true) break;
					}
					if (flag == true) break;
				}
			}
			TestSession.logger.info("hbaseVersion  = " + hbaseVersion);

		} catch(Exception e) {
			TestSession.logger.info("exception " + e );
			e.printStackTrace();
			this.stackComponent.setHealth(false);
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setErrorString(e.getMessage());
			TestSession.logger.info("exception  --------------");
		} finally {
			if (flag == false) {
				hbaseVersion = "down";	
				this.stackComponent.setHealth(false);
				this.stackComponent.setStackComponentVersion("0.0");
			}
			TestSession.logger.info("hbaseVersion  = " + hbaseVersion);
		}
	}
	
	/**
	 * Check whether there is any hbase regional server or up and running.
	 * @return
	 */
	public int getHBaseRegionalServerHealthCheckup() {
		int regionalServersHostSize = 0;
		try{
			List<String> reginalServerHostNameList ;
			String hbaseMasterHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
			String hbaseMasterPortNo = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterPort").trim();
			ConsoleHandle consoleHandle = new ConsoleHandle();
			String cookie  = consoleHandle.httpHandle.getBouncerCookie();
			String hbaseJmxUrl = "http://" + hbaseMasterHostName + ":" + hbaseMasterPortNo + "/jmx?qry=hadoop:service=Group,name=Group";
			com.jayway.restassured.response.Response response = given().cookie(cookie).get(hbaseJmxUrl);
			String reponseString = response.getBody().asString();
			TestSession.logger.info("reponseString = " + reponseString);

			JSONObject obj =  (JSONObject) JSONSerializer.toJSON(reponseString.toString());
			JSONArray beanJsonArray = obj.getJSONArray("beans");
			String str = beanJsonArray.getString(0);
			JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(str.toString());
			TestSession.logger.info("name  = " +obj1.getString("name") );
			JSONArray SystemPropertiesJsonArray = obj1.getJSONArray("ServersByGroup");

			TestSession.logger.info("SystemPropertiesJsonArray = " + SystemPropertiesJsonArray.toString());
			if ( SystemPropertiesJsonArray.size() > 0 ) {
				Iterator iterator = SystemPropertiesJsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					String key = jsonObject.getString("key");
					if (key.equals("default")) {
						JSONArray regionalServerHostJsonArray = jsonObject.getJSONArray("value");
						String tempRegionalServersHostNames = regionalServerHostJsonArray.toString();
						if (tempRegionalServersHostNames.length() > 0 && tempRegionalServersHostNames != null) {
							reginalServerHostNameList  = Arrays.asList(tempRegionalServersHostNames.split(","));
							regionalServersHostSize = reginalServerHostNameList.size();
							TestSession.logger.info("regionalServerHostJsonArray   = " + reginalServerHostNameList);
							break;
						} else {
							// if there is no regional server exists or down.
							regionalServersHostSize = 0;
						}
					}
				}
			}
		}catch(Exception e) {
			regionalServersHostSize = 0;
			this.stackComponent.setHealth(false);
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setErrorString(e.getMessage());
			TestSession.logger.info("** HBase Regional servers are down..! **** " + e);
		}
		return  regionalServersHostSize;
	}

}
