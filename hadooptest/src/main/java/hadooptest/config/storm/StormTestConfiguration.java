package hadooptest.config.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

@SuppressWarnings("serial")
public class StormTestConfiguration extends HashMap<String, Object> {
	ArrayList<TestInfo> testList = new ArrayList<TestInfo>();
	HashMap<String, Object> nodeList;
	
	public static class TestInfo extends HashMap<String, Object>{
		String className;
		String testName;
		String output;
		Integer timeout;
		Integer uptime;
		Integer testNum;
		String expectedFile;
		String testHome;
		HashMap<String, Object> info;
		private String artifactsDir;
		
		public TestInfo(Map testInfo){
			super(testInfo);
			testName = (String) testInfo.get("name");
			className =  (String) testInfo.get("test_class");
			timeout = (Integer)testInfo.get("timeout");
			uptime = (Integer)testInfo.get("uptime");
			output = (String)testInfo.get("output");
			expectedFile = (String)testInfo.get("expected_file");
			artifactsDir = (String)testInfo.get("artifactsDir");
			testHome = (String)testInfo.get("testHome");			 
			testNum = 0;
		}
		
		/*
		public Test getTest(StormCluster cluster) throws ClassNotFoundException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException{
			Class<TestTopology> testClass = (Class<TestTopology>) Class.forName(className);
			Constructor<TestTopology> ctor = testClass.getConstructor(new Class[]{TestInfo.class, StormCluster.class});
			return ctor.newInstance(new Object[]{this, cluster});
		}
		*/
		
		public String getTestName() {
			return testName;
		}
		
		public Integer getTimeout(){
			return timeout;
		}
		
		public Integer getUpTime() {
			return uptime;
		}
		
		public String getOutputLocation() {
			return output;
		}

		public Integer getTestNum(){
			return testNum;
		}
		
		public String getExpectedFileName() {
			return expectedFile;
		}
		
		public String getTestHome(){
			return testHome;
		}
		
		public void setRootArtifactsDir(String artifactsDir){
			this.artifactsDir = artifactsDir;		
		}
		
		public String getRootArtifactsDir(){
			return artifactsDir;		
		}
		
		public String getTAPFile(){
			return artifactsDir+"/TAP.log";
		}
		
		public String getTestArtifactDir(){
			return artifactsDir+"/"+testName;
		}
	}
	
	public Map<String, Object> getNodeList() {
		return nodeList;
	}
	
	public StormTestConfiguration(String configFile, String testHome, String artifactsDir)  {
		Yaml yaml = new Yaml();		
		
		InputStream mainConfigStream;
		try {
			mainConfigStream = new FileInputStream(new File(configFile));
			put("testHome", testHome);
			if(mainConfigStream!=null){
				HashMap<String, Object> config = (HashMap<String, Object>) yaml.load(mainConfigStream);
				putAll(config);
				
				HashMap<String, Object> mainConfig = (HashMap<String, Object>) get("Main");
				
				String testListFile = (String) mainConfig.get("test_list");
				HashSet<String> testToRun=null;
				if (mainConfig.containsKey("test_to_run"))
					testToRun = new HashSet<String> ((ArrayList<String>) mainConfig.get("test_to_run"));
				
				if (testListFile != null){
					InputStream testConfigStream;
					try {
						testConfigStream = new FileInputStream(new File(testListFile));
						HashMap<String, Object> testListConfig = 
								(HashMap<String, Object>) yaml.load(testConfigStream);
//						putAll(testListConfig);
						
						ArrayList<Map> testListInfo = (ArrayList<Map>) testListConfig.get("Tests");
						if (testListInfo==null){
							System.out.println("Test list not found");
						}
						else {
							for (Map test: testListInfo){
								if (testToRun != null) {
									if (testToRun.contains(test.get("name"))){
										test.put("artifactsDir", artifactsDir);
										test.put("testHome", testHome);
										testList.add(new TestInfo(test));
									}
								} else {
									test.put("artifactsDir", artifactsDir);
									test.put("testHome", testHome);
									testList.add(new TestInfo(test));
								}
							}
						}
					} catch (FileNotFoundException e) {
						e.printStackTrace();
						System.out.println("Test list file not found");
						System.exit(1);
					}				
				}
				else {
					System.out.println("Test list file not specified");
					System.exit(1);
				}
				
				String nodeListFile = (String) ((HashMap<String, Object>) get("Main")).get("node_list");
				if (nodeListFile != null){
					InputStream nodeListStream = new FileInputStream(new File(nodeListFile));
					nodeList = (HashMap<String, Object>) yaml.load(nodeListStream);
					
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Test main config file not found");
			System.exit(1);			
		}
					
	}	
	
	public String getStormConfigPath(){
		@SuppressWarnings("unchecked")
		HashMap<String, Object> mainTestConfig = (HashMap<String, Object>) this.get("Main");
		return (String) mainTestConfig.get("storm_config_path");		
	}
	
	public String getStormConfigFile(){
		@SuppressWarnings("unchecked")
		HashMap<String, Object> mainTestConfig = (HashMap<String, Object>) this.get("Main");
		return (String) mainTestConfig.get("storm_config_file");
	}
	
	public String getTestJar(){
		@SuppressWarnings("unchecked")
		HashMap<String, Object> mainTestConfig = (HashMap<String, Object>) this.get("Main");
		return (String) mainTestConfig.get("test_jar");
	}
	
	public ArrayList<TestInfo> getTestList(){
		return testList;
	}
	
	
		
}
