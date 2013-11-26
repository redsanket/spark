package tests.hadoop23.libs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import tests.hadoop23.libs.hdfs.NameNode;
import tests.hadoop23.libs.hdfs.SecondaryNameNode;

public class Cluster {
	private ProcessBuilder builder  = new ProcessBuilder();
	private Map<String, String> env = builder.environment();
	private String hadoopHome;
	private String confDir;
	private String name;
	
	private String clusterType;	
	
	private String user;	
	
	private String kerberTicketDir;
	private String kerberTicketPath;
	
	private Log LOG = LogFactory.getLog(Cluster.class);
	
	private final String TICKET_FILE_SUFFIX = ".kerberos.ticket";
	
	private MiniDFSCluster minicluster;
	private Configuration conf;
	
	ArrayList<NameNode> nnList = new ArrayList<NameNode>();
	ArrayList<SecondaryNameNode> snnList = new ArrayList<SecondaryNameNode>();
		
	public Cluster(){
		
	}
	
	public Cluster(String name, String clusterType, String hadoopHome, String confDir, String kerberTicketDir, String miniclusterUser) throws Exception {
		this.hadoopHome = hadoopHome;
		this.confDir = confDir;
		this.clusterType = clusterType;
		
		Assert.assertTrue(hadoopHome!=null);
		Assert.assertTrue(confDir!=null);
		Assert.assertTrue(clusterType!=null);
		
		try{
			builder.directory(new File(hadoopHome));			
			if(clusterType.equals("minicluster")){
				this.kerberTicketDir="";
				Assert.assertTrue(miniclusterUser!=null);
				//this.user=miniclusterUser;
				
				String coreSiteFile = confDir+"/"+"core-site.xml"; 
				final int numNN = 2;
				final int numDN = 3;
				
				conf = new HdfsConfiguration(false);
			    conf.addResource(new URL("file://"+confDir+"/"+"core-site.xml"));
			    conf.addResource(new URL("file://"+confDir+"/"+"hdfs-site.xml"));
			    
			    MiniDFSCluster.Builder builder;
			    builder = new MiniDFSCluster.Builder(conf);
			    builder.federation(true);
			    builder.numNameNodes(numNN);
			    builder.numDataNodes(numDN);
			    minicluster = builder.build();
			    minicluster.waitActive();
			    
			    //change config files with the new value of fs.defaultFS
			    //created by the minicluster builder
			    String fsconfig = conf.get("fs.defaultFS");
			    changeConfigFile(coreSiteFile, "fs.defaultFS", fsconfig);
			}
			else {
				Assert.assertTrue(kerberTicketDir!=null);
				Assert.assertTrue(name!=null);
				this.kerberTicketDir = kerberTicketDir;
				this.name = name;
				conf = new Configuration(false);
		    	File dir = new File(confDir);
		    	String[] files = dir.list();
		    	for(String filename: files){ 
		    		if (filename.matches(".+\\.xml$")){
		    			conf.addResource(new URL("file://"+confDir+"/"+filename));
		    		}		    		
		    	}
			}
			buildNNList();
			buildSNNList();
		}
		catch (NullPointerException e){
			e.printStackTrace();
			throw(e);
		}
	}

	public void buildNNList() throws IOException{
		String[] nameServices = getNameServiceFromConfig();
								
		for(int i=0; i< nameServices.length; i++){
			String namenodeAddress = conf.get("dfs.namenode.rpc-address"+"."+nameServices[i]);
			NameNode nameNode = new NameNode(this, namenodeAddress);
			nnList.add(nameNode);			
		}						
	}
	
	public void buildSNNList() throws IOException{
		String[] nameServices = getNameServiceFromConfig();
								
		for(int i=0; i< nameServices.length; i++){
			String namenodeAddress = conf.get("dfs.namenode.secondary.http-address"+"."+nameServices[i]);
			SecondaryNameNode nameNode = new SecondaryNameNode(this, namenodeAddress);
			snnList.add(nameNode);			
		}						
	}
	
	private String[] getNameServiceFromConfig() throws IOException{		
		return conf.get("dfs.federation.nameservices").split(",");
	}
	
	public String runHadoopCommand(String command) throws IOException{		
		builder.command(command.trim().split("\\s+"));
		
		if(!clusterType.equals("minicluster"))
			env.put("KRB5CCNAME", kerberTicketPath);
		
		try {
			
			builder.redirectErrorStream(true);
			System.out.println(">>> Execute command: "+command);
			Process process = builder.start();
	
			InputStream stdout= process.getInputStream();
	
	    	String str = new java.util.Scanner(stdout).useDelimiter("\\A").next();        	
	    	System.out.println(">>> Command output: ");
	    	System.out.println(str);  
	    	return str;
	    } catch (java.util.NoSuchElementException e) {
	    	LOG.info("Get no return from execute hadoop command. Notice: some command returns nothing");
	    	return "";
	    } catch (IOException e) {
	    	e.printStackTrace();
	    	throw(e);
	    }					
	}
	
	public void setUser(String newUser) throws IOException{
		if (user!=null && user.equals(newUser))
			return;
		
		user = newUser;
		
		if (!clusterType.equals("minicluster")){
			kerberTicketPath=kerberTicketDir+File.separator+user+TICKET_FILE_SUFFIX;
			System.out.println("Run as user: "+user+" with kerberos ticket at "+kerberTicketPath);
		}
		else {
			System.out.println("Run as minicluster user: "+user);
		}
//		runHadoopCommand("bin/hdfs --config " + confDir +  
//					" dfs -mkdir "+"/user/"+user);
		
	}
		
	private void changeConfigFile(String fname, String property, String value) throws Exception{
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document document;
		
		try {
			document = dbf.newDocumentBuilder().parse(new File(fname));
		    XPathFactory xpf = XPathFactory.newInstance();
	        XPath xpath = xpf.newXPath();
	        XPathExpression expression;
		
			expression = xpath.compile("//property[name='"+property+"']/value/text()");
			Object result = expression.evaluate(document, XPathConstants.NODESET);
	        
	        NodeList nodes = (NodeList) result;
	        for (int i = 0; i < nodes.getLength(); i++) {
	        	nodes.item(i).setTextContent(value);
	        }
	        
	        TransformerFactory tf = TransformerFactory.newInstance();
	        Transformer t;
			t = tf.newTransformer();
			t.transform(new DOMSource(document), new StreamResult(new FileWriter(fname)));
		} catch (Exception e) {
			e.printStackTrace();
			throw(e);
		}
	}
	
	public ArrayList<NameNode> getNNList(){
		return nnList;
	}
	
	public ArrayList<SecondaryNameNode> getSNNList(){
		return snnList;
	}
	
	public String getName(){
		return name;
	}
	
	public String getUser(){
		return user;
	}
	
	public String getConfDir(){
		return confDir;
	}
	
	public String getType(){
		return clusterType;
	}
}
