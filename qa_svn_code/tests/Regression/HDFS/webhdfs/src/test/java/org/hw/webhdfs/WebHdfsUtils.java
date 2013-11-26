package org.hw.webhdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.json.JSONException;
import org.xml.sax.SAXException;

public class WebHdfsUtils {
  
  private static final Logger LOG = Logger.getLogger(WebHdfsUtils.class);
  
  //method to delete a given path as a super user
  //method will fail if delete fails
  public static void deletePathUsingWebHdfs(String host, int port, String path,List<NameValuePair> qparams) throws URISyntaxException, ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    //generate the query parameters
    qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_RECURSIVE, "true"));
    Request rqs = new Request();
    rqs.setUri(WebHdfsKeys.SCHEME,host,port,WebHdfsKeys.PATH+path,qparams);
    Response rsp = rqs.Delete();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  public static String getDToken(String host, int port,String user, String keytab, String renewer) throws IOException, InterruptedException {
    LOG.debug("Get Delegation Token");
    String token=null;
    kinit(user,keytab);
    klist();
    
    String currDir=System.getProperty("user.dir");
    String url=null;
    if (renewer.isEmpty()) {
      url=WebHdfsKeys.SCHEME+"://"+host+":"+String.valueOf(port)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.GETDELEGATIONTOKEN;
    }
    else {
      url=WebHdfsKeys.SCHEME+"://"+host+":"+String.valueOf(port)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.GETDELEGATIONTOKEN+"&="+WebHdfsKeys.PARAM_RENEWER+"="+renewer;
    }
    Process exec=Runtime.getRuntime().exec(currDir+"/getToken.sh "+url); 
    Process p=exec;
    p.waitFor(); 
    BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream())); 
    token=reader.readLine(); 
    LOG.debug("Delegation Token is -> " + token);
    return token;
  }
  
  //method to kinit
  public static void kinit(String user, String keytab) throws IOException, InterruptedException {
    String kinit_cmd=WebHdfsKeys.KINIT_CMD+" -kt " + keytab + " "+user;
    run_system_cmd(kinit_cmd);
  }
  
  //method to klist
  public static void klist() throws IOException, InterruptedException {
    run_system_cmd(WebHdfsKeys.KLIST_CMD);
  }
  
  //method to kdestroy
  public static void kdestroy() throws IOException, InterruptedException {
    run_system_cmd(WebHdfsKeys.KDESTROY_CMD);
  }
  
  //method to run a system command
  public static List<String> run_system_cmd_old(String cmd) throws IOException, InterruptedException {
    LOG.debug("Run CMD -> " +cmd);
    Process exec = Runtime.getRuntime().exec(cmd);
    Process p=exec; 
    p.waitFor(); 
    BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream())); 
    // BufferedReader reader=new BufferedReader(new InputStreamReader(p.getErrorStream())); 
    String line=reader.readLine(); 
    List<String> output=new ArrayList<String>();
    while(line!=null) 
    { 
      LOG.debug(line); 
      output.add(line);
      line=reader.readLine(); 
    }
    return output;
  }

  //method to run a system command
  public static List<String> run_system_cmd(String cmdStr) throws IOException, InterruptedException {
      return run_system_cmd(cmdStr.split(" "));
  }

  //method to run a system command
  public static List<String> run_system_cmd(String[] cmd) throws IOException, InterruptedException {
    String cmdStr = "";
    for (String str : cmd) { cmdStr += str + " "; }
    LOG.debug("Run CMD -> " +cmdStr);

    ProcessBuilder pb = new ProcessBuilder(cmd);
    List<String> command = pb.command();
    pb.redirectErrorStream(true);
    final Process process = pb.start();
    InputStream is = process.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);

    String line;
    List<String> output=new ArrayList<String>();
    while ((line = br.readLine()) != null) {
        LOG.debug(line); 
        output.add(line);
    }

    int exitValue  = process.waitFor();
    LOG.debug("Process exit value = " + exitValue);

    return output;
  }

}
