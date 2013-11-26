package org.hw.webhdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Content;
import org.hw.wsfrm.JSONContent;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Ignore;
import org.junit.Test;

public class TestSecurity extends WebHdfs {

  @Test 
  public void securityOff_sendNoUser() throws Exception {
    if (! IS_SECURE) {
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 200 with appropriate content type
      WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    }
  }
  
  @Test 
  //if security is on dont send any authentication
  public void securityOn_sendNoUser() throws Exception {
    if (IS_SECURE) {
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 401 with appropriate content type
      WSFRMUtils.checkHttp401(rsp,WSFRMKeys.TEXT_HTML_CONTENT_TYPE);
      
      Header hdr = rsp.getHeader("WWW-Authenticate");
      assertEquals("Check the WWW-Authenticate header value","Negotiate",hdr.getValue());
    }
  }
  
  @Test 
  @Ignore
  //ignore the test until you determine if this is a bug or not
  //if security is on dont send wrong token
  public void securityOn_sendWrongDTokenAddInvalidStringAtEnd() throws Exception {
    if (IS_SECURE) {
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, DToken+"makeitinvalid"));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 500 with appropriate content type
      WSFRMUtils.checkHttp500(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
      
      //check the response content
      checkFailedResponse("ContainerException","com.sun.jersey.api.container.ContainerException","Exception obtaining parameters",rsp.getDocument());
    }
  }
  
  @Test 
  //if security is on dont send wrong token
  public void securityOn_sendWrongDToken() throws Exception {
    if (IS_SECURE) {
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, "invalid"));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 401 with appropriate content type
      WSFRMUtils.checkHttp401(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
      
      //check the response content
      checkFailedResponse("SecurityException","java.lang.SecurityException","Failed to obtain user group information: java.io.EOFException",rsp.getDocument());
    }
  }
  
  @Test 
  //if security is on, renew delegation token
  public void securityOn_renewDTokenWithNoRenwer() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, "");
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.RENEWDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      List<String> output = WebHdfsUtils.run_system_cmd(cmd);
      Boolean foundStatus=false;
      Boolean foundContentType=false;
      Iterator<String> it = output.iterator();
      String jsonResponse="";
      while (it.hasNext()) {
        //check if the status has been found
        if ((!foundStatus)&&(it.next().equals("HTTP/1.1 200 OK"))) {
          foundStatus=true;
        }
        
        //if the status has been found see if you can see the content type
        if ( (foundStatus) && (!foundContentType) && (it.next().equals("Content-Type: "+WSFRMKeys.JSON_CONTENT_TYPE)) ) {
          foundContentType=true;
        }
        
        //get the json response
        String data = it.next();
        if (data.contains("\"long\"")) {
          jsonResponse=data;
          break;
        }
      }
     
      assertTrue("Found Status Line" , foundStatus);
      assertTrue("Found Content Type", foundContentType);
      Content content = new JSONContent(jsonResponse);
      XPathUtils.checkXpathTextContentWithRegex("/long", "[0-9]{13}", content.getDocument());
    }
  }
  
  @Test 
  //if security is on, renew delegation token
  public void securityOn_useRenewedDToken() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, "");
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.RENEWDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      WebHdfsUtils.run_system_cmd(cmd);
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, token));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 200 with appropriate content type
      WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    }
  }
  
  @Test 
  //if security is on, renew delegation token
  public void securityOn_renewDTokenWithRenwer() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, USER);
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.RENEWDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      List<String> output = WebHdfsUtils.run_system_cmd(cmd);
      Boolean foundStatus=false;
      Boolean foundContentType=false;
      Iterator<String> it = output.iterator();
      String jsonResponse="";
      while (it.hasNext()) {
        //check if the status has been found
        if ((!foundStatus)&&(it.next().equals("HTTP/1.1 200 OK"))) {
          foundStatus=true;
        }
        
        //if the status has been found see if you can see the content type
        if ( (foundStatus) && (!foundContentType) && (it.next().equals("Content-Type: "+WSFRMKeys.JSON_CONTENT_TYPE)) ) {
          foundContentType=true;
        }
        
        //get the json response
        String data = it.next();
        if (data.contains("\"long\"")) {
          jsonResponse=data;
          break;
        }
      }
     
      assertTrue("Found Status Line" , foundStatus);
      assertTrue("Found Content Type", foundContentType);
      Content content = new JSONContent(jsonResponse);
      XPathUtils.checkXpathTextContentWithRegex("/long", "[0-9]{13}", content.getDocument());
    }
  }
  
  @Test 
  //if security is on, renew delegation token
  public void securityOn_renewDTokenWithWrongRenwer() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, USER);
      
      //kinit as a user
      WebHdfsUtils.kinit(WebHdfsKeys.USER2, WebHdfsKeys.USER2_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.RENEWDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      List<String> output = WebHdfsUtils.run_system_cmd(cmd);
      Boolean foundStatus=false;
      Boolean foundContentType=false;
      Iterator<String> it = output.iterator();
      String jsonResponse="";
      while (it.hasNext()) {
        //check if the status has been found
        if ((!foundStatus)&&(it.next().equals("HTTP/1.1 403 Forbidden"))) {
          foundStatus=true;
        }
        
        //if the status has been found see if you can see the content type
        if ( (foundStatus) && (!foundContentType) && (it.next().equals("Content-Type: "+WSFRMKeys.JSON_CONTENT_TYPE)) ) {
          foundContentType=true;
        }
        
        //get the json response
        String data = it.next();
        if (data.contains("\"RemoteException\"")) {
          jsonResponse=data;
          break;
        }
      }
     
      assertTrue("Found Status Line" , foundStatus);
      assertTrue("Found Content Type", foundContentType);
      Content content = new JSONContent(jsonResponse);
      //check the response content
      checkFailedResponse("AccessControlException","org.apache.hadoop.security.AccessControlException","Client "+WebHdfsKeys.USER2+" tries to renew a token with renewer specified as "+USER,content.getDocument());
    }
  }
  
  @Test 
  //if security is on, cancel delegation token
  public void securityOn_cancelDToken() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, USER);
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.CANCELDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      List<String> output = WebHdfsUtils.run_system_cmd(cmd);
      Boolean foundStatus=false;
      Boolean foundContentType=false;
      Iterator<String> it = output.iterator();
      while (it.hasNext()) {
        //check if the status has been found
        if ((!foundStatus)&&(it.next().equals("HTTP/1.1 200 OK"))) {
          foundStatus=true;
        }
        
        //if the status has been found see if you can see the content type
        if ( (foundStatus) && (!foundContentType) && (it.next().equals("Content-Type: "+WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE)) ) {
          foundContentType=true;
          break;
        }
      }
     
      assertTrue("Found Status Line" , foundStatus);
      assertTrue("Found Content Type", foundContentType);
    }
  }
  
  public void securityOn_useCanceledDToken() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, USER);
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.CANCELDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      WebHdfsUtils.run_system_cmd(cmd);
      
      List<NameValuePair> qparams=new ArrayList<NameValuePair>();
      //generate the query parameters
      qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, token));
      
      Request rqs = new Request();
      rqs.setUri(SCHEME,HOST,PORT,PATH+"/",qparams);
      Response rsp = rqs.Get();
      
      //check if we get 401 with appropriate content type
      WSFRMUtils.checkHttp401(rsp,WSFRMKeys.TEXT_HTML_CONTENT_TYPE);
    }
  }
  
  @Test 
  //if security is on, cancel delegation token thats already canceled
  public void securityOn_cancelDTokenThatsCanceled() throws Exception {
    if (IS_SECURE) {
      
      //get a token
      String token = WebHdfsUtils.getDToken(HOST,PORT,USER, WebHdfsKeys.USER_KEYTAB_FILE, USER);
      
      //kinit as a user
      WebHdfsUtils.kinit(USER, WebHdfsKeys.USER_KEYTAB_FILE);
      
      String host=WebHdfsKeys.SCHEME+"://"+HOST+":"+String.valueOf(PORT)+WebHdfsKeys.PATH+"?op="+WebHdfsKeys.CANCELDELEGATIONTOKEN+"&"+WebHdfsKeys.PARAM_TOKEN+"="+token;
      String currDir=System.getProperty("user.dir");
      String cmd=currDir+"/renewOrCancelDToken.sh "+host;
      //run the cmd and capture the output
      //cancel it twice
      WebHdfsUtils.run_system_cmd(cmd);
      List<String> output = WebHdfsUtils.run_system_cmd(cmd);
      Boolean foundStatus=false;
      Boolean foundContentType=false;
      Iterator<String> it = output.iterator();
      String jsonResponse="";
      while (it.hasNext()) {
        //check if the status has been found
        if ((!foundStatus)&&(it.next().equals("HTTP/1.1 403 Forbidden"))) {
          foundStatus=true;
        }
        
        //if the status has been found see if you can see the content type
        if ( (foundStatus) && (!foundContentType) && (it.next().equals("Content-Type: "+WSFRMKeys.JSON_CONTENT_TYPE)) ) {
          foundContentType=true;
        }
        
        //get the json response
        String data = it.next();
        if (data.contains("\"RemoteException\"")) {
          jsonResponse=data;
          break;
        }
      }
     
      assertTrue("Found Status Line" , foundStatus);
      assertTrue("Found Content Type", foundContentType);
      Content content = new JSONContent(jsonResponse);
      checkFailedResponse("InvalidToken","org.apache.hadoop.security.token.SecretManager$InvalidToken","Token not found",content.getDocument());
    }
  }
}