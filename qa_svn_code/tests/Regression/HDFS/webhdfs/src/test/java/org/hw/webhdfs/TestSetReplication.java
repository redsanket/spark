package org.hw.webhdfs;

import static org.junit.Assert.assertEquals;


import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestSetReplication extends WebHdfs{
  
  //test set replication factor on a file
  @Test 
  public void setReplicationFactorOnFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_replication_tests/file_set_rep_factor.txt";
    String rep="5";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETREPLICATION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION, rep));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    short actRep = dfs.getFileStatus(new Path (path)).getReplication();
    short expRep = Short.valueOf(rep);
    assertEquals("Check replication factor for path: "+path,expRep,actRep);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());  
  }
  
  //test set replication factor on a Dir, should be a no-op, remain 0
  //HDFS-2432
  @Test 
  public void setReplicationFactorOnDir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_replication_tests";
    String rep="5";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETREPLICATION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION, rep));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    XPathUtils.checkXpathTextContent("boolean","false",rsp.getDocument());  
  }
  
  //test set invalid replication factor on a file
  @Test 
  public void setInvalidReplication() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_replication_tests/file_set_invalid_rep_factor.txt";
    String rep="a";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETREPLICATION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION, rep));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"replication\": Failed to parse \"a\" as a radix-10 short integer.",rsp.getDocument());
    
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }

}
