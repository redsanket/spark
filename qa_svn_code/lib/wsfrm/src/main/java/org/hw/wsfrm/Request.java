package org.hw.wsfrm;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.xml.sax.SAXException;

public  class Request {
  private Response response;
  private HttpResponse httpResponse;
  private DefaultHttpClient client;
  private static final Logger LOG = Logger.getLogger(Request.class);
  private URI uri;
  
  public Request() {
    this.uri=null;
    this.client= new DefaultHttpClient();
  }
  
  public Request(URI uri) {
    this();
    this.uri=uri;
  }
  
  //method to set the URI, call this before calling the request
  public Request(String scheme, String host, int port, String path, List<NameValuePair> qparams) throws URISyntaxException {
      this();
      setUri(scheme, host, port, path, qparams);
  }
  
  //method to set the URI, call this before calling the request
  public void setUri(String scheme, String host, int port, String path, List<NameValuePair> qparams) throws URISyntaxException {
      this.uri = URIUtils.createURI(scheme, host, port, path, URLEncodedUtils.format(qparams, WSFRMKeys.UTF_8_ENCODING), null);
  }
  
  //method to make a get call
  public Response Get() throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    HttpGet get = new HttpGet(uri);
    return sendRequest(get);
  }
  
  //method to make a put call
  public Response Put() throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    HttpPut put = new HttpPut(uri);
    return sendRequest(put);
  }
  
  //method to make a put call, and upload a file
  public Response Put(File file) throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    
    HttpPut put = new HttpPut(uri);
    
    FileEntity fileEntity = new FileEntity(file, WSFRMKeys.BINARY_OCTET_STREAM_CONTENT_TYPE);
    put.setEntity(fileEntity);
    return sendRequest(put);
  }
  
  //method to make a put call, and upload a file with a custom redirect startegy
  public Response Put(File file, DefaultRedirectStrategy redirectStrategy) throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    
    client.getParams().setParameter(CoreProtocolPNames.USE_EXPECT_CONTINUE, true);
    client.setRedirectStrategy(redirectStrategy);
    
    HttpPut put = new HttpPut(uri);
    
    FileEntity fileEntity = new FileEntity(file, WSFRMKeys.BINARY_OCTET_STREAM_CONTENT_TYPE);
    put.setEntity(fileEntity);
    return sendRequest(put);
  }
  
  //method to make a postt call
  public Response Post() throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    HttpPost post = new HttpPost(uri);
    return sendRequest(post);
  }
  
  //method to make a postt call, and upload a file
  public Response Post(File file) throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    HttpPost post = new HttpPost(uri);
    FileEntity fileEntity = new FileEntity(file, WSFRMKeys.BINARY_OCTET_STREAM_CONTENT_TYPE);
    post.setEntity(fileEntity);
    fileEntity.setContentType(WSFRMKeys.BINARY_OCTET_STREAM_CONTENT_TYPE);
    return sendRequest(post);
  }
  
  //method to make a delete call
  public Response Delete() throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    HttpDelete delete = new HttpDelete(uri);
    return sendRequest(delete);
  }
  
  //method to send the request
  private Response sendRequest(HttpRequestBase hrb) throws ClientProtocolException, IOException, ParserConfigurationException, SAXException, JSONException {
    Response response = null;
    
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request Uri: " + hrb.getURI());
        LOG.debug("Request Method: " + hrb.getMethod());
      }
      httpResponse = client.execute(hrb);
      response=new Response(httpResponse);
      //set the response object
      client.getConnectionManager().shutdown();
    
    return response;
  }

  //method to get the response
  public Response getResponse() {
    return response;
  }
  
//method to get the response
  public HttpResponse getHttpResponse() {
    return httpResponse;
  }
}