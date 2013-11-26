package org.hw.wsfrm;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class Response {
  private String content;
  private long contentLength;
  private Header contentTypeHeader;
  private String contentType;
  private HttpEntity entity;
  private Content contentObject;
  private String protocolVersion;
  private int statusCode;
  private String responseMsg;
  private String statusLine;
  private HttpResponse httpResponse;
  private static final Logger LOG = Logger.getLogger(Response.class);
  
  public Response(HttpResponse response) throws ParserConfigurationException, SAXException, JSONException, IOException {
    
      if (LOG.isDebugEnabled()) {
        LOG.debug("Status Line: " + response.getStatusLine().toString());  
      }
      httpResponse=response;
      protocolVersion=response.getStatusLine().getProtocolVersion().toString();
      statusCode=response.getStatusLine().getStatusCode();
      responseMsg=response.getStatusLine().getReasonPhrase();
      statusLine=response.getStatusLine().toString();
      
      entity=response.getEntity();
      if (entity != null) {
        content=IOUtils.toString(entity.getContent(),"UTF-8");
        //if trace is enabled print the data
        if (LOG.isTraceEnabled()) {
          LOG.trace("Response Content: " + content);
        }
        contentLength=entity.getContentLength();
        contentTypeHeader=entity.getContentType();
        contentType=contentTypeHeader.getValue();
        if ( LOG.isDebugEnabled()) {
          LOG.debug("Response content type "+contentType);
        }
        /*
        //if the content is null instatiate a null content object
        if (null == content ){
          contentObject = new EmptyContent();
        }*/
        
        if (contentType.startsWith(WSFRMKeys.XML_CONTENT_TYPE)) {
          contentObject= new XMLContent(content);
        }
        else if (contentType.startsWith(WSFRMKeys.JSON_CONTENT_TYPE)) {
          contentObject= new JSONContent(content); 
        }
        else if (contentType.startsWith(WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE)) {
          contentObject = new TextContent(content);
        }
        else if (contentType.startsWith(WSFRMKeys.TEXT_HTML_CONTENT_TYPE)) {
          if(content.startsWith("{")) {
            contentObject = new JSONContent(content);
          } else {
            contentObject = new TextContent(content);
          }
        }
      }  
  }   
  
  //get the response content
  public String getContent(){
    return contentObject.getContent();
  }
  
  //get the response lenght
  public long getContentLength() {
    return contentLength;
  }
  
  //get the response content type
  public String getContentType() {
    return contentType;
  } 
  
  //get a handle to the document
  public Document getDocument() {
    return contentObject.getDocument();
  }
  
  //get the status code
  public int getStatusCode() {
    return statusCode;
  }
  
  //get the response phrase
  public String getResponseMsg() {
    return responseMsg;
  }
  
  //get the protocol version
  public String getProtocolVersion() {
    return protocolVersion;
  }
  
//get the status line
  public String getStatusLine() {
    return statusLine;
  }
  
  //method to get the value for a given header
  //if a header is not found with the name then it will return null
  public Header getHeader(String name) {
    Header hdr=null;
    HeaderIterator itr = httpResponse.headerIterator();
    while (itr.hasNext()) {
      hdr = itr.nextHeader();
      //if the header name is as expected return the header
      if(hdr.getName().equals(name)) {
        return hdr;
      }
    }
    return hdr;
  }
}
