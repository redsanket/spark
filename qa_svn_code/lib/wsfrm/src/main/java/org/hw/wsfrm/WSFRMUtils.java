package org.hw.wsfrm;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class WSFRMUtils {
  private static final Logger LOG = Logger.getLogger(WSFRMUtils.class);
  
  //method to check the status code using a response object
  public static void checkStatusCode(int exp, int act) {
    assertEquals("Status Code",exp, act);
  }
  
  //method to check the response message
  public static void checkResponseMsg(String exp, String act) {
    assertEquals("Response Message",exp, act);
  }
  
  //method to check the protocol version
  public static void checkProtocolVersion(String exp, String act) {
   assertEquals("Protocol Version",exp, act);
  }

  //method to chech the content type
  public static void checkContentType(String exp, String act) {
    assertEquals("Content-Type",exp, act);
  }
  
  //method to check for a 200 response code
  //method will check for status code of 200, message of OK and protocl version of HTTP/1.1
  public static void checkHttp200(Response rsp) {
    checkStatusCode(200,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_OK_MSG,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
  }
  
  //method to check for a 200 response code
  //method will check for status code of 200, message of OK and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp200(Response rsp, String expContentType) {
    checkStatusCode(200,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_OK_MSG,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 201 response code
  //method will check for status code of 201, message of OK and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp201(Response rsp, String expContentType) {
    checkStatusCode(201,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_CREATED_MSG,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 206 response code
  //method will check for status code of 201, message of OK and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp206(Response rsp, String expContentType) {
    checkStatusCode(206,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_PARTIAL_CONTENT_MSG,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 400 response code
  //method will check for status code of 400, message of Bad Request and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp400(Response rsp, String expContentType) {
    checkStatusCode(400,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_BAD_REQUEST,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 401 response code
  //method will check for status code of 401, message of Bad Request and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp401(Response rsp, String expContentType) {
    checkStatusCode(401,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_UNAUTHORIZED,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 404 response code
  //method will check for status code of 404, message of Bad Request and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp404(Response rsp, String expContentType) {
    checkStatusCode(404,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_NOT_FOUND,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 403 response code
  //method will check for status code of 403, message of Forbidden and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp403(Response rsp, String expContentType) {
    checkStatusCode(403,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_FORBIDDEN,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check for a 500 response code
  //method will check for status code of 500, message of Forbidden and protocl version of HTTP/1.1
  // Also check the content type that is returned.
  public static void checkHttp500(Response rsp, String expContentType) {
    checkStatusCode(500,rsp.getStatusCode());
    checkResponseMsg(WSFRMKeys.HTTP_INTERNAL_SERVER_ERROR,rsp.getResponseMsg());
    checkProtocolVersion(WSFRMKeys.HTTP_PROTOCOL_VERSION,rsp.getProtocolVersion());
    checkContentType(expContentType,rsp.getContentType());
  }
  
  //method to check if the content type is xml
  public static void checkXmlContenType(String exp) {
    checkContentType(exp,WSFRMKeys.XML_CONTENT_TYPE);
  }
  
//method to check if the content type is json
  public static void checkJsonContenType(String exp) {
    checkContentType(exp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //method to get a handle to a properties
  public static Properties loadProperties(String file) {
    Properties properties=null;
    //create a reader object from a file
    try {
      FileReader fr = new FileReader (file);
      properties = new Properties();
      properties.load(fr);
    } catch (FileNotFoundException e) {
      LOG.error(e.getMessage(), e.fillInStackTrace());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e.fillInStackTrace());
    }
    
    return properties; 
  }  
  
  
  //method to compare content of a file with a string
  //will consider the file as the expected data
  // and string as the actual data
  public static void compareFileToString (String file, String actData) {
    try {
      FileInputStream fin = new FileInputStream(file);
      String expData = IOUtils.toString(fin,WSFRMKeys.UTF_8_ENCODING);
      //compare the two strings
      assertEquals("Check content of file('"+file+"') with actual data",expData, actData);
      
    } catch (FileNotFoundException e) {
      LOG.error(e.getMessage(),e.fillInStackTrace());
      fail(e.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage(),e.fillInStackTrace());
      fail(e.getMessage());
    }
  }
  
  //method to compare an input stream
  //will consider the file as the expected data
  // and string as the actual data
  public static void compareByteArrayToString (byte[] buffer, String actData) {
      String expData = new String (buffer);
      //compare the two strings
      assertEquals("Check content of byte array data",expData, actData);
  }
}
