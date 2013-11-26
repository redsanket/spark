package org.hw.wsfrm;

public interface WSFRMKeys {
  public static final String HTTP_GET="GET";
  public static final String HTTP_PUT="PUT";
  public static final String HTTP_POST="POST";
  public static final String HTTP_DELETE="DELETE";
  
  public static final String XML_CONTENT_TYPE="application/xml";
  public static final String JSON_CONTENT_TYPE="application/json";
  public static final String TEXT_HTML_CONTENT_TYPE="text/html; charset=utf-8";
  public static final String TEXT_PLAIN_CONTENT_TYPE="text/plain; charset=utf-8";
  public static final String BINARY_OCTET_STREAM_CONTENT_TYPE = "binary/octet-stream";
  public static final String APP_OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
  
  public static final String HTTP_PROTOCOL_VERSION="HTTP/1.1";
  public static final String HTTP_OK_MSG="OK";
  public static final String HTTP_CREATED_MSG="Created";
  public static final String HTTP_PARTIAL_CONTENT_MSG="Partial Content";
  
  public static final String HTTP_BAD_REQUEST="Bad Request";
  public static final String HTTP_NOT_FOUND="Not Found";
  public static final String HTTP_FORBIDDEN="Forbidden";
  public static final String HTTP_UNAUTHORIZED="Unauthorized";
  public static final String HTTP_INTERNAL_SERVER_ERROR="Internal Server Error";
  public static final String UTF_8_ENCODING="UTF-8";
}