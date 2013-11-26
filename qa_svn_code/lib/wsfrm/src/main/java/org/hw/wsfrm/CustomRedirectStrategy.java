package org.hw.wsfrm;

import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.protocol.HttpContext;

public class CustomRedirectStrategy extends DefaultRedirectStrategy {
  
  @Override
  //return true regardless of the method
  public boolean isRedirected(
      final HttpRequest request,
      final HttpResponse response,
      final HttpContext context) throws ProtocolException {
    if (response == null) {
        throw new IllegalArgumentException("HTTP response may not be null");
    }
  
    int statusCode = response.getStatusLine().getStatusCode();
    Header locationHeader = response.getFirstHeader("location");
    switch (statusCode) {
    case HttpStatus.SC_MOVED_TEMPORARILY:
        return locationHeader != null;
    case HttpStatus.SC_MOVED_PERMANENTLY:
    case HttpStatus.SC_TEMPORARY_REDIRECT:
        return true;
    case HttpStatus.SC_SEE_OTHER:
        return true;
    default:
        return false;
    } //end of switch
  }
  
  @Override
  //get all methods
  public HttpUriRequest getRedirect(
      final HttpRequest request,
      final HttpResponse response,
      final HttpContext context) throws ProtocolException {
    URI uri = getLocationURI(request, response, context);
    String method = request.getRequestLine().getMethod();
    if (method.equalsIgnoreCase(HttpHead.METHOD_NAME)) {
        return new HttpHead(uri);
    } else if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
        return new HttpGet(uri);
    } else if (method.equalsIgnoreCase(HttpPut.METHOD_NAME)) {
      return new HttpPut(uri);
    } else if (method.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
      return new HttpPost(uri);
    } else if (method.equalsIgnoreCase(HttpDelete.METHOD_NAME)) {
      return new HttpDelete(uri);
    } else {
      return new HttpGet(uri);
  }
  }
}
