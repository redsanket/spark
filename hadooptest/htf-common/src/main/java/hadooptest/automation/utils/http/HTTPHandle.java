package hadooptest.automation.utils.http;

import hadooptest.TestSession;
import hadooptest.TestSessionCore;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.EasySSLProtocolSocketFactory;
import hadooptest.automation.utils.http.EasyX509TrustManager;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.log4j.Logger;
import org.junit.Assert;

import yjava.byauth.jaas.HttpClientBouncerAuth;

public class HTTPHandle {
	private HttpClient httpClient;
	public static String SSO_SERVER = "bouncer.by.corp.yahoo.com";
	public static int SSO_PORT = 443;
	public static String USER = "";
	public static String PASSWORD = "";
	public static Header YBYCookieHeader = null;
	public static String YBYCookie = null; 
	private String baseURL;
	
	Logger logger = Logger.getLogger(HTTPHandle.class);

	public HTTPHandle() {
                // The following configs are required to use self signed certs - this should NOT 
                // be used in production
                Protocol easyhttps = new Protocol("https", new EasySSLProtocolSocketFactory(), 443);
                Protocol.registerProtocol("https", easyhttps);

		this.httpClient = new HttpClient();

		this.baseURL = null;

		this.httpClient.getParams().setParameter("User-Agent",
				"Jakarta Commons-HttpClient/3.1");
		this.httpClient.getParams().setParameter(
				"http.protocol.content-charset", "ISO-8859-1");
	}

	public void logonToBouncer(String paramString1, String paramString2) {
		HttpClientBouncerAuth localHttpClientBouncerAuth = new HttpClientBouncerAuth();
		YBYCookie = null;
		try {
            YBYCookie = localHttpClientBouncerAuth.authenticate(
                TestSessionCore.conf.getProperty("BOUNCER_URL", HadooptestConstants.Location.Bouncer.SSO_SERVER), paramString1,
                paramString2.toCharArray());
		} catch (Exception localException) {
			logger.error(new StringBuilder()
				.append("SSO authentication failed. ")
				.append(localException.toString()).toString(), localException);
		}
		YBYCookieHeader = new Header("Cookie", YBYCookie);
		this.httpClient.getParams().setParameter("Cookie", YBYCookie);
		logger.info("SSO auth cookie set");
	}

	public String loginAndReturnCookie(String username) throws Exception{
		String cookie = null;

		String gdmConfigPath = Util.getResourceFullPath(
				"gdm/conf/config.xml");
		 	Configuration gdmConf = new XMLConfiguration(gdmConfigPath);
			String password = gdmConf.getString("auth.pp");
    		HttpClientBouncerAuth localHttpClientBouncerAuth = new HttpClientBouncerAuth();
    		try {
    			cookie = localHttpClientBouncerAuth.authenticate(
    					HadooptestConstants.Location.Bouncer.SSO_SERVER, username,
    					password.toCharArray());
    		} catch (Exception localException) {
    			logger.error(new StringBuilder()
    					.append("SSO authentication failed. ")
    					.append(localException.toString()).toString(), localException);
    		}    	
    		return cookie;
	}
	
	public HttpMethod makeGET(String schemaAndHost, String resource,
	        ArrayList<CustomNameValuePair> paramArrayList) {
	    String str = constructFinalURL(schemaAndHost, resource, paramArrayList);
	    str = new StringBuilder().append(str).toString();
	    GetMethod localGetMethod = null;

	    int returnVal = 0;

	    try {
	        localGetMethod = new GetMethod(str);
	    } catch (Exception localException1) {
	        logger.error(new StringBuilder().append("Bad URL. ")
	                .append(localException1.toString()).toString());
	    }
	    if (YBYCookieHeader != null) { 
	        logger.info("Attempting to use Bouncer authentication: " + YBYCookieHeader.toString());
	        localGetMethod.addRequestHeader(YBYCookieHeader);
	    }
	    logger.info(new StringBuilder().append("Making a GET to ").append(str)
	            .toString());
	    try {
	        this.httpClient.executeMethod(localGetMethod);
	    } catch (Exception localException2) {
	        logger.error(localException2);
	        logger.error(localException2.getMessage());
	        localException2.printStackTrace();
	    }
	    logger.info(localGetMethod.getStatusLine().toString());
	    try {
	    } catch (Exception localException3) {
	        logger.error(localException3.toString());
	    }
	    return localGetMethod;
	}

	protected final PostMethod makePOST(String paramString1,
			String paramString2, ArrayList<CustomNameValuePair> paramArrayList) {
		String str = new StringBuilder().append(this.baseURL)
				.append(paramString1).toString();

		PostMethod localPostMethod = new PostMethod(str);
		NameValuePair[] arrayOfNameValuePair = null;

		localPostMethod.getParams().setCookiePolicy("ignoreCookies");
		localPostMethod.addRequestHeader(YBYCookieHeader);
		localPostMethod = new PostMethod(str);

		if ((paramArrayList != null) && (paramArrayList.size() > 0)) {
			int i = paramArrayList.size();
			arrayOfNameValuePair = new NameValuePair[i];
			for (int j = 0; j < i; j++) {
				arrayOfNameValuePair[j] = new NameValuePair(
						((CustomNameValuePair) paramArrayList.get(j)).getName(),
						((CustomNameValuePair) paramArrayList.get(j))
								.getValue());
			}
		}
		localPostMethod.setRequestBody(arrayOfNameValuePair);
		localPostMethod.addRequestHeader(YBYCookieHeader);

		localPostMethod.setRequestHeader("Content-Type",
				"application/x-www-form-urlencoded");
		try {
			logger.debug(new StringBuilder()
					.append("Making HTTP POST request to ").append(str)
					.toString());
			this.httpClient.executeMethod(localPostMethod);
		} catch (Exception localException) {
			logger.error(localException.toString());
		}

		logger.debug(new StringBuilder().append("HTTP Response Code: ")
				.append(localPostMethod.getStatusCode()).toString());

		return localPostMethod;
	}

	protected final PostMethod makePOST(String paramString1,
			String paramString2, String paramString3) {
		String str = new StringBuilder().append(this.baseURL)
				.append(paramString1).toString();

		PostMethod localPostMethod = new PostMethod(str);

		localPostMethod.getParams().setCookiePolicy("ignoreCookies");
		localPostMethod.addRequestHeader(YBYCookieHeader);
		localPostMethod = new PostMethod(str);
		try {
			localPostMethod.setRequestEntity(new StringRequestEntity(
					paramString3, "text/plain", "UTF-8"));
		} catch (UnsupportedEncodingException localUnsupportedEncodingException) {
			logger.error("Encoding not right for POST body");
		}
		localPostMethod.addRequestHeader(YBYCookieHeader);
		localPostMethod.setRequestHeader("Content-Type", "application/xml");
		try {
			logger.debug(new StringBuilder()
					.append("Making HTTP POST request to ").append(str)
					.toString());
			this.httpClient.executeMethod(localPostMethod);
		} catch (Exception localException) {
			logger.error(localException.toString());
		}

		logger.debug(new StringBuilder().append("HTTP Response Code: ")
				.append(localPostMethod.getStatusCode()).toString());

		return localPostMethod;
	}

	private String constructFinalURL(String schemaAndHost, String resource,
			ArrayList<CustomNameValuePair> paramArrayList) {
		StringBuilder localStringBuilder = new StringBuilder();
		URL localURL = null;

		if ((paramArrayList != null) && (!paramArrayList.isEmpty())) {
			for (CustomNameValuePair localCustomNameValuePair : paramArrayList) {
				String str2 = localCustomNameValuePair.getName();
				String str3 = localCustomNameValuePair.getValue();
				logger.debug(new StringBuilder().append("Param: ").append(str2)
						.append("=").append(str3).toString());
				try {
					str3 = URLEncoder.encode(str3, "UTF-8");
				} catch (UnsupportedEncodingException localUnsupportedEncodingException) {
					logger.error(new StringBuilder()
							.append("Problem encoding query params. ")
							.append(localUnsupportedEncodingException
									.toString()).toString());
				}
				logger.debug(new StringBuilder().append("URLEncoded Param: ")
						.append(str2).append("=").append(str3).toString());
				String str4 = new StringBuilder().append(str2).append("=")
						.append(str3).toString();
				if (localStringBuilder.length() == 0) {
					localStringBuilder.append("?");
					localStringBuilder.append(str4);
				} else {
					localStringBuilder.append("&");
					localStringBuilder.append(str4);
				}
			}
		}

		logger.info(new StringBuilder()
				.append("Constructing final URL from 'base'> ")
				.append(schemaAndHost).append(" and 'resource'>")
				.append(resource).append(" and 'queryString'>")
				.append(localStringBuilder.toString()).toString());

		localStringBuilder.insert(0, new StringBuilder().append(schemaAndHost)
				.append(resource).toString());
		String str1 = localStringBuilder.toString();
		logger.info(str1);
		try {
			localURL = new URL(str1);
		} catch (MalformedURLException localMalformedURLException) {
			logger.error("Final URL is bad. ");
		}

		logger.debug(new StringBuilder().append("Final URL is ").append(str1)
				.toString());
		return str1;
	}
}
