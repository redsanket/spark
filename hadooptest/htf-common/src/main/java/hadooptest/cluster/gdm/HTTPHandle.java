// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import hadooptest.Util;
import yjava.byauth.jaas.HttpClientBouncerAuth;
import hadooptest.TestSession;

public class HTTPHandle
{
    private HttpClient httpClient;
    public static String SSO_SERVER = "bouncer.by.corp.yahoo.com";
    public static int SSO_PORT = 443;
    public static String userName = "";
    public static String passWord = "";
    public static Header YBYCookieHeader = null;
    private String baseURL;
    private Configuration conf;
    public String cookie;

    public HTTPHandle() {
        init();
        try {
            this.userName = this.conf.getString("auth.usr");
            this.passWord = Util.getTestUserPasswordFromYkeykey(this.userName);
            if (this.passWord != null) {
        	this.logonToBouncer(this.userName,this.passWord);
            }
        }catch (Exception e) {
            System.out.println("exception while reading userName or Password " + e.getStackTrace());
        }   
    }
    
    public HTTPHandle(String userName, String passWord) {
        init();
        this.logonToBouncer(userName , passWord);   
    } 
    
    
    public void init(){
        this.httpClient = new HttpClient();
        try {
            String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
            this.conf = new XMLConfiguration(configPath);
            SSO_SERVER = this.conf.getString("sso_server.resource", "https://gh.bouncer.login.yahoo.com/login/");
            String environmentType = this.conf.getString("hostconfig.console.test_environment_type");
            if (environmentType.equals("oneNode")) {
                TestSession.logger.info("****** QE or Dev test Environment ******** ");
                this.baseURL = this.conf.getString("hostconfig.console.base_url");
            } else if (environmentType.equals("staging")) {
                TestSession.logger.debug("****** Staging test Environment ******** ");
                this.baseURL = this.conf.getString("hostconfig.console.staging_console_url");
            } else  {
                TestSession.logger.info("****** Specified invalid test environment ******** ");
                System.exit(1);
            }
            TestSession.logger.debug(new StringBuilder().append("Console Base URL: ").append(this.baseURL).toString());
         
        } catch (ConfigurationException localConfigurationException) {
            TestSession.logger.error(localConfigurationException.toString());
        }
        this.httpClient.getParams().setParameter("User-Agent", "Jakarta Commons-HttpClient/3.1");
        this.httpClient.getParams().setParameter("http.protocol.content-charset", "ISO-8859-1");
    }
    
    /**
     * Return the bouncer cookie
     * @return - cookie as String
     */
    public String getBouncerCookie() {
        return cookie;
    }
    
    /**
     * Invoked when users want to set the base url explicitly. example in case of cross colo where 
     * the user want to switch between the current console url to the cross colo url.
     * @param currentConsoleURL  - current console url
     */
    public void setBaseURL(String currentConsoleURL) {
        this.baseURL = currentConsoleURL;
        TestSession.logger.info("current baseURL = " + this.baseURL);
    }
    
    /*
     * Get base url of the current console
     */
    public String getBaseURL() {
        return this.baseURL;
    }
    
    public void logonToBouncer(String paramString1, String paramString2)
    {
        TestSession.logger.info("*****************  user name = " + paramString1);
        
        HttpClientBouncerAuth localHttpClientBouncerAuth = new HttpClientBouncerAuth();
        String str = null;
        try {
            str = localHttpClientBouncerAuth.authenticate("https://gh.bouncer.login.yahoo.com/login/", paramString1, paramString2.toCharArray());
        } catch (Exception localException) {
            TestSession.logger.error(new StringBuilder().append("SSO authentication failed. ").append(localException.toString()).toString());
        }

        cookie = str; 
        YBYCookieHeader = new Header("Cookie", str);
        //this.httpClient.getParams().setParameter("Cookie", str);
    }

    public HttpMethod makeGET(String paramString1, String paramString2, ArrayList<CustomNameValuePair> paramArrayList) {
        String str = constructFinalURL(paramString1, paramString2, paramArrayList);
        GetMethod localGetMethod = null;
        try {
            localGetMethod = new GetMethod(str);
        } catch (Exception localException1) {
            TestSession.logger.error(new StringBuilder().append("Bad URL. ").append(localException1.toString()).toString());
        }

        localGetMethod.addRequestHeader(YBYCookieHeader);
        TestSession.logger.info(new StringBuilder().append("Making a GET to ").append(str).toString());
        try {
            this.httpClient.executeMethod(localGetMethod);
            TestSession.logger.debug(localGetMethod.getResponseBodyAsString());
        } catch (Exception localException2) {
            TestSession.logger.error(localException2);
        }
        try {
            TestSession.logger.debug(localGetMethod.getResponseBodyAsString());
        } catch (Exception localException3) {
            TestSession.logger.error(localException3.toString());
        }
        return localGetMethod;
    }

    protected final PostMethod makePOST(String paramString1, ArrayList<CustomNameValuePair> paramArrayList)
    {
        String str = new StringBuilder().append(this.baseURL).append(paramString1).toString();

        PostMethod localPostMethod = new PostMethod(str);
        NameValuePair[] arrayOfNameValuePair = null;

        localPostMethod.getParams().setCookiePolicy("ignoreCookies");
        localPostMethod.addRequestHeader(YBYCookieHeader);
        localPostMethod = new PostMethod(str);

        if ((paramArrayList != null) && (paramArrayList.size() > 0)) {
            int i = paramArrayList.size();
            arrayOfNameValuePair = new NameValuePair[i];
            for (int j = 0; j < i; j++) {
                arrayOfNameValuePair[j] = new NameValuePair(((CustomNameValuePair)paramArrayList.get(j)).getName(), ((CustomNameValuePair)paramArrayList.get(j)).getValue());
            }
        }
        localPostMethod.setRequestBody(arrayOfNameValuePair);
        localPostMethod.addRequestHeader(YBYCookieHeader);

        localPostMethod.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
        try {
            TestSession.logger.debug(new StringBuilder().append("Making HTTP POST request to ").append(str).toString());
            this.httpClient.executeMethod(localPostMethod);
        } catch (Exception localException) {
            TestSession.logger.error(localException.toString());
        }

        TestSession.logger.debug(new StringBuilder().append("HTTP Response Code: ").append(localPostMethod.getStatusCode()).toString());

        return localPostMethod;
    }

    protected final PostMethod makePOST(String paramString1, String paramString3) {
        String str = new StringBuilder().append(this.baseURL).append(paramString1).toString();

        PostMethod localPostMethod = new PostMethod(str);

        localPostMethod.getParams().setCookiePolicy("ignoreCookies");
        localPostMethod.addRequestHeader(YBYCookieHeader);
        localPostMethod = new PostMethod(str);
        try {
            localPostMethod.setRequestEntity(new StringRequestEntity(paramString3, "text/plain", "UTF-8"));
        } catch (UnsupportedEncodingException localUnsupportedEncodingException) {
            TestSession.logger.error("Encoding not right for POST body");
        }
        localPostMethod.addRequestHeader(YBYCookieHeader);
        localPostMethod.setRequestHeader("Content-Type", "application/xml");
        try
        {
            TestSession.logger.debug(new StringBuilder().append("Making HTTP POST request to ").append(str).toString());
            this.httpClient.executeMethod(localPostMethod);
        } catch (Exception localException) {
            TestSession.logger.error(localException.toString());
        }

        TestSession.logger.debug(new StringBuilder().append("HTTP Response Code: ").append(localPostMethod.getStatusCode()).toString());

        return localPostMethod;
    }

    private String constructFinalURL(String paramString1, String paramString2, ArrayList<CustomNameValuePair> paramArrayList)
    {
        StringBuilder localStringBuilder = new StringBuilder();
        URL localURL = null;

        if ((paramArrayList != null) && (!paramArrayList.isEmpty())) {
            for (CustomNameValuePair localCustomNameValuePair : paramArrayList)
            {
                String str2 = localCustomNameValuePair.getName();
                String str3 = localCustomNameValuePair.getValue();
                TestSession.logger.debug(new StringBuilder().append("Param: ").append(str2).append("=").append(str3).toString());
                try {
                    str3 = URLEncoder.encode(str3, "UTF-8");
                } catch (UnsupportedEncodingException localUnsupportedEncodingException) {
                    TestSession.logger.error(new StringBuilder().append("Problem encoding query params. ").append(localUnsupportedEncodingException.toString()).toString());
                }
                TestSession.logger.debug(new StringBuilder().append("URLEncoded Param: ").append(str2).append("=").append(str3).toString());
                String str4 = new StringBuilder().append(str2).append("=").append(str3).toString();
                if (localStringBuilder.length() == 0) {
                    localStringBuilder.append("?");
                    localStringBuilder.append(str4);
                } else {
                    localStringBuilder.append("&");
                    localStringBuilder.append(str4);
                }
            }
        }

        TestSession.logger.debug(new StringBuilder().append("Constructing final URL from 'base'> ").append(paramString1).append(" and 'resource'>").append(paramString2).append(" and 'queryString'>").append(localStringBuilder.toString()).toString());

        localStringBuilder.insert(0, new StringBuilder().append(paramString1).append(paramString2).toString());
        String str1 = localStringBuilder.toString();
        try
        {
            localURL = new URL(str1);
        } catch (MalformedURLException localMalformedURLException) {
            TestSession.logger.error("Final URL is bad. Check the conf/config.xml");
        }

        TestSession.logger.debug(new StringBuilder().append("Final URL is ").append(str1).toString());
        return str1;
    }
}

