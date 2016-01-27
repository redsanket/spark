package hadooptest.cluster.gdm;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.StatusLine;
import org.apache.log4j.Logger;
import net.sf.json.JSONSerializer;
import net.sf.json.JSONObject;
import net.sf.json.JSONArray;

public class Response
{

    private JSONUtil jsonUtil;
    private String status;
    private String responseBodyAsString;
    private int statusCode;
    private JSONObject jsonObject;

    public Response(HttpMethod method)
    {
        this.statusCode = method.getStatusCode();
        this.status = method.getStatusLine().toString();
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream()));
            String line = null;

            while ((line = br.readLine()) != null)
                sb.append(line);
        }
        catch (IOException ex) {
            TestSession.logger.error(ex.toString());
        }
        this.responseBodyAsString = sb.toString();
        this.jsonUtil = new JSONUtil();
        try {
            this.jsonUtil.setContent(this.responseBodyAsString);
        } catch (Exception ex) {
            TestSession.logger.error(ex.toString());
        }

        // response may not be json
        try {
            jsonObject=(JSONObject) JSONSerializer.toJSON(responseBodyAsString);
        } catch (Exception e) {
            TestSession.logger.debug("Response was not JSON");
        }
        TestSession.logger.info("Response jsonString: " + responseBodyAsString);
    }

    public Response(HttpMethod method, boolean isJSON) {
        this.statusCode = method.getStatusCode();
        this.status = method.getStatusLine().toString();
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream()));
            String line = null;

            while ((line = br.readLine()) != null)
                sb.append(line);
        }
        catch (IOException ex) {
            TestSession.logger.error(ex.toString());
        }
        this.responseBodyAsString = sb.toString();
    }

    public JSONObject getJsonObject()
    {
        return this.jsonObject;
    }
    
    public String getStatus()
    {
        return this.status;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public String getResponseBodyAsString()
    {
        return this.responseBodyAsString;
    }

    public void setResponseBodyAsString(String responseBodyAsString)
    {
        this.responseBodyAsString = responseBodyAsString;
    }

    public Object getElementAtPath(String path) {
        return this.jsonUtil.getElement(path);
    }

    public int getStatusCode()
    {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode)
    {
        this.statusCode = statusCode;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(this.status);
        builder.append("\n");
        builder.append(this.responseBodyAsString);
        return builder.toString();
    }
}

