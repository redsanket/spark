package hadooptest.automation.utils.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.log4j.Logger;

public class Response {

	private JSONUtil jsonUtil;
	private String status;
	private String responseBodyAsString;
	private int statusCode;
	private JSONObject jsonObject;
	Logger logger = Logger.getLogger(Response.class);

	public Response(HttpMethod method) {
		this.statusCode = method.getStatusCode();
		this.status = method.getStatusLine().toString();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			br = new BufferedReader(new InputStreamReader(
					method.getResponseBodyAsStream()));
			String line = null;

			while ((line = br.readLine()) != null)
				sb.append(line);
		} catch (IOException ex) {
			logger.error(ex.toString());
		}
		this.responseBodyAsString = sb.toString();
		logger.info(this.responseBodyAsString);
		this.jsonUtil = new JSONUtil();
		try {
			this.jsonUtil.setContent(this.responseBodyAsString);
		} catch (Exception ex) {
			logger.error(ex.toString());
		}

		jsonObject = (JSONObject) JSONSerializer.toJSON(responseBodyAsString);
		logger.info("Response jsonString: " + responseBodyAsString);
	}

	public Response(HttpMethod method, boolean isJSON) {
		this.statusCode = method.getStatusCode();
		this.status = method.getStatusLine().toString();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			br = new BufferedReader(new InputStreamReader(
					method.getResponseBodyAsStream()));
			String line = null;

			while ((line = br.readLine()) != null){
				sb.append(line);
				sb.append("\n");
			}
		} catch (IOException ex) {
			logger.error(ex.toString());
		}
		this.responseBodyAsString = sb.toString();
	}

	public JSONObject getJsonObject() {
		return this.jsonObject;
	}

	public String getStatus() {
		return this.status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getResponseBodyAsString() {
		return this.responseBodyAsString;
	}

	public void setResponseBodyAsString(String responseBodyAsString) {
		this.responseBodyAsString = responseBodyAsString;
	}

	public Object getElementAtPath(String path) {
		return this.jsonUtil.getElement(path);
	}

	public int getStatusCode() {
		return this.statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.status);
		builder.append("\n");
		builder.append(this.responseBodyAsString);
		return builder.toString();
	}
}
