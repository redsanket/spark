package hadooptest.automation.utils.http;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import hadooptest.TestSession;

public class JSONUtil
{
	private String jsonString;
	private Object jsonObject;

    public String readFileToString(String pathToFile)
            throws Exception
            {
                return new String(Files.readAllBytes(Paths.get(pathToFile)));
            }

	public final void setContentFromFile(String pathToFile)
			throws Exception
			{
                setContent(readFileToString(pathToFile));
			}

	public final void setContent(String contentString)
			throws Exception
			{
				if (contentString == null) {
					TestSession.logger.error("Trying to set content 'null'");
					throw new IllegalArgumentException("Content cannot be null");
				}
				TestSession.logger.debug(new StringBuilder().append("JSON string set. Content=").append(contentString).toString());
				this.jsonString = contentString;
				map();
			}

	public Object getElement(String path)
	{
		Object tmpObject = this.jsonObject;
		StringBuilder currentToken = new StringBuilder();

		if ((path == null) || (path.trim().length() == 0)) {
			TestSession.logger.error("Path expression is null.");
			return null;
		}

		for (StringTokenizer stringTokenizer = new StringTokenizer(path, "/"); stringTokenizer.hasMoreTokens(); ) {
			String token = stringTokenizer.nextToken();
			currentToken.append("/");
			currentToken.append(token);
			TestSession.logger.debug(new StringBuilder().append("Current token: ").append(token).toString());

			if ((token.startsWith("[")) && (token.endsWith("]"))) {
				TestSession.logger.debug("token is for an array");
				String arrayIndex = token.substring(token.indexOf("[") + 1, token.indexOf("]"));
				if ((tmpObject instanceof ArrayList)) {
					TestSession.logger.debug(new StringBuilder().append("Array found at ").append(currentToken.toString()).toString());
					int index = -1;
					try {
						index = Integer.parseInt(arrayIndex);
						TestSession.logger.debug(new StringBuilder().append("array index(simple) in token is ").append(index).toString());
					} catch (NumberFormatException ex) {
						TestSession.logger.debug("Array Index is not a plain number");
					}

					if (index > -1) {
						ArrayList tmp = (ArrayList)tmpObject;
						TestSession.logger.debug(new StringBuilder().append("Array available size is ").append(tmp.size()).toString());
						if (index >= tmp.size()) {
							throw new ArrayIndexOutOfBoundsException(new StringBuilder().append("At token '").append(currentToken.toString()).append("' array size is ").append(tmp.size()).append(" and you are looking for index ").append(index).toString());
						}
						tmpObject = tmp.get(index);
					} else if (arrayIndex.equals(":last")) {
						ArrayList tmp = (ArrayList)tmpObject;
						TestSession.logger.debug(new StringBuilder().append("Array available size is ").append(tmp.size()).toString());
						TestSession.logger.debug(new StringBuilder().append("Element from last index [").append(tmp.size() - 1).append("] ").toString());
						tmpObject = tmp.get(tmp.size() - 1); } else {
							if (arrayIndex.equals(":size")) {
								ArrayList tmp = (ArrayList)tmpObject;
								TestSession.logger.debug(new StringBuilder().append("Array available size is ").append(tmp.size()).toString());
								return Integer.valueOf(tmp.size());
							}if (arrayIndex.equals(":all")) {
								ArrayList tmp = (ArrayList)tmpObject;
								return tmp;
							}
							TestSession.logger.warn(new StringBuilder().append("Looks like a wrong path expression ").append(path).toString());
						}
				}
				else {
					if (tmpObject == null) {
						TestSession.logger.warn(new StringBuilder().append("Element not found at ").append(currentToken.toString()).append(". Returning 'null'").toString());
						return null;
					}
					TestSession.logger.warn(new StringBuilder().append("Looking for Array, but found ").append(tmpObject.getClass().getName()).toString());
				}
			}
			else {
				TestSession.logger.debug("token is for an object");
				if ((tmpObject instanceof HashMap)) {
					TestSession.logger.debug(new StringBuilder().append("HashMap found at ").append(currentToken.toString()).toString());
					HashMap tmp = (HashMap)tmpObject;
					tmpObject = tmp.get(token);
				}

			}

			if (!stringTokenizer.hasMoreTokens())
			{
				TestSession.logger.info(new StringBuilder().append("Returning ").append(tmpObject.toString()).toString());
				return tmpObject;
			}
		}
		TestSession.logger.warn(new StringBuilder().append("Path '").append(path).append("' not found").toString());
		return null;
	}

	private void map() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		TestSession.logger.debug("Mapping the content string into a JSON object");
		try {
			this.jsonObject = mapper.readValue(this.jsonString, Object.class);
		} catch (JsonParseException ex) {
			throw new Exception(new StringBuilder().append("JsonParseException while mapping the json string. ").append(ex.toString()).toString());
		} catch (JsonMappingException ex) {
			throw new Exception(new StringBuilder().append("JsonMappingException while mapping the json string. ").append(ex.toString()).toString());
		} catch (IOException ex) {
			throw new Exception(new StringBuilder().append("IOException while mapping the json string. ").append(ex.toString()).toString());
		}
	}
}
