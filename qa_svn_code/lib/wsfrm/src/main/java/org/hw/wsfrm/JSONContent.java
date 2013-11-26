package org.hw.wsfrm;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.xml.sax.SAXException;

public class JSONContent extends Content {

  private JSONObject jsonObject;
  private String xmlString;
  public JSONContent(String jsonString) throws ParserConfigurationException, SAXException, IOException, JSONException {
    
    if (jsonString.isEmpty()) {
      xmlString = null;
    } else {
      jsonObject = new JSONObject(jsonString);
      xmlString = XML.toString(jsonObject);
    }
    setContent(xmlString);
    //set the xml document
    createXMLDocument();
  }
  
}
