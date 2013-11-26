package org.hw.wsfrm;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

public class XMLContent extends Content {
  
  public XMLContent(String content) throws ParserConfigurationException, SAXException, IOException 
  {
    //generate an xml doucment from the string
    setContent(content);
    createXMLDocument();
  }
}
