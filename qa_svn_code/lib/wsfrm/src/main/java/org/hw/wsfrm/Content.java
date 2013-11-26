package org.hw.wsfrm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public abstract class Content {

  //private vars
  private DocumentBuilderFactory dcf;
  private DocumentBuilder db;
  private InputStream ins;
  private String content;
  
  //vars available to the child classes
  protected Document xmlDoc;
  
  
  //method to return the content as a document
  public Document getDocument() {
    return xmlDoc;
  }
 
  protected void setDocument(Document doc) {
    this.xmlDoc = doc;
  }
  
  //method to get an xml document from the content string
  public void createXMLDocument() throws ParserConfigurationException, SAXException, IOException {
    //if data is null return
    if (null == content) {
      xmlDoc=null;
      return;
    }
    dcf = DocumentBuilderFactory.newInstance();
    dcf.setNamespaceAware(true); // never forget this!
    
    db = dcf.newDocumentBuilder();
    ins = new ByteArrayInputStream (content.getBytes());
    xmlDoc = db.parse(ins);
  }
  
  //method to set the content
  protected void setContent(String data) {
    this.content=data;
  }
  
  //get a handle to the content string
  public String getContent() {
    return content;
  }
}
