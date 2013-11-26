package org.hw.wsfrm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import static org.junit.Assert.*;

public class XPathUtils {
  
  //Method to check the xpath
  public static void checkXpathTextContent(String xpath, String[] expValues, Document doc) {
    
    //message prefix for all failures
    String msgPrefix="For xpath('"+xpath+"')";
    //get all the nodes
    NodeList nodes=parseXpath(xpath, doc);
    
    //check if the nodes object is not null
    assertNotNull(msgPrefix+" no nodes returned", nodes);
  
    //check the number of nodes returned and how many exp values sent
    int actNodes=nodes.getLength();
    int expNodes=expValues.length;
    assertEquals(msgPrefix+" # of Nodes",expNodes,actNodes);
    
    //run through the nodes and check the value
    for (int i=0 ; i < actNodes ; i++) {
      String actVal=nodes.item(i).getTextContent();
      String expVal=expValues[i];
      //check if they are the same
      assertEquals(msgPrefix+" value",expVal,actVal);
    }
  }
  
  //Method to check the xpath
  public static void checkNumOfNodesReturned(String xpath, int expNodes, Document doc) {
    
    //message prefix for all failures
    String msgPrefix="For xpath('"+xpath+"')";
    //get all the nodes
    NodeList nodes=parseXpath(xpath, doc);
    
    //check if the nodes object is not null
    assertNotNull(msgPrefix+" no nodes returned", nodes);
  
    //check the number of nodes returned and how many exp values sent
    int actNodes=nodes.getLength();
    //check if they are the same
    assertEquals(msgPrefix+" # of nodes returned",expNodes,actNodes);
  }
  
  
  //Method to check the xpath, with a regex pattern
  public static void checkXpathTextContentWithRegex(String xpath, String[] expRegex, Document doc) {
    
    Pattern pattern;
    Matcher match;
    Boolean matches;
    
    //message prefix for all failures
    String msgPrefix="For xpath('"+xpath+"')";
    //get all the nodes
    NodeList nodes=parseXpath(xpath, doc);
    
    //check if the nodes object is not null
    assertNotNull(msgPrefix+" no nodes returned", nodes);
  
    //check the number of nodes returned and how many exp values sent
    int actNodes=nodes.getLength();
    int expNodes=expRegex.length;
    assertEquals(msgPrefix+" # of Nodes",expNodes,actNodes);
    
    //run through the nodes and check the value
    for (int i=0 ; i < actNodes ; i++) {
      String actVal=nodes.item(i).getTextContent();
      //generate the patter and see if the pattern matches
      pattern = Pattern.compile(expRegex[i],Pattern.DOTALL);
      match = pattern.matcher(actVal);
      matches=match.matches();
      assertTrue(msgPrefix+" actual value ('"+actVal+"') does not match the regex ('"+expRegex[i]+"')",matches);
    }
  }
  
  //check a single xpath
  public static void checkXpathTextContent(String xpath, String expValue, Document doc) {
    String[] expValues = {expValue};
    checkXpathTextContent(xpath, expValues, doc);
  }
  
  //Method to check the xpath, with a regex pattern
  public static void checkXpathTextContentWithRegex(String xpath, String expRegex, Document doc) {
    String[] regexes = {expRegex};
    //Method to check the xpath, with a regex pattern
    checkXpathTextContentWithRegex(xpath, regexes, doc);
    
  }
  
  //return a node list which match the xpath
  public static NodeList parseXpath(String xpath, Document doc) {
    //if doc is null return null
    if (null == doc) {
      return null;
    }
    
    NodeList nodes=null;
    XPathFactory factory = XPathFactory.newInstance();
    XPath xp = factory.newXPath();
    try {
      XPathExpression expr = xp.compile(xpath);
      Object result = expr.evaluate(doc, XPathConstants.NODESET);
      nodes = (NodeList) result;
      return nodes;
    } catch (XPathExpressionException xpee) {
      // TODO Auto-generated catch block
      xpee.printStackTrace();
    }
    return nodes;
  }
}
