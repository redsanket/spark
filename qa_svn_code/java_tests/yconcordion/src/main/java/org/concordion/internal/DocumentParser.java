package org.concordion.internal;

import java.io.FileNotFoundException;
import java.io.IOException;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Elements;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.concordion.api.Command;
import org.concordion.api.CommandCall;
import org.concordion.api.CommandFactory;
import org.concordion.api.Element;
import org.concordion.api.Resource;
import org.concordion.api.Specification;
import org.concordion.api.listener.DocumentParsingListener;
import org.concordion.internal.util.Announcer;
import org.concordion.internal.util.Check;

public class DocumentParser {

    private final CommandFactory commandFactory;
    private final Announcer<DocumentParsingListener> listeners = Announcer.to(DocumentParsingListener.class);
    private static TestList testList;
    private Log LOG = LogFactory.getLog(DocumentParser.class);
    
    public DocumentParser(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    public void addDocumentParsingListener(DocumentParsingListener listener) {
        listeners.addListener(listener);
    }

    public void removeDocumentParsingListener(DocumentParsingListener listener) {
        listeners.removeListener(listener);
    }
    
    private void announceBeforeParsing(Document document) {
        listeners.announce().beforeParsing(document);
    }

    public Specification parse(Document document, Resource resource) throws IOException {
        announceBeforeParsing(document);
        nu.xom.Element xomElement = document.getRootElement();
        CommandCall rootCommandCall = new CommandCall(createSpecificationCommand(), new Element(xomElement), "", resource);
        String testListFile = System.getProperty("testList");
        if (testListFile != null){
    		LOG.info("Got test list file: "+testListFile);
        	testList = new TestList(testListFile);
    	}
        generateCommandCallTree(xomElement, rootCommandCall, resource);             
        return new XMLSpecification(rootCommandCall);
    }
    
    static public TestList getTestList(){
    	return testList;
    }
    
    private void generateCommandCallTree(nu.xom.Element xomElement, CommandCall parentCommandCall, Resource resource) throws IOException {
        boolean commandIsAssigned = false;
                                      
        for (int i = 0; i < xomElement.getAttributeCount(); i++) {
            Attribute attribute = xomElement.getAttribute(i);
            String namespaceURI = attribute.getNamespaceURI();
            
            if (!namespaceURI.equals("")) {
                String commandName = attribute.getLocalName();
                
                Command command = createCommand(namespaceURI, commandName);
                if (command != null) {
                    Check.isFalse(commandIsAssigned, "Multiple commands per element is currently not supported.");
                    commandIsAssigned = true;
                    String expression = attribute.getValue();
                    CommandCall commandCall = new CommandCall(command, new Element(xomElement), expression, resource);
                    parentCommandCall.appendChild(commandCall);
                    parentCommandCall = commandCall;
                }
            }
        }
        
        Elements children = xomElement.getChildElements();
        for (int i = 0; i < children.size(); i++) {
            generateCommandCallTree(children.get(i), parentCommandCall, resource);
        }
    }

    private Command createSpecificationCommand() {
        Command specCmd = createCommand("", "specification");
        return specCmd;
    }

    private Command createCommand(String namespaceURI, String commandName) {
        return commandFactory.createCommand(namespaceURI, commandName);
    }
}
