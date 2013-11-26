package org.concordion.internal.listener;

import java.util.Arrays;

import org.concordion.api.Element;
import org.concordion.api.listener.AssertEqualsListener;
import org.concordion.api.listener.AssertFailureEvent;
import org.concordion.api.listener.AssertFalseListener;
import org.concordion.api.listener.AssertSuccessEvent;
import org.concordion.api.listener.AssertTrueListener;

public class AssertResultRenderer implements AssertEqualsListener, AssertTrueListener, AssertFalseListener {

//    public void failureReported(AssertFailureEvent event) {
//        Element element = event.getElement();
//        element.addStyleClass("failure");
//        
//        Element spanExpected = new Element("del");
//        spanExpected.addStyleClass("expected");
//        element.moveChildrenTo(spanExpected);
//        element.appendChild(spanExpected);
//        spanExpected.appendNonBreakingSpaceIfBlank();
//        
//        Element spanActual = new Element("ins");
//        spanActual.addStyleClass("actual");
//        spanActual.appendText(convertToString(event.getActual()));
//        spanActual.appendNonBreakingSpaceIfBlank();
//        
//        element.appendText("\n");
//        element.appendChild(spanActual);        
//    }
	
//	public void successReported(AssertSuccessEvent event) {
//        event.getElement()
//            .addStyleClass("success")
//            .appendNonBreakingSpaceIfBlank();
//    }
	
    public void failureReported(AssertFailureEvent event) {
        Element element = event.getElement();
        element.addStyleClass("failure");
        
        String text = element.getText();
               
        Element spanRemove  = new Element("span");
        element.moveChildrenTo(spanRemove);
        
        Element spanExpected = new Element("del"); 
        spanExpected = new Element("del");
        spanExpected.addStyleClass("expected");
        replaceNewLineByBreak(spanExpected,text);
        spanExpected.appendNonBreakingSpaceIfBlank();
        element.appendChild(spanExpected);
        
        
        
        Element spanActual = new Element("ins");
        spanActual.addStyleClass("actual");
        text = convertToString(event.getActual());
        replaceNewLineByBreak(spanActual,text);
        spanActual.appendNonBreakingSpaceIfBlank();
        element.appendText("\n");
        element.appendChild(spanActual);        
    }
    
    private void replaceNewLineByBreak(Element elem, String text){
    	String[] textList=text.split("\n");
        Element brk;
        for (String t: textList){
        	Element spanText = new Element("span");
        	spanText.appendText(t);
        	elem.appendChild(spanText);
        	brk = new Element("br");
        	elem.appendChild(brk);
        }
    }
    public void successReported(AssertSuccessEvent event) {
    	Element element = event.getElement();
        
        element.addStyleClass("success");
        
        String text = element.getText();
               
        Element spanRemove  = new Element("span");
        element.moveChildrenTo(spanRemove);
        
        Element spanExpected = new Element("span"); 
        replaceNewLineByBreak(spanExpected,text);
        spanExpected.appendNonBreakingSpaceIfBlank();
        element.appendChild(spanExpected);
    }
    
    private String convertToString(Object object) {
        if (object == null) {
            return "(null)";
        }
        else if (object instanceof String[]){
        	String text="";
        	String[] result = (String[])object;
        	for (int i=0; i<result.length; i++){
        		text += "++++ Actual output "+i+":\n"+result[i];
        	}
        	return text;
        }
        return "++++ Actual output:\n" + object;
    }
}
