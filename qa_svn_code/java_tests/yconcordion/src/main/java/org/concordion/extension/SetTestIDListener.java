package org.concordion.extension;

import org.concordion.api.AbstractCommand;
import org.concordion.api.CommandCall;
import org.concordion.api.CommandCallList;
import org.concordion.api.Element;
import org.concordion.api.Evaluator;
import org.concordion.api.ResultRecorder;
import org.concordion.api.extension.ConcordionExtender;
import org.concordion.api.extension.ConcordionExtension;
import org.concordion.api.listener.ExecuteEvent;
import org.concordion.internal.DocumentParser;
import org.concordion.internal.TestList;
import org.concordion.internal.command.ExecuteCommand;

public class SetTestIDListener implements ConcordionExtension {

	public void addTo(ConcordionExtender concordionExtender) {
		// TODO Auto-generated method stub
		SetTestID command = new SetTestID();
		concordionExtender.withCommand("http://www.yahoo.com/hadoop", "setTestID", command);				
	}
	
	private class SetTestID extends ExecuteCommand{
		@Override
		public void execute(CommandCall commandCall, Evaluator evaluator, ResultRecorder resultRecorder) {
			String testID=commandCall.getExpression();
			TestList testList = DocumentParser.getTestList();
			
	        if(testID != null){
	        	if (testList.isIncluded(testID) == false){
	        		CommandCallList callList = commandCall.getChildren();
	        		callList = null;
	        		return;
	        	}
	        }
			
	        System.out.println("*******************************************************");
			System.out.println("Running test with testID: "+testID);
			System.out.println("*******************************************************");	
			CommandCallList childCommands = commandCall.getChildren();
            
            childCommands.setUp(evaluator, resultRecorder);
            childCommands.execute(evaluator, resultRecorder);
            announceExecuteCompleted(commandCall.getElement());
            childCommands.verify(evaluator, resultRecorder);
		}				
	}
}
