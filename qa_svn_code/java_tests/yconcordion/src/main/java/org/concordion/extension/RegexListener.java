package org.concordion.extension;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.concordion.api.extension.ConcordionExtender;
import org.concordion.api.extension.ConcordionExtension;
import org.concordion.api.listener.AssertEqualsListener;
import org.concordion.internal.command.AssertEqualsCommand;
import org.concordion.internal.listener.AssertResultRenderer;
import org.concordion.internal.util.Announcer;


public class RegexListener implements ConcordionExtension {
	 
	public void addTo(ConcordionExtender concordionExtender) {
		RegexMatch cmp = new RegexMatch();
		AssertResultRenderer assertRenderer = new AssertResultRenderer();		
		
		AssertEqualsCommand command = new AssertEqualsCommand(cmp);
		command.addAssertEqualsListener(assertRenderer);
		concordionExtender.withCommand("http://www.yahoo.com/hadoop", "assertRegexEquals", command);		
		
		FileRegexMatch fileCmp = new FileRegexMatch();
		assertRenderer = new AssertResultRenderer();	
		
		command = new AssertEqualsCommand(fileCmp);
		command.addAssertEqualsListener(assertRenderer);
		concordionExtender.withCommand("http://www.yahoo.com/hadoop", "assertFileRegexEquals", command);
    }
	
	/*
	 * Comparator for regular expression matching
	 */
	private class RegexMatch implements Comparator{
		public int compare(Object result, Object expect) {
			Pattern expectedPattern = Pattern.compile((String)expect, Pattern.DOTALL);
			if (result instanceof String[]){
				 String[] a=(String[])result;
				 if (a.length==0) return 1;
				 
				 for(String res: (String[])result){
					 Matcher m = expectedPattern.matcher(res);
					 if (!m.matches()){
						 return 1;					 
					 }
				 }
				 return 0;				 
			 }
			 else {
				 return ((String)result).matches((String)expect) ? 0:1;
			 }
		}				
	}
	
	/*
	 * Comparator for regular expression matching where the expression is in a file
	 */
	private class FileRegexMatch implements Comparator{
		public int compare(Object result, Object expect){
			String expectedStr = "";
			String filename = ((String)expect).trim(); 
			try{				  
				  InputStream is = ClassLoader.getSystemResourceAsStream((String) filename);
				  DataInputStream in = new DataInputStream(is);
				  BufferedReader br = new BufferedReader(new InputStreamReader(in));
				  String strLine;
				  if ((strLine = br.readLine()) != null) {
					  expectedStr+=strLine;
				  }
				  while ((strLine = br.readLine()) != null)   {
					  expectedStr+=("\n"+strLine);					  
				  }				  				  
				  in.close();
			} catch (Exception e){
				e.printStackTrace();
				return 1;
			}
			
			Pattern expectedPattern = Pattern.compile(expectedStr, Pattern.DOTALL);
			if (result instanceof String[]){
				 String[] a=(String[])result;
				 if (a.length==0) return 1;
				 
				 for(String res: (String[])result){
					 Matcher m = expectedPattern.matcher(res);
					 if (!m.matches()){
						 return 1;					 
					 }
				 }
				 return 0;				 
			 }
			 else {
				 return ((String)result).matches((String)expect) ? 0:1;
			 }
		}				
	}
}
