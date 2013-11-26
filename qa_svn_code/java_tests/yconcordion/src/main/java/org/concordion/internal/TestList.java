package org.concordion.internal;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TestList {
	private ArrayList<String> testList = new ArrayList<String>();
	private Log LOG = LogFactory.getLog(TestList.class);
	
	public TestList(nu.xom.Element xomElement) {
		
	}
	
	public TestList(String path) throws IOException{
		try{
			BufferedReader in = new BufferedReader(new FileReader(path));
			String str;
			while ((str = in.readLine()) != null){
				LOG.info("Including test "+str);
				testList.add(str);
			}
			in.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public boolean isIncluded(String testID){
		for(String str: testList){			
			if(str.equals("*") || str.equals(testID)){
				return true;			
			}
		}
		return false;
	}
}
