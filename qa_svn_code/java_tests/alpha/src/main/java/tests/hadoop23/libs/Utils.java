package tests.hadoop23.libs;

import java.util.List;

public class Utils {
	
	public static String listToString(List<String> str){
		String ret="";		
		for(String e: str){
			ret+=e+" ";
		}
		return ret;
	}
	
	public static String arrayToString(String[] str){
		String ret="";		
		for(String e: str){
			ret+=e+" ";
		}
		return ret;
	}
}
