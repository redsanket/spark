package hadooptest.hadoop.stress.ClusterUsage;

import java.io.IOException;

public class ConvertUnit {

	public final static long kilo = 1024;
	public final static long mega = kilo*1024;
	public final static long giga = mega*1024;
	public final static long tera = giga*1024;
	
	public static void main(String [] args) throws IOException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, InterruptedException {
		float v = (float) 1.2345;
		System.out.println(convertUnit(v));
	}
	
	static public String convertUnit(float input){
		if(input > tera)
			return String.format("%.2f", input/(float)tera) + 'T';
		else if (input > giga)
			return String.format("%.2f", input/(float)giga) + 'G';
		else if (input > mega)
			return String.format("%.2f", input/(float)mega) + 'M';
		else if (input > kilo)
			return String.format("%.2f", input/(float)kilo) + 'K';
		return String.format("%.2f", input);
	}
}
