package Cluster;

import java.io.*;
import java.util.*;

public class RCombine {
	public static List<String> inpathList=new ArrayList<String>();
	public static List<String> lines=new ArrayList<String>();
	public static String outpath="/project/data/cluster/part-r-00000";
	public static void main(String args[]) throws Exception{
		loadInpath();
		
		
		for(int i=0;i<inpathList.size();i++){
			BufferedReader reader=new BufferedReader(new FileReader(inpathList.get(i)));
			String line="";
			while((line=reader.readLine())!=null){
				lines.add(line);
			}
			reader.close();
		}
		lines.sort(new Comparator<String>(){

			public int compare(String o1, String o2) {
				Long u1,u2;
				u1=Long.parseLong(o1.substring(0,o1.indexOf('\t')));
				u2=Long.parseLong(o2.substring(0,o2.indexOf('\t')));
				if(u1>u2)
					return 1;
				else if(u1<u2)
					return -1;
				else
					return 0;
			}
			
		});
		
		BufferedWriter writer=new BufferedWriter(new FileWriter(outpath));
		for(int n=0;n<lines.size();n++){
			writer.write(lines.get(n));
			writer.newLine();
		}
		writer.close();
	}
	
	public static void loadInpath(){
		//inpathList.add("/project/data/cluster/part-r-00001");
		inpathList.add("/project/data/cluster/part-r-00002");
		//inpathList.add("/project/data/cluster/part-r-00003");
		//inpathList.add("/project/data/cluster/part-r-00004");
		inpathList.add("/project/data/cluster/part-r-00005");
	}
}
