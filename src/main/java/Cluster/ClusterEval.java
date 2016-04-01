package Cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;

public class ClusterEval {

	public static String ctPath="/project/data/cluster/cluster.txt";
	public static String resPath="/project/data/cluster/point.txt";
	public static String evalPath="/project/data/cluster/ClusterEval";
	static ArrayList<double []> centers=new ArrayList<double []> ();
	static ArrayList diffs=new ArrayList();

	
	public static void main(String args[]) throws Exception{
		BufferedReader cin=new BufferedReader(new FileReader(ctPath));
		BufferedReader rin=new BufferedReader(new FileReader(resPath));
		BufferedWriter out1=new BufferedWriter(new FileWriter(evalPath));
		String line="";
		while((line=cin.readLine())!=null){
			double[] cpoints=new double[13];
			int start,end;
			for(int i=0;i<12;i++){
				start=line.indexOf(':');
				end=line.indexOf(',');
				cpoints[i]=Double.parseDouble(line.substring(start+1, end));
				line=line.substring(end+1);
			}
			start=line.indexOf(':');
			end=line.indexOf(']');
			cpoints[12]=Double.parseDouble(line.substring(start+1, end));
			centers.add(cpoints);
		}
		cin.close();
		
		//////////////////////////////////////////////////////////////////////////////		
		double[] dist={0,0,0,0,0,0,0,0,0,0,0,0,0};
		double diffs=0;
		double []totalDiff={0,0,0,0,0};
		int [] size={0,0,0,0,0};
		
		while((line=rin.readLine())!=null){
			int key=0;
			int start,stop=0,end=0;
			int d;
			double val;
			//System.out.println(line);
			
			line=line.substring(line.indexOf(':')+2);
			key=Integer.parseInt(line.substring(0,line.indexOf(':')));
			switch(key){
				case 39:
					key=0;
					break;
				case 235:
					key=1;
					break;
				case 1139:
					key=2;
					break;
				case 1029:
					key=3;
					break;
				case 1084:
					key=4;
					break;
			}
			line=line.substring(line.indexOf('[')+1,line.indexOf(']'));
			
			start=line.indexOf(':');
			stop=line.indexOf(',');
			end=line.lastIndexOf(':');
			boolean lastOne=false;
			while(start<=end){
				
				if(start<end){
					stop=line.indexOf(',');
				}
				else if(start==end){
					stop=line.length();
					lastOne=true;
				}
				
				d=Integer.parseInt(line.substring(0,start));
				val=Double.parseDouble(line.substring(start+1,stop));
				switch(d){
					case 0:
						dist[0]=val-centers.get(key)[0];
						break;
					case 1:
						dist[1]=val-centers.get(key)[1];
						break;
					case 2:
						dist[2]=val-centers.get(key)[2];
						break;
					case 4:
						dist[3]=val-centers.get(key)[3];
						break;
					case 5:
						dist[4]=val-centers.get(key)[4];
						break;
					case 6:
						dist[5]=val-centers.get(key)[5];
						break;
					case 7:
						dist[6]=val-centers.get(key)[6];
						break;
					case 8:
						dist[7]=val-centers.get(key)[7];
						break;
					case 9:
						dist[8]=val-centers.get(key)[8];
						break;
					case 10:
						dist[9]=val-centers.get(key)[9];
						break;
					case 11:
						dist[10]=val-centers.get(key)[10];
						break;
					case 12:
						dist[11]=val-centers.get(key)[11];
						break;
					case 13:
						dist[12]=val-centers.get(key)[12];
						break;
					default:
						break;
				}
				
				
				
				if(lastOne)
					break;
				
				line=line.substring(stop+2);
				start=line.indexOf(':');
				end=line.lastIndexOf(':');
			}

			
			for(int i=0;i<dist.length;i++){
				diffs+=dist[i]*dist[i];
			}
			
			totalDiff[key]+=Math.sqrt(diffs);
			size[key]++;
			
		}
		rin.close();
		for(int i=0;i<totalDiff.length;i++)
			System.out.println("Key:"+(i+1)+":"+totalDiff[i]/size[i]);
	}
	
}
