package Cluster;

import java.io.*;
import java.util.*;

public class DivideByCluster {
	public static String cPath="/project/data/cluster/IDCluster";
	public static String scorePath="/project/data/cluster/trainSet.txt";
	public static String outPath1="/project/data/cluster/c1Set.txt";
	public static String outPath2="/project/data/cluster/c2Set.txt";
	public static String outPath3="/project/data/cluster/c3Set.txt";
	public static String outPath4="/project/data/cluster/c4Set.txt";
	public static String outPath5="/project/data/cluster/c5Set.txt";
	
	public static List<ClusterInfo> cinfoList;
	public static void main(String args[]) throws Exception{
		LoadClusterInfo();
		BufferedReader sin=new BufferedReader(new FileReader(scorePath));
		BufferedWriter out1=new BufferedWriter(new FileWriter(outPath1));
		BufferedWriter out2=new BufferedWriter(new FileWriter(outPath2));
		BufferedWriter out3=new BufferedWriter(new FileWriter(outPath3));
		BufferedWriter out4=new BufferedWriter(new FileWriter(outPath4));
		BufferedWriter out5=new BufferedWriter(new FileWriter(outPath5));
		
		String line="";
		int n=1;
		Long Uid_p=0L,Cid=0L;
		while((line=sin.readLine())!=null){
			
			Long Uid=Long.parseLong(line.substring(0, line.indexOf(',')));
			if(n==1){
				Uid_p=Uid;
				Cid=getCid(Uid,cinfoList);
			}
			if(!Uid.equals(Uid_p)){
				Cid=getCid(Uid,cinfoList);
				Uid_p=Uid;
			}
			n++;
			Cid.toString();
			
			switch(Cid.intValue()){
				case 39:
					out1.write(line);
					out1.newLine();
					break;
				case 235:
					out2.write(line);
					out2.newLine();
					break;
				case 1084:
					out3.write(line);
					out3.newLine();
					break;
				case 1029:
					out4.write(line);
					out4.newLine();
					break;
				case 1139:
					out5.write(line);
					out5.newLine();
					break;
				default:
					break;
				
			}
			System.out.println(n);
			
		}
		sin.close();
		out1.close();
		out2.close();
		out3.close();
		out4.close();
		out5.close();
		
		
	}
	
	public static long getCid(Long Uid,List<ClusterInfo> list){
		ClusterInfo cif=new ClusterInfo();
		int start,end,middle;
		start=0;
		end=list.size();
		
		middle=(start+end)/2;
		cif=list.get(middle);
		while(!Uid.equals(cif.Uid)){		
			if(Uid>cif.Uid)
				start=middle+1;
			else if(Uid<cif.Uid)
				end=middle-1;
			middle=(start+end)/2;
			cif=list.get(middle);
			if(start>=end)
				break;
		}
		if(Uid.equals(cif.Uid))
			return cif.Cid;
		else
			return 0;
	}
	
	public static void LoadClusterInfo() throws Exception{
		BufferedReader cin=new BufferedReader(new FileReader(cPath));
		cinfoList=new ArrayList<ClusterInfo>();
		String line="";
		while((line=cin.readLine())!=null){
			ClusterInfo cinfo=new ClusterInfo();
			cinfo.Uid=Long.parseLong(line.substring(0, line.indexOf('\t')));
			cinfo.Cid=Long.parseLong(line.substring(line.indexOf('\t')+2,line.lastIndexOf('\t')));
			
			cinfoList.add(cinfo);
			//System.out.println(cinfo.Uid+","+cinfo.Cid);
		}
		cinfoList.sort(new ClusterInfo());
		cin.close();
	}

	
}

class ClusterInfo implements Comparator<ClusterInfo>{
	public Long Uid;
	public Long Cid;

	public int compare(ClusterInfo o1, ClusterInfo o2) {
		if(o1.Uid>o2.Uid)
			return 1;
		else if(o1.Uid<o2.Uid)
			return -1;
		else
		return 0;
	}
	
}