package DataMining;

import java.io.*;
import org.apache.hadoop.fs.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WatchingDataDistribution {
	public static void main(String args[]) throws Exception{
		PrefData();
	}
	
	public static void watchData() throws Exception{
		String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
		String root=HDFS+"valuePref/input/";
		String out=HDFS+"valuePref/DataDistribution-watch";
		
		Configuration conf=new Configuration();
		conf.addResource("./bin/core-site.xml");
		conf.addResource("./bin/hdfs-site.xml");
		
		Path rp=new Path(root);		
		FileSystem hdfs=rp.getFileSystem(conf);
		FileStatus[] stats = hdfs.listStatus(rp);
		Path outPath=new Path(out);
		OutputStream out1=hdfs.create(outPath);
		BufferedWriter buffout=new BufferedWriter(new OutputStreamWriter(out1));
		
		int nums[]=new int[101];
		
		
		for(int i = 0; i < stats.length; ++i){
			if (stats[i].isDirectory()){
				String directory=stats[i].getPath().toString();	
				FileStatus[] childStatus = hdfs.listStatus(new Path(directory));
				

				for(int j = 0; j < childStatus.length; ++j){
					String file=childStatus[j].getPath().toString();
					file.toString();
					Path inPath=new Path(file);
					InputStream in1=hdfs.open(inPath);
					BufferedReader buffin=new BufferedReader(new InputStreamReader(in1));
					String line;
					while((line=buffin.readLine())!=null){
						if(line.length()>10){
							String [] contents=line.split("\t");
							String date=contents[0];
							String type=contents[1];
							String Iid=contents[2];
							//context.write(new Text(lines[i].split("\t")[0]),new IntWritable(1));
							if(Iid.contains("_")){
								Iid=Iid.substring(0,Iid.lastIndexOf("_"));
							}	
							if(Iid.length()>19)
								Iid=Iid.substring(Iid.length()-10, Iid.length());
							//筛选数据
							if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")
								&&contents.length>6&&isNumeric(Iid)){
								String time_item=contents[6],time_watched=contents[7];
								if(isNumeric(time_item)&&isNumeric(time_watched)){
									//tw观看时长,ti节目时长,q观看质量
									float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
									Integer range=(int) (q*100);
									if(range>100)
										range=100;
									nums[range]++;
								}
							}
						}
					}
					buffin.close();
				}	
			}  
		}
		
		nums.toString();
		for(int k=0;k<nums.length;k++){
			buffout.write(nums[k]+",");
		}
		buffout.newLine();
		for(int k=0;k<nums.length;k++){
			buffout.write("0."+k+",");
		}
		buffout.newLine();
		buffout.close();

	}
	public static void PrefData() throws Exception{
		String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
		String root=HDFS+"beta-1/Preprocess/ImpScore-new/scores-std_origin/";
		String out=HDFS+"beta-1/Preprocess/ImpScore-new/DataDistribution-pref";
		
		Configuration conf=new Configuration();
		conf.addResource("./bin/core-site.xml");
		conf.addResource("./bin/hdfs-site.xml");
		
		Path rp=new Path(root);		
		FileSystem hdfs=rp.getFileSystem(conf);
		FileStatus[] stats = hdfs.listStatus(rp);
		Path outPath=new Path(out);
		OutputStream out1=hdfs.create(outPath);
		//BufferedWriter buffout=new BufferedWriter(new OutputStreamWriter(out1));
		
		double nums[]=new double[2001];
		for(int j = 0; j < stats.length; ++j){
			String file=stats[j].getPath().toString();
			file.toString();
			Path inPath=new Path(file);
			InputStream in1=hdfs.open(inPath);
			BufferedReader buffin=new BufferedReader(new InputStreamReader(in1));
			String line;
			while((line=buffin.readLine())!=null){
				if(line.length()>10){
					String [] contents=line.split(",");
					double pref=Double.parseDouble(contents[2]);
					int range=(int)(pref*100);
					if(range>2000)
						range=2000;
					nums[range]++;
				}
				
			}
			buffin.close();
		}	
			 
		nums.toString();
		for(int k=0;k<nums.length;k++){
			//if(k%10!=0)
				//System.out.print(nums[k]+",");
			double a=0;
			if(nums[k]!=0)
				a=10/nums[k];
			System.out.print(((double)k/100)+":"+a+",");
			//buffout.write(nums[k]+",");
		}
		//buffout.newLine();
	/*	System.out.println();
		for(int k=0;k<nums.length;k++){
			if(k%10!=0)
				System.out.print(((double)k/100)+",");
			//buffout.write(((double)k/100)+",");
		}
		//buffout.newLine();
		//buffout.close();*/
	}
	
	
	
	static boolean isNumeric(String str){
		Pattern pattern= Pattern.compile("[0-9]*");
		return pattern.matcher(str).matches();
	}	

}

