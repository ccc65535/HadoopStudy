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
		//watchData_3D();
		//PrefData();
		watchData();
	}
	
	public static void watchData() throws Exception{
		String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
		String root=HDFS+"valuePref/input/";
		String out=HDFS+"valuePref/DataDistribution-watch-1h";
		
		Configuration conf=new Configuration();
		conf.addResource("./bin/core-site.xml");
		conf.addResource("./bin/hdfs-site.xml");
		
		Path rp=new Path(root);		
		FileSystem hdfs=rp.getFileSystem(conf);
		FileStatus[] stats = hdfs.listStatus(rp);
		Path outPath=new Path(out);
		OutputStream out1=hdfs.create(outPath);
		BufferedWriter buffout=new BufferedWriter(new OutputStreamWriter(out1));
		
		int nums[]=new int[11];
		
		long startTime=System.currentTimeMillis();
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
									//Integer range=(int) tw;
									Integer range=(int) (q*10);
									if(range>10)
										range=10;
									//if(ti>=3600)
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
			if(nums[k]!=0){
				//buffout.write(nums[k]+",");
				buffout.write(k+"\t"+nums[k]);
				buffout.newLine();
				//System.out.print(((double)k/100)+":"+nums[k]+",");
				System.out.print(k+":"+nums[k]+",");
			}
		}
		/*buffout.newLine();
		for(int k=0;k<nums.length;k++){
			if(nums[k]!=0){
				buffout.write("0."+k+",");
			}
		}
		buffout.newLine();*/
		buffout.close();
		long endTime=System.currentTimeMillis();
		System.out.println(endTime-startTime+"ms");

	}

	
	
	public static void watchData_3D() throws Exception{
		String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
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
		
		int nums[][]=new int[18001][101];
		//ArrayList<int []> list=new ArrayList<int []>();
		
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
	
									int x=(int) tw,y=(int) ti;
									if(x>18000)
										x=18000;
									
								
									
									nums[x][y]++;
								}
							}
						}
					}
					buffin.close();
				}	
			}  
		}
		
		nums.toString();
		for(int k=0;k<18000;k++){
			for(int g=0;g<100;g++){
				if(nums[k][g]>0){
					buffout.write(k+"\t"+g+"\t"+nums[k][g]);
					buffout.newLine();
					System.out.println(k+","+g+","+nums[k][g]);
				}
			}
		}

		buffout.newLine();
		buffout.close();

	}
	
	
	public static void PrefData() throws Exception{
		String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
		String root=HDFS+"beta-1/Preprocess/ImpScore-2nd/scores-byTime/";
		String out=HDFS+"beta-1/Preprocess/ImpScore-2nd/DataDistribution-byTime";
		
		Configuration conf=new Configuration();
		conf.addResource("./bin/core-site.xml");
		conf.addResource("./bin/hdfs-site.xml");
		
		Path rp=new Path(root);		
		FileSystem hdfs=rp.getFileSystem(conf);
		FileStatus[] stats = hdfs.listStatus(rp);
		Path outPath=new Path(out);
		OutputStream out1=hdfs.create(outPath);
		BufferedWriter buffout=new BufferedWriter(new OutputStreamWriter(out1));
		
		double nums[]=new double[5001];
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
					if(range>5000)
						range=5000;
					nums[range]++;
				}
				
			}
			buffin.close();
		}	
			 
		nums.toString();
		for(int k=0;k<nums.length;k++){
			if(nums[k]!=0){
				buffout.write(nums[k]+",");
				//System.out.print(((double)k/100)+":"+nums[k]+",");
				System.out.print((double)k/100+":"+nums[k]+",");
			}
		}
		buffout.newLine();
		for(int k=0;k<nums.length;k++){
			if(nums[k]!=0){
				buffout.write(/*"0."+*/k+",");
			}
		}
		buffout.newLine();
		buffout.close();

	}
	
	
	
	static boolean isNumeric(String str){
		Pattern pattern= Pattern.compile("[0-9]*");
		return pattern.matcher(str).matches();
	}	

}

