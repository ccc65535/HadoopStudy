package ImpScore;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class ImplicitScore {
	
	public static String testName="testName";
	//public static String HDFS="hdfs://202.113.127.209:9000/CollaborativeFilter/";
	public static String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
	public static String input,output,status;
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		conf.set("mapred.textoutputformat.separator",",");
		//String input="/home/Hadoop/data/words";
		//String output="/home/Hadoop/data/result";
		
		//String input=HDFS+"testInput/0220000011";
		input=HDFS+"/valuePref/input/";
		output=HDFS+"/valuePref-new2/output-noceiling";
		status=HDFS+"/valuePref-new2/outStatus";
		
		Path path=new Path(input);
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);  
		
		if(fs.isDirectory(new Path(status))){
			fs.delete(new Path(status), true);
		}
		
		for(int i = 0; i < stats.length; ++i){
			if (stats[i].isDirectory()){
				String directory=stats[i].getPath().toString();
				String Uid=directory.substring(directory.lastIndexOf('/')+1);
				
				
				testName=Uid;
				//TMapper_2.total=0;
				Job job = new Job(conf, "test");
				job.setJarByClass(ImplicitScore.class);
				job.setMapperClass(TMapper_2.class);
				job.setReducerClass(TReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				FileInputFormat.addInputPath(job, new Path(input+"/"+Uid));
				FileOutputFormat.setOutputPath(job, new Path(status+"/"+Uid));
				job.waitForCompletion(true);
			}  
		}
		fs.close();
		/*Job job = new Job(conf, "test");
		job.setJarByClass(ImplicitScore.class);
		job.setMapperClass(TMapper_2.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(TReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
	}
	
	public static void doScore(Configuration conf) throws Exception{
		Path path=new Path(input);
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);  
		
		if(fs.isDirectory(new Path(status))){
			fs.delete(new Path(status), true);
		}
		
		for(int i = 0; i < stats.length; ++i){
			if (stats[i].isDirectory()){
				String directory=stats[i].getPath().toString();
				String Uid=directory.substring(directory.lastIndexOf('/')+1);
				
				
				testName=Uid;
				//TMapper_2.total=0;
				Job job = new Job(conf, "test");
				job.setJarByClass(ImplicitScore.class);
				job.setMapperClass(TMapper_2.class);
				job.setReducerClass(TReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				FileInputFormat.addInputPath(job, new Path(input+"/"+Uid));
				FileOutputFormat.setOutputPath(job, new Path(status+"/"+Uid));
				job.waitForCompletion(true);
			}  
		}
		fs.close();
	}
	
}

/*class TMapper extends Mapper<Object, Text,Text,DoubleWritable>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		
		//将value按行分割
		String [] lines=value.toString().split("\n");
		
		for(int i=0;i<lines.length;i++){
			
			//有效行判断
			if(lines[i].length()>10){
				
				//每行按分割符分割
				String [] contents=lines[i].split("\t");
				String date=contents[0];
				String type=contents[1];
				String Iid=contents[2];
				//context.write(new Text(lines[i].split("\t")[0]),new IntWritable(1));
				
				
				//筛选数据
				if(!type.equals("0")&&Iid!=null&&!Iid.equals("00000")&&contents.length>6){
					String time_item=contents[6],time_watched=contents[7];
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1){
						Float pref=(float) (Math.log(ti)/Math.log(30)*q);
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
						
					}
					//context.write(new Text(contents[6]), new DoubleWritable(1));
				}
			}
		}
		
	}
		
}*/
	
class TReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{


	private MultipleOutputs<Text, DoubleWritable> output;
	private static int  ceiling=5;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		output=new MultipleOutputs<Text,DoubleWritable>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
		/*int count =	0;
		while(value.iterator().hasNext()){
			count++;
			value.iterator().next();
		}
		//System.out.println(key.toString()+(new IntWritable(count)));
		context.write(key, new IntWritable(count));*/
		
		double sum = 0;
		int count =	0;
		for (DoubleWritable val : values) {
			sum += val.get();
			count++;
		}
		/*if(sum>ceiling)
			sum=ceiling;*/
		
		//sum/=TMapper_2.total;
		output.write(key, new DoubleWritable(sum/*/count*/),ImplicitScore.output+"/"+ImplicitScore.testName);
		
		
		
		/*for(DoubleWritable val:values){
			context.write(key,val);
		}*/
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		output.close();
	}
	
}



class TMapper_2 extends Mapper<Object, Text,Text,DoubleWritable>{

	//public static double total;
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

			String line=value.toString();
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
				if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&isNumeric(Iid)){
					String time_item=contents[6],time_watched=contents[7];
					if(isNumeric(time_item)&&isNumeric(time_watched)){
						//tw观看时长,ti节目时长,q观看质量
						float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
						if(q>=0.1){
							
							Float pref=(float) (Math.log(ti)/Math.log(30)*q*(Math.cos(q*6.28)/4+0.75));
							/*Float pref=0f;
							Float Cr=(float)(Math.log(ti)/Math.log(30));
							if(q<0.5)
								pref=(float) (Cr*q*(Math.cos(q*12.56)/4+0.75));
							else
								pref=(float) (Cr*q);*/
							
							//total+=pref;
							context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
						}
						
					}
					//context.write(new Text(contents[6]), new DoubleWritable(1));
				}
			}
	}
	
	static boolean isNumeric(String str){
		Pattern pattern= Pattern.compile("[0-9]*");
		return pattern.matcher(str).matches();
	}
		
}
