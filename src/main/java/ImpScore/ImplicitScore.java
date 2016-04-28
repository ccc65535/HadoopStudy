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
	public static String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
	public static String input,output,status;
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		conf.set("mapred.textoutputformat.separator",",");
		//String input="/home/Hadoop/data/words";
		//String output="/home/Hadoop/data/result";
		
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
				job.setMapperClass(TMapper_std.class);
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
	
	public static void doScore_std(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_std.class);
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
	
	public static void doScore_std_0(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_std_0.class);
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
	
	
	public static void doScore_origin(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_origin.class);
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
	
	
	public static void doScore_imp(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_imp.class);
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
	
	public static void doScore_LR(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_LR.class);
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
	
	public static void doScore_LR_2(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_LR_2.class);
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
	public static void doScore_LR_Line(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_LR_line.class);
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
	
	public static void doScore_byTime(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_byTime.class);
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
	
	public static void doScore_mixedTime(Configuration conf) throws Exception{
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
				job.setMapperClass(TMapper_mixedTime.class);
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
	
	static boolean isNumeric(String str){
		Pattern pattern= Pattern.compile("[0-9]*");
		return pattern.matcher(str).matches();
	}
}



class TReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{


	private MultipleOutputs<Text, DoubleWritable> output;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		output=new MultipleOutputs<Text,DoubleWritable>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {		
		double sum = 0;
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		
		output.write(key, new DoubleWritable(sum),ImplicitScore.output+"/"+ImplicitScore.testName);
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		output.close();
	}
	
}



class TMapper_std extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1){
					//if(tw>=15||q>=0.1){
						Float pref=(float) (/*Math.log(ti)/Math.log(30)**/q*(Math.cos(q*6.28)/4+0.75));
						/*Float pref=0f;
						Float Cr=(float)(Math.log(ti)/Math.log(30));
						if(q<0.5)
							pref=(float) (Cr*q*(Math.cos(q*12.56)/4+0.75));
						else
							pref=(float) (Cr*q);*/					
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}		
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}

}
class TMapper_std_0 extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1||tw>=15){
						Float pref=(float) (/*Math.log(ti)/Math.log(30)**/q*(Math.cos(q*6.28)/2+0.5));
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}		
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}

}

class TMapper_origin extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					//if(q>=0.1||tw>=15){
					if(q>=0.1){
						Float pref=q;
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}			
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}
}

class TMapper_mixedTime extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1||tw>=15){	
						
						Float pref=0f;
						if(q<0.5)
							pref=(float) (q*(Math.cos(q*6.28)/4+0.75));
						else
							pref=(float) (q*(Math.cos(q*6.28)/2+1));
						if(q>=0.25&&q<=0.75)
							pref*=(float) (q*(2/(1+Math.exp(9-tw/10))));
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}			
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}
}

class TMapper_LR extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					/*if(q>=0.1){
						Float pref=0f;
						if(q<0.9)
							pref=(float)(Math.log(ti)/Math.log(30)*q*(10.475-24.55*q+18.05*q*q));
						else
							pref=q;
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}	*/	
					
					if(q>=0.1||tw>=15){	
						Float pref= q;
						if(q<0.5)
							pref*=(1-q);
						else
							pref*=q;
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}	
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}

}


class TMapper_LR_2 extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.45){
						Float pref=0f;
						if(q>=0.68)
							pref=(float)(/*Math.log(ti)/Math.log(30)**/q*(9.348-24.55*q+18.05*q*q));
						else
							pref=(float)(/*Math.log(ti)/Math.log(30)**/q*(24.55*q-18.05*q*q-7.348));
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}
					else if(q>0.1){
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(q));
					}
				}
			}
		}
	}
}


class TMapper_LR_line extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>0.1){
						Float pref=0f;
						pref=(float) (q*1.47);
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}
				}
			}
		}
	}
}
class TMapper_imp extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1||tw>=15){
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(-255));
					}		
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}

}
class TMapper_byTime extends Mapper<Object, Text,Text,DoubleWritable>{

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
			if(!type.equals("0")&&Iid.length()>0&&!Iid.equals("00000")&&contents.length>6&&ImplicitScore.isNumeric(Iid)){
				String time_item=contents[6],time_watched=contents[7];
				if(ImplicitScore.isNumeric(time_item)&&ImplicitScore.isNumeric(time_watched)){
					//tw观看时长,ti节目时长,q观看质量
					float ti=Float.parseFloat(time_item),tw=Float.parseFloat(time_watched),q=tw/ti;
					if(q>=0.1||tw>=15){								
						Float pref=(float) (q*(2*(q-0.5)*(q-0.5)+0.5));
						context.write(new Text(ImplicitScore.testName+","+Iid),new DoubleWritable(pref));
					}			
				}
				//context.write(new Text(contents[6]), new DoubleWritable(1));
			}
		}
	}
}

