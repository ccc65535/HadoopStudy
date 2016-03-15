package ccc.examples.CFTest;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ScoreRegulation {
	public static String rootPath="/RecommendSystem/valuePref/output/";
	//public static String outPath="/RecommendSystem/valuePref/output-regulation/";
	public static String outPath="/RecommendSystem/valuePref/result";
	private static int  ceiling=10;
	
	public static void main(String args[]) throws Exception{
		Configuration conf=new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		/*Path path=new Path(rootPath);
		

		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);     
		for(int i = 0; i < 10stats.length; ++i){
			String directory=stats[i].getPath().toString();
			String Uid=directory.substring(directory.lastIndexOf('/')+1);
			Regulation(Uid,conf);
		}*/
		Combine(conf);
	}
	
	public static void Regulation(String Uid,Configuration conf){
		String input=rootPath+Uid;
		String output=outPath;
		Path srcPath=new Path(input);
		Path dstPath=new Path(output);
		
		try {
			FileSystem hdfs=srcPath.getFileSystem(conf);
			InputStream in1=hdfs.open(srcPath);
			//OutputStream out=hdfs.create(dstPath);
			double sum=0,score=0;
			int n=0;
			
			
			BufferedReader reader1=new BufferedReader(new InputStreamReader(in1));
			
			//reader1.mark(1000);
			String line="";
			while((line=reader1.readLine())!=null){
				sum+=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				n++;
			}
			
			reader1.close();
			in1.close();
			//reader=new BufferedReader(new InputStreamReader(in));
			
			
			
			InputStream in2=hdfs.open(srcPath);
			OutputStream out=hdfs.create(dstPath);

			BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
			BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
			/*BufferedWriter writer= new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(output, true))); */
			
			line="";
			while((line=reader2.readLine())!=null){
				score=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				line=line.substring(0,line.lastIndexOf(',')+1);
				score/=(sum/n);
				line+=score/(1+score)*ceiling;
				writer.write(line);
				writer.newLine();
			}
			reader2.close();
			in1.close();
			writer.close();
			out.close();
			
		}
		catch (IOException e) {
			e.printStackTrace();;
		}
	}

	public static void Combine(Configuration conf) throws IOException{
		
		Path path=new Path(rootPath);
		String output=outPath;
		Path dstPath=new Path(output);
		
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);   		
		OutputStream out=fs.create(dstPath);
		
		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
		
		for(int i = 0; i < stats.length; ++i){
			String directory=stats[i].getPath().toString();
			String Uid=directory.substring(directory.lastIndexOf('/')+1);
					
			String input=rootPath+Uid;	
			Path srcPath=new Path(input);

			double sum=0,score=0;
			int n=0;
			
			InputStream in1=fs.open(srcPath);
			BufferedReader reader1=new BufferedReader(new InputStreamReader(in1));
			String line="";
			while((line=reader1.readLine())!=null){
				sum+=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				n++;
			}	
			reader1.close();
			in1.close();
			
			
			
			InputStream in2=fs.open(srcPath);
			BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
			line="";
			while((line=reader2.readLine())!=null){
				score=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				line=line.substring(0,line.lastIndexOf(',')+1);
				score/=(sum/n);
				line+=score/(1+score)*ceiling;
				writer.write(line);
				writer.newLine();
			}
			reader2.close();
			in1.close();
		}
		writer.close();
		out.close();
	}

}
