package ImpScore;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ScoreRegulation {
	//public static String HDFS="hdfs://202.113.127.208:9000";
	public static String rootPath;
	//public static String outPath="/RecommendSystem/valuePref/output-regulation/";
	public static String outPath;
	public static String outPath1;
	public static String outPath2;
	private static int  ceiling=5;
	
	public static void main(String args[]) throws Exception{
		Configuration conf=new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		
		rootPath="/RecommendSystem/valuePref-new2/output-noceiling/";
		//public static String outPath="/RecommendSystem/valuePref/output-regulation/";
		outPath="/RecommendSystem/valuePref-new3/result-alpha";
		outPath1="/RecommendSystem/valuePref-new3/result-alpha-1";
		outPath2="/RecommendSystem/valuePref-new3/result-alpha-2";
		/*Path path=new Path(rootPath);
		

		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);     
		for(int i = 0; i < 10stats.length; ++i){
			String directory=stats[i].getPath().toString();
			String Uid=directory.substring(directory.lastIndexOf('/')+1);
			Regulation(Uid,conf);
		}*/
		Combine(conf,10);
		Combine1(conf,10);
		Combine2(conf,10);
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

	///
	///同一用户评分标准化，设置上限
	///
	public static void Combine(Configuration conf,int size) throws IOException{
	
		Path path=new Path(rootPath);
		String output=outPath;
		Path dstPath=new Path(output);
		
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);   		
		OutputStream out=fs.create(dstPath);
		
		int numUser;
		if(size<=stats.length)
			numUser=size;
		else if(size==0)
			numUser=stats.length;
		else{
			System.out.println("size out of amount of the users.");
			return;
		}
		
		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
		
		for(int i = 0; i < numUser; ++i){
			String directory=stats[i].getPath().toString();
			String Uid=directory.substring(directory.lastIndexOf('/')+1);
					
			String input=rootPath+Uid;	
			Path srcPath=new Path(input);
			
			String line="";
			double sum=0,score=0;
			int numPrefs=0;
			
			InputStream in1=fs.open(srcPath);
			BufferedReader reader1=new BufferedReader(new InputStreamReader(in1));
			
			while((line=reader1.readLine())!=null){
				sum+=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				numPrefs++;
			}	
			reader1.close();
			in1.close();
			
			
			InputStream in2=fs.open(srcPath);
			BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
			line="";
			while((line=reader2.readLine())!=null){
				score=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				line=line.substring(0,line.lastIndexOf(',')+1);
				score/=(sum/numPrefs/2);
				score=(score>ceiling?ceiling:score);
				//int s=(int)(score+0.5);
				//if(s!=0){
					line+=String.format("%.2f", score);//score/(1+score)*ceiling;
					writer.write(line);
					writer.newLine();
				//}
				
			}
			reader2.close();
			in2.close();
		}
		writer.close();
		out.close();
	}
	
	///
	///同一用户的评分之间无标准化,设置上限
	///
	public static void Combine1(Configuration conf,int size) throws IOException{
		
		Path path=new Path(rootPath);
		String output=outPath1;
		Path dstPath=new Path(output);
		
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(path);   		
		OutputStream out=fs.create(dstPath);
		
		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
		
		int numUser;
		if(size<=stats.length)
			numUser=size;
		else if(size==0)
			numUser=stats.length;
		else{
			System.out.println("size out of amount of the users.");
			return;
		}
		
		for(int i = 0; i < numUser; ++i){
			String directory=stats[i].getPath().toString();
			String Uid=directory.substring(directory.lastIndexOf('/')+1);
					
			String input=rootPath+Uid;	
			Path srcPath=new Path(input);
			
			String line="";
			double score=0;

			InputStream in2=fs.open(srcPath);
			BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
			line="";
			while((line=reader2.readLine())!=null){
				score=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
				line=line.substring(0,line.lastIndexOf(',')+1);
				score=(score>ceiling?ceiling:score);
				line+=String.format("%.2f", score);
				writer.write(line);
				writer.newLine();

				
			}
			reader2.close();
			in2.close();
		}
		writer.close();
		out.close();
	}
	
	///
		///评分无处里
		///
		public static void Combine2(Configuration conf,int size) throws IOException{
			
			Path path=new Path(rootPath);
			String output=outPath2;
			Path dstPath=new Path(output);
			
			FileSystem fs=FileSystem.get(conf);
			FileStatus[] stats = fs.listStatus(path);   		
			OutputStream out=fs.create(dstPath);
			
			BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
			int numUser;
			if(size<=stats.length)
				numUser=size;
			else if(size==0)
				numUser=stats.length;
			else{
				System.out.println("size out of amount of the users.");
				return;
			}
			
			for(int i = 0; i < numUser; ++i){
				String directory=stats[i].getPath().toString();
				String Uid=directory.substring(directory.lastIndexOf('/')+1);
						
				String input=rootPath+Uid;	
				Path srcPath=new Path(input);
				
				String line="";
				InputStream in2=fs.open(srcPath);
				BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
				line="";
				while((line=reader2.readLine())!=null){					
					//line=line.substring(0,line.lastIndexOf(',')+1);				
					//line+=ceiling;
					writer.write(line);
					writer.newLine();

					
				}
				reader2.close();
				in2.close();
			}
			writer.close();
			out.close();
		}
		
		
		public static void Combine3(Configuration conf,int size) throws IOException{
			
			Path path=new Path(rootPath);
			String output=outPath2;
			Path dstPath=new Path(output);
			
			FileSystem fs=FileSystem.get(conf);
			FileStatus[] stats = fs.listStatus(path);   		
			OutputStream out=fs.create(dstPath);
			
			BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(out));
			
			int numUser;
			if(size<=stats.length)
				numUser=size;
			else if(size==0)
				numUser=stats.length;
			else{
				System.out.println("size out of amount of the users.");
				return;
			}
			
			for(int i = 0; i < numUser; ++i){
				String directory=stats[i].getPath().toString();
				String Uid=directory.substring(directory.lastIndexOf('/')+1);
						
				String input=rootPath+Uid;	
				Path srcPath=new Path(input);
				
				String line="";
				double score=0;

				InputStream in2=fs.open(srcPath);
				BufferedReader reader2=new BufferedReader(new InputStreamReader(in2));
				line="";
				while((line=reader2.readLine())!=null){
					score=Double.parseDouble(line.substring(line.lastIndexOf(',')+1));
					//score=score*(0.935-2.46*score+1.8*score*score);
					/*if(score>0.68)
						score=8.348-24.55*score+18.05*score*score;
					else
						score=-8.348+24.55*score-18.05*score*score;*/
					
					line=line.substring(0,line.lastIndexOf(',')+1);
					line+=score;
					writer.write(line);
					writer.newLine();

					
				}
				reader2.close();
				in2.close();
			}
			writer.close();
			out.close();
		}

}
