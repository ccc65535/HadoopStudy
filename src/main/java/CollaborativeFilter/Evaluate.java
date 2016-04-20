package CollaborativeFilter;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class Evaluate {
	private String resultPath,examplePath,outPath;
	public List<Pref> result=new ArrayList<Pref>();
	public List<Pref> example=new ArrayList<Pref>();
	
	
	public String getOutPath() {
		return outPath;
	}
	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}
	
	public String getResultPath() {
		return resultPath;
	}
	public void setResultPath(String resultPath) {
		this.resultPath = resultPath;
	}

	public String getExamplePath() {
		return examplePath;
	}
	public void setExamplePath(String examplePath) {
		this.examplePath = examplePath;
	}
	
	public void generateData() throws Exception{
		BufferedReader resultFile=new BufferedReader(new FileReader(resultPath));
		BufferedReader exampleFile=new BufferedReader(new FileReader(examplePath));
		String line;
		while((line=resultFile.readLine())!=null){
			Pref p=new Pref();
			p.uID=line.split(",")[0];
			p.iID=line.split(",")[1];
			p.score=line.split(",")[2];
			result.add(p);
		}
		resultFile.close();
		
		while((line=exampleFile.readLine())!=null){
			Pref p=new Pref();
			p.uID=line.split(",")[0];
			p.iID=line.split(",")[1];
			p.score=line.split(",")[2];
			example.add(p);
		}
		exampleFile.close();
	}

	public void generateDataHDFS(Configuration conf) throws Exception{
		
		Path res=new Path(resultPath);
		Path examp=new Path(examplePath);
		org.apache.hadoop.fs.FileSystem hdfs=res.getFileSystem(conf);
		
		InputStream resPath=hdfs.open(res);
		InputStream exampPath=hdfs.open(examp);

		
		BufferedReader resultFile=new BufferedReader(new InputStreamReader(resPath));
		BufferedReader exampleFile=new BufferedReader(new InputStreamReader(exampPath));
		String line;
		while((line=resultFile.readLine())!=null){
			Pref p=new Pref();
			p.uID=line.split(",")[0];
			p.iID=line.split(",")[1];
			p.score=line.split(",")[2];
			result.add(p);
		}
		resultFile.close();
		
		while((line=exampleFile.readLine())!=null){
			line.toString();
			//System.out.println(line);
			Pref p=new Pref();
			p.uID=line.split(",")[0];
			p.iID=line.split(",")[1];
			p.score=line.split(",")[2];
			example.add(p);
		}
		exampleFile.close();
	}
	
	public void eval(BufferedWriter out) throws Exception{
		int rn,en,correct=0;
		double precision,recall;
		rn=result.size();
		en=example.size();
		
		Pref p=new Pref();
		for(int i=0;i<rn;i++){
			p=result.get(i);
			if(contains(p,example)){
				correct++;
			}
		}
		precision=(double)correct/rn;
		recall=(double)correct/en;
		
		System.out.println("Precision:"+precision);
		System.out.println("Recall:"+recall);
		out.write("Precision:"+precision+" ,Recall:"+recall);
		out.newLine();
		
		
	}
	public void eval1(BufferedWriter out) throws Exception{
		int rn,en,correct=0;
		double precision,recall;
		boolean contains=false;
		List<Double> diffs=new ArrayList<Double>();
		Double MSE=(double) 0;
		List<Pref>res=new ArrayList<Pref>(result);
		List<Pref>exp=new ArrayList<Pref>(example);
		rn=res.size();
		en=exp.size();
		
		Pref p=new Pref();
		Pref q=new Pref();
		for(int i=0;i<rn;i++){
			p=res.get(i);
			for(int j=0;j<exp.size();){
				q=exp.get(j);
				if(Long.parseLong(p.uID)==Long.parseLong(q.uID)&&Long.parseLong(p.iID)==Long.parseLong(q.iID)){
					correct++;
					
					diffs.add(Double.parseDouble(p.score)-Double.parseDouble(q.score));
					exp.remove(j);
					contains=true;
				}
				else
					j++;
			}
			if(!contains){
				if(!p.score.equals("NaN"))
					diffs.add(Double.parseDouble(p.score));
			}
		}
		
		if(!exp.isEmpty()){
			for(int j=0;j<exp.size();j++){		
				if(!exp.get(j).score.equals("NaN"))
					diffs.add(Double.parseDouble(exp.get(j).score));
			}
		}
		
		if(!diffs.isEmpty()){
			int n=diffs.size();
			for(int j=0;j<n;j++){
				if(diffs.get(j)!=Double.NaN)
					MSE+=(diffs.get(j)*diffs.get(j))/n;
			}
		}
		
		precision=(double)correct/rn;
		recall=(double)correct/en;
		
		System.out.println("Precision:"+precision);
		System.out.println("Recall:"+recall);
		System.out.println("MSE:"+MSE);
		out.write("Precision:"+precision+" ,Recall:"+recall+" ,MSE:"+MSE);
		out.newLine();
		
		
	}
	
	
	
	public void evalHDFS(Configuration conf) throws Exception{
		Path in=new Path(resultPath);
		Path out=new Path(outPath);
		org.apache.hadoop.fs.FileSystem hdfs=in.getFileSystem(conf);
		
		//InputStream in1=hdfs.open(in);
		OutputStream out1=hdfs.create(out);
		BufferedWriter outFile=new BufferedWriter(new OutputStreamWriter(out1));
		
		
		
		int rn,en,correct=0;
		double precision,recall;
		boolean contains=false;
		List<Double> diffs=new ArrayList<Double>();
		Double MSE=(double) 0;
		List<Pref>res=new ArrayList<Pref>(result);
		List<Pref>exp=new ArrayList<Pref>(example);
		rn=res.size();
		en=exp.size();
		
		Pref p=new Pref();
		Pref q=new Pref();
		for(int i=0;i<rn;i++){
			p=res.get(i);
			for(int j=0;j<exp.size();){
				q=exp.get(j);
				if(Long.parseLong(p.uID)==Long.parseLong(q.uID)&&Long.parseLong(p.iID)==Long.parseLong(q.iID)){
					correct++;
					
					diffs.add(Double.parseDouble(p.score)-Double.parseDouble(q.score));
					exp.remove(j);
					contains=true;
				}
				else
					j++;
			}
			if(!contains){
				if(!p.score.equals("NaN"))
					diffs.add(Double.parseDouble(p.score));
			}
		}
		
		if(!exp.isEmpty()){
			for(int j=0;j<exp.size();j++){		
				if(!exp.get(j).score.equals("NaN"))
					diffs.add(Double.parseDouble(exp.get(j).score));
			}
		}
		
		if(!diffs.isEmpty()){
			int n=diffs.size();
			for(int j=0;j<n;j++){
				if(diffs.get(j)!=Double.NaN)
					MSE+=(diffs.get(j)*diffs.get(j))/n;
			}
		}
		
		precision=(double)correct/rn;
		recall=(double)correct/en;
		
		System.out.println("Precision:"+precision);
		System.out.println("Recall:"+recall);
		System.out.println("MSE:"+MSE);
		outFile.write("Precision:"+precision+" ,Recall:"+recall+" ,MSE:"+MSE);
		outFile.newLine();
		outFile.close();
		
	}
	
	
	/*public void evalUser(List<Pref> userResult,List<Pref> userExample){

		if(userResult==null||userResult.size()==0){
			System.out.println("null");
		}
		else if(userExample==null||userExample.size()==0){
			long uID=Long.parseLong(userResult.get(0).uID);
			System.out.println(uID+"\tno example");
		}
		else{
			int rn,en,correct=0;
			long uID=Long.parseLong(userResult.get(0).uID);
			double precision,recall;
			rn=userResult.size();
			en=userExample.size();
			
			Pref p=new Pref();
			for(int i=0;i<rn;i++){
				p=userResult.get(i);
				if(contains(p,userExample)){
					correct++;
				}
			}
			precision=(double)correct/rn;
			recall=(double)correct/en;
			
			System.out.println(uID+"\tPrecision:"+precision+", Recall:"+recall);
		}
		
	}*/
	
	public void evalUser1(List<Pref> userResult,List<Pref> userExample,BufferedWriter out) throws Exception{

		
		if(userResult==null||userResult.size()==0){
			System.out.println("null");
		}
		else if(userExample==null||userExample.size()==0){
			long uID=Long.parseLong(userResult.get(0).uID);
			System.out.println(uID+"\tno example");
		}
		else{
			
			int rn,en,correct=0;
			long uID=Long.parseLong(userResult.get(0).uID);
			double precision,recall;
			List<Double> diffs=new ArrayList<Double>();
			double MSE=0;
			boolean contains;
			
			
			rn=userResult.size();
			en=userExample.size();
			
			Pref p1,p2;
			for(int i=0;i<userResult.size();i++){
				p1=userResult.get(i);
				contains=false;
				for(int j=0;j<userExample.size();j++){
					p2=userExample.get(j);
					if(Long.parseLong(p1.iID)==Long.parseLong(p2.iID)){
						contains=true;
						diffs.add(Double.parseDouble(p1.score)-Double.parseDouble(p2.score));
						userExample.remove(j);
						correct++;
						break;
					}
				}
				
				if(!contains){
					diffs.add(Double.parseDouble(p1.score));
				}
			}
			
			if(!userExample.isEmpty()){
				for(int j=0;j<userExample.size();j++){
					p2=userExample.get(j);
					diffs.add(Double.parseDouble(p2.score));
				}
					
			}
			
			if(!diffs.isEmpty()){
				for(int n=0;n<diffs.size();n++)
					MSE+=diffs.get(n)*diffs.get(n);
				MSE/=diffs.size();
			}
			
			precision=(double)correct/rn;
			recall=(double)correct/en;
			
			System.out.println(uID+"\tPrecision:"+precision+", Recall:"+recall+", MSE:"+MSE);
			out.write(uID+"\tPrecision:"+precision+", Recall:"+recall+", MSE:"+MSE);
			out.newLine();
		}
		
	}
	
	
	
	
	
	public static boolean contains(Pref record,List<Pref> list){
		if(list!=null){
			int n=list.size();
			Pref content;
			for(int i=0;i<n;i++){
				content=list.get(i);
				if(Long.parseLong(record.uID)==Long.parseLong(content.uID)&&Long.parseLong(record.iID)==Long.parseLong(content.iID)){
					return true;
				}
			}
			return false;
		}
		
		else{
			System.out.println("list is empty");
			return false;
		}
	}
	
	public static void main(String args[]){
		try {
			/*Evaluate val=new Evaluate();

			val.resultPath="/project/data/cluster/final";
			val.examplePath="/project/data/cluster/example.txt";
			val.outPath="/project/data/cluster/eval";
			val.generateData();
			val.toString();
			
			BufferedWriter out=new BufferedWriter(new FileWriter(val.outPath));
			
			val.eval1(out);*/
			
			
			
			Evaluate val=new Evaluate();
			String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
			Configuration conf=new Configuration();
			conf.addResource(new Path("./bin/core-site.xml"));
			conf.addResource(new Path("./bin/hdfs-site.xml"));
			
			val.resultPath=HDFS+"/CF/final";
			val.examplePath=HDFS+"/CF/example.txt";
			val.outPath=HDFS+"/CF/eval";
			val.generateDataHDFS(conf);
			val.evalHDFS(conf);
			
			
			
			
			/*List<Pref> userResult=new ArrayList<Pref>();
			List<Pref> userExample=new ArrayList<Pref>();
			long u,u1,u2;
			boolean flag=false;
			u=Long.parseLong(val.result.get(0).uID);
			for(int i=0,j=0;i<val.result.size()&&j<val.example.size();){
				
				u1=Long.parseLong(val.result.get(i).uID);
				u2=Long.parseLong(val.example.get(j).uID);
				if(u==u1||u==u2){
					if(u1==u2){
						userResult.add(val.result.get(i));
						userExample.add(val.example.get(j));
						i++;j++;
					}
					if(u1<u2){
						userResult.add(val.result.get(i));
						i++;
						//u1=Long.parseLong(val.result.get(i).uID);
					}
					if(u1>u2){
						userExample.add(val.example.get(j));
						j++;
						//u2=Long.parseLong(val.result.get(j).uID);
					}
					flag=true;
				}
				else{
					if(flag){
						val.evalUser1(userResult, userExample,out);
						userResult.clear();
						userExample.clear();
						//userResult=new ArrayList<Pref>();
						//userExample=new ArrayList<Pref>();
						flag=false;
					}
					u=u1<u2?u1:u2;
					
				}
					
			}*/
			//out.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}




class Pref{
	public String uID;
	public String iID;
	public String score;
}