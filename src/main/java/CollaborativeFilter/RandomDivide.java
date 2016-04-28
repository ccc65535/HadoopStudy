package CollaborativeFilter;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class RandomDivide {
	private double scale;		//分割比例
	private String inPath;		//输入文件
	private String trainSetPath,examplePath;		//输出文件
	
	public double getScale() {
		return scale;
	}
	public void setScale(double scale) {
		this.scale = scale;
	}
	
	
	public String getInPath() {
		return inPath;
	}
	public void setInPath(String inPath) {
		this.inPath = inPath;
	}

		
	public String getTrainSetPath() {
		return trainSetPath;
	}
	public void setTrainSetPath(String tranSetPath) {
		this.trainSetPath = tranSetPath;
	}
	
	public String getExamplePath() {
		return examplePath;
	}
	public void setExamplePath(String examplePath) {
		this.examplePath = examplePath;
	}
	
	
	public void divide() throws Exception{//不用了
		int scaleSize,count=0,linePointer=0;
		int []nums;
		BufferedReader inFile=new BufferedReader(new FileReader(inPath));
		BufferedWriter tranFile=new BufferedWriter(new FileWriter(trainSetPath));
		BufferedWriter exampleFile=new BufferedWriter(new FileWriter(examplePath));
		
		
		while(inFile.readLine()!=null){
			count++;
		}
		
		scaleSize=(int)(count*scale);
		nums=new int[scaleSize];
		
		for(int k=0;k<scaleSize;k++){
			nums[k]=(int)(Math.random()*count);
		}
		sort(nums);

		
		
		String line;
		int s=0;
		inFile=new BufferedReader(new FileReader(inPath));
		while((line=inFile.readLine())!=null){
			if(linePointer==nums[s]){
				exampleFile.write(line);
				exampleFile.newLine();
				s++;
			}
			else if(linePointer<nums[s]){
				tranFile.write(line);
				tranFile.newLine();
				linePointer++;
			}
			else
				s++;
		}
		
		inFile.close();
		tranFile.close();
		exampleFile.close();
		
	}
	
	public void divide1() throws Exception{//本地文件用这个分割
		int scaleSize,count=0,linePointer=0;
		int []nums;
		BufferedReader inFile=new BufferedReader(new FileReader(inPath));
		BufferedWriter tranFile=new BufferedWriter(new FileWriter(trainSetPath));
		BufferedWriter exampleFile=new BufferedWriter(new FileWriter(examplePath));
		
		List<String> lines=new ArrayList<String>();
		String line;
		while((line=inFile.readLine())!=null){
			count++;
			lines.add(line);
		}
		inFile.close();
		
		scaleSize=(int)(count*scale);
		nums=new int[scaleSize];
		
		for(int k=0;k<scaleSize;k++){
			nums[k]=(int)(Math.random()*count);
		}
		sort(nums);
		
		
		
		List<String> tempLine=new ArrayList<String>();
		for(int i=0;i<nums.length;i++){
			tempLine.add(lines.remove(nums[i]%lines.size()));
		}
		
		tempLine.sort(new Comparator<String>(){
			public int compare(String a,String b){
				long au=Long.parseLong(a.split(",")[0].trim());
				long bu=Long.parseLong(b.split(",")[0].trim());
				if(au>bu)
					return 1;
				else if(au<bu)
					return -1;
				else
					return 0;
			}
		});
		
		
		
		for(int i=0;i<tempLine.size();i++){
			exampleFile.write(tempLine.get(i));
			exampleFile.newLine();
		}
		for(int i=0;i<lines.size();i++){
			tranFile.write(lines.get(i));
			tranFile.newLine();
		}
		
		tranFile.close();
		exampleFile.close();
		
	}
	
	public void divideHDFS(Configuration conf) throws Exception{//HDFS文件用这个分割
		int scaleSize,count=0,linePointer=0;
		int []nums;
		
		Path srcPath=new Path(inPath);
		Path outPath1=new Path(trainSetPath);
		Path outPath2=new Path(examplePath);
		
		org.apache.hadoop.fs.FileSystem hdfs=srcPath.getFileSystem(conf);
		InputStream in1=hdfs.open(srcPath);
		OutputStream trainSet=hdfs.create(outPath1);
		OutputStream example=hdfs.create(outPath2);
		
		BufferedReader inFile=new BufferedReader(new InputStreamReader(in1));
		BufferedWriter tranFile=new BufferedWriter(new OutputStreamWriter(trainSet));
		BufferedWriter exampleFile=new BufferedWriter(new OutputStreamWriter(example));
		
		List<String> lines=new ArrayList<String>();
		String line;
		while((line=inFile.readLine())!=null){
			count++;
			lines.add(line);
		}
		inFile.close();
		
		scaleSize=(int)(count*scale);
		nums=new int[scaleSize];
		
		for(int k=0;k<scaleSize;k++){
			nums[k]=(int)(Math.random()*count);
		}
		sort(nums);
		
		
		
		List<String> tempLine=new ArrayList<String>();
		for(int i=0;i<nums.length;i++){
			tempLine.add(lines.remove(nums[i]%lines.size()));
		}
		
		tempLine.sort(new Comparator<String>(){
			public int compare(String a,String b){
				long au=Long.parseLong(a.split(",")[0].trim());
				long bu=Long.parseLong(b.split(",")[0].trim());
				if(au>bu)
					return 1;
				else if(au<bu)
					return -1;
				else
					return 0;
			}
		});
		
		
		
		for(int i=0;i<tempLine.size();i++){
			exampleFile.write(tempLine.get(i));
			exampleFile.newLine();
		}
		for(int i=0;i<lines.size();i++){
			tranFile.write(lines.get(i));
			tranFile.newLine();
		}
		
		tranFile.close();
		exampleFile.close();
		
	}
	
	
	
	public void divideHDFS_byModel(Configuration conf,String model,String score,String outFile) throws Exception{//HDFS文件用这个分割

		Path modelPath=new Path(model);
		Path scPath=new Path(score);
		Path outPath=new Path(outFile);

		
		org.apache.hadoop.fs.FileSystem hdfs=modelPath.getFileSystem(conf);
		InputStream modelIn=hdfs.open(modelPath);
		InputStream scoreIn=hdfs.open(scPath);
		OutputStream outStream=hdfs.create(outPath);
		List<String> scList=new ArrayList<String>();
		List<String> mdList=new ArrayList<String>();

		BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(outStream));
		
		loadAndSort(scList,scoreIn);
		loadAndSort(mdList,modelIn);
		
		String mdLine;
		String scLine;


		
		int i=0,j=0;;
		while(i<scList.size()&&j<mdList.size()){
			scLine=scList.get(i);
			mdLine=mdList.get(j);
			if(compare(scLine,mdLine)==0){
				writer.write(scLine);
				writer.newLine();
				i++;
				j++;
			}
			else if(compare(scList.get(i),mdLine)<0){
				i++;
			}
			else{
				System.out.println("Files don't macth.At:"+scLine+"<>"+mdLine);
				break;
			}
		}
		writer.close();
		
	}
	
	void loadAndSort(List<String> list,InputStream inStream) throws IOException{

		BufferedReader reader=new BufferedReader(new InputStreamReader(inStream));		
		String line;
		while((line=reader.readLine())!=null){
			list.add(line);
		}
		list.sort(new Comparator<String>(){
			public int compare(String o1, String o2) {
				String U1,U2;
				U1=o1.substring(0,o1.indexOf(','));
				U2=o2.substring(0,o2.indexOf(','));
				
				Long u1,u2;
				u1=Long.parseLong(U1);
				u2=Long.parseLong(U2);
				
				if(u1>u2)
					return 1;
				else if(u1<u2)
					return -1;
				else 
					return 0;
			}
			
		});
		reader.close();
	}
	
	int compare(String line1,String line2){
		String U1,U2,I1,I2;
		U1=line1.substring(0,line1.indexOf(','));
		U2=line2.substring(0,line2.indexOf(','));
		I1=line1.substring(line1.indexOf(',')+1);
		I2=line2.substring(line2.indexOf(',')+1);
		
		if(I1.contains(","))
			I1=I1.substring(0, I1.indexOf(","));			

		if(I2.contains(","))
			I2=I2.substring(0, I2.indexOf(","));
		
		Long u1=Long.parseLong(U1);
		Long u2=Long.parseLong(U2);
		
		if(u1>u2)
			return 1;
		else if(u1<u2)
			return -1;
		else
			return 0;

	}
	
	void sort(int []x){
		int a;
		for(int k=0;k<x.length-1;k++){
			for(int j=k+1;j<x.length;j++){
				if(x[k]>x[j]){
					a=x[k];
					x[k]=x[j];
					x[j]=a;
				}
			}
		}
	}
	
	boolean contains(int []x,int add,int size){
		for(int k=0;k<x.length;k++){
			if(add==x[k])
				return true;
		}
		return false;
	}
	
	
	public static void main(String args[]){
		
		/*try{
			//File preFile=new File("E:\\RATE\\formal1\\RateData.txt");
			//File resFile=new File("E:\\RATE\\formal1\\result.txt");
			File preFile=new File("/project/data/one/result-alpha");
			File resFile=new File("/project/data/one/");
			BufferedReader in=new BufferedReader(new FileReader(preFile));
			BufferedWriter out=new BufferedWriter(new FileWriter(resFile));
			String line;
			String uID,iID,score;
			String []contents;
		
			if(preFile.exists()){
				if(resFile.exists()){
					resFile.delete();
					resFile.createNewFile();
				}
				while((line=in.readLine())!=null){
					uID=line.substring(0, 1);
					iID=line.substring(1, 5).trim();
					score=line.substring(7, 15);
					out.write(uID+","+iID+","+score);
					contents=line.split(",");
					out.write(contents[0].trim()+","+contents[1].trim()+","+contents[2].trim());
					out.newLine();
					contents=line.split("\t");
					out.write(contents[0].trim()+","+contents[1].trim()+","+contents[2].trim());
					out.newLine();
				}
			}
			out.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}*/
		
		
		
		
		try {
			RandomDivide rd=new RandomDivide();
			rd.scale=0.1;
			/*rd.inPath="/project/data/nzz/result-alpha-1";
			rd.trainSetPath="/project/data/nzz/trainSet-1.txt";
			rd.examplePath="/project/data/nzz/example.txt";
			rd.divide1();*/
			String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
			rd.inPath=HDFS+"/valuePref-new2/result-alpha";
			rd.trainSetPath=HDFS+"/trainSet.txt";
			rd.examplePath=HDFS+"/example.txt";
			Configuration conf=new Configuration();
			conf.addResource(new Path("./bin/core-site.xml"));
			conf.addResource(new Path("./bin/hdfs-site.xml"));
			rd.divideHDFS(conf);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
