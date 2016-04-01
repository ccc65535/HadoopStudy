package CollaborativeFilter;
import java.io.*;
import java.util.*;


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
	
	public void divide1() throws Exception{//用着个分割
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
			//File resFile=new File("E:\\RATE\\formal1\\��ʽ����.txt");
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
			rd.inPath="/project/data/nzz/result-alpha-1";
			rd.trainSetPath="/project/data/nzz/trainSet-1.txt";
			rd.examplePath="/project/data/nzz/example.txt";
			rd.divide1();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
