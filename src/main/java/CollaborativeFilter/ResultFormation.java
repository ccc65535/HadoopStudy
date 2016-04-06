package CollaborativeFilter;
import java.io.*;
import java.nio.file.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class ResultFormation {
	private String inPath,outPath;

	public String getInPath() {
		return inPath;
	}

	public void setInPath(String inPath) {
		this.inPath = inPath;
	}

	public String getOutPath() {
		return outPath;
	}

	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}

	void formation() throws Exception{
		BufferedReader inFile=new BufferedReader(new FileReader(inPath));	
		BufferedWriter outFile=new BufferedWriter(new FileWriter(outPath));
		String line;
		String uID;
		String []prefs;
		
		while((line=inFile.readLine())!=null){
			uID=(line.split("\t"))[0];
			prefs=(line.substring(line.indexOf('[')+1, line.indexOf(']'))).split(",");
			
			for(int i=0;i<prefs.length;i++){
				outFile.write(uID+","+prefs[i].substring(0,prefs[i].indexOf(':'))+","+prefs[i].substring(prefs[i].indexOf(':')+1,prefs[i].length()));
				outFile.newLine();
			}
			//line.toString();
		
		}	
		inFile.close();
		outFile.close();
	}
	
	public void formationHDFS(Configuration conf) throws Exception{
		Path in=new Path(inPath);
		Path out=new Path(outPath);
		org.apache.hadoop.fs.FileSystem hdfs=in.getFileSystem(conf);
		
		InputStream in1=hdfs.open(in);
		OutputStream out1=hdfs.create(out);
		BufferedReader inFile=new BufferedReader(new InputStreamReader(in1));	
		BufferedWriter outFile=new BufferedWriter(new OutputStreamWriter(out1));
		String line;
		String uID;
		String []prefs;
		
		while((line=inFile.readLine())!=null){
			uID=(line.split("\t"))[0];
			prefs=(line.substring(line.indexOf('[')+1, line.indexOf(']'))).split(",");
			
			for(int i=0;i<prefs.length;i++){
				outFile.write(uID+","+prefs[i].substring(0,prefs[i].indexOf(':'))+","+prefs[i].substring(prefs[i].indexOf(':')+1,prefs[i].length()));
				outFile.newLine();
			}
			//line.toString();
		
		}	
		inFile.close();
		outFile.close();
	}
	
	public static void main(String args[]){
		try {
			ResultFormation rf=new ResultFormation();
			//rf.inPath="E:\\RATE\\formal1\\result";
			//rf.outPath="E:\\RATE\\formal1\\result-fm.txt";
			//rf.inPath="E:\\RATE\\test2\\itemBasedRecommend.txt";
			//rf.inPath="E:\\RATE\\movielens\\part-r-00000";
			/*rf.inPath="/project/data/cluster/part-r-00000";
			rf.outPath="/project/data/cluster/final";
			rf.formation();*/
			String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
			rf.inPath=HDFS+"/CF/imp/output/part-r-00000";
			rf.outPath=HDFS+"/CF/final";
			
			Configuration conf=new Configuration();
			conf.addResource(new Path("./bin/core-site.xml"));
			conf.addResource(new Path("./bin/hdfs-site.xml"));
			
			rf.formationHDFS(conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
