package CollaborativeFilter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;


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
	
	public static void main(String args[]){
		try {
			ResultFormation rf=new ResultFormation();
			//rf.inPath="E:\\RATE\\formal1\\result";
			//rf.outPath="E:\\RATE\\formal1\\result-fm.txt";
			//rf.inPath="E:\\RATE\\test2\\itemBasedRecommend.txt";
			//rf.inPath="E:\\RATE\\movielens\\part-r-00000";
			rf.inPath="/project/data/cluster/part-r-00000";
			rf.outPath="/project/data/cluster/final";
			rf.formation();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
