package Frame;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.RandomDivide;
import ImpScore.*;

public class DataPrepare {
	
	public static String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/beta-1";
	//public static ImplicitScore impScore=new ImplicitScore();
	//public static ScoreRegulation sRegualtion=new ScoreRegulation();
	//public static String inPath,outPath,temp;
	
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		conf.set("mapred.textoutputformat.separator",",");
		
		
		try {
			/*隐式评分
			 * ImplicitScore.input="hdfs://192.168.32.10:9000/RecommendSystem/valuePref/input/";
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore/scores";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore/outStatus";
			ImplicitScore.doScore(conf);*/
			
			
			for(int i=200;i<=1000;i+=200){
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore/scores/";
				ScoreRegulation.outPath=HDFS+"/Preprocess/ImpScore/"+i+"/result-standard";
				ScoreRegulation.outPath1=HDFS+"/Preprocess/ImpScore/"+i+"/result-nstandard";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore/"+i+"/result-ones";
				ScoreRegulation.Combine(conf,i);	//标准化评分
				//ScoreRegulation.Combine1(conf,i);	//未标准化评分
				ScoreRegulation.Combine2(conf,i);	//0-1评分
*/				
				RandomDivide div=new RandomDivide();
				div.setScale(0.1);
				div.setInPath(HDFS+"/Preprocess/ImpScore/"+i+"/result-standard");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/a/scale-"+i+"/trainSet-std");
				div.setExamplePath(HDFS+"/CollaborativeFilter/a/scale-"+i+"/example-std");
				div.divideHDFS(conf);
				
				div.setInPath(HDFS+"/Preprocess/ImpScore/"+i+"/result-ones");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/a/scale-"+i+"/trainSet-one");
				div.setExamplePath(HDFS+"/CollaborativeFilter/a/scale-"+i+"/example-one");
				div.divideHDFS(conf);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
