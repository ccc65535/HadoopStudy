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
			//隐式评分
			ImplicitScore.input="hdfs://192.168.32.10:9000/RecommendSystem/valuePref/input/";
			/*ImplicitScore.output=HDFS+"/Preprocess/ImpScore-new/scores-newLR";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-new/outStatus-newLR";
			ImplicitScore.doScore_LR_2(conf);*/
			
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-new/scores-std";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-new/outStatus-std";
			ImplicitScore.doScore_std(conf);
			
			/*ImplicitScore.output=HDFS+"/Preprocess/ImpScore-new/scores-std_origin";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-new/outStatus-origin";
			ImplicitScore.doScore_origin(conf);*/
			
			/*ImplicitScore.input="hdfs://192.168.32.10:9000/RecommendSystem/valuePref/input/";
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-new/scores-imp";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-new/outStatus-imp";
			ImplicitScore.doScore_imp(conf);*/
			

			
			
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-new/scores-LR_Line";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-new/outStatus-LR_Line";
			ImplicitScore.doScore_LR_Line(conf);
			
			

			
			for(int i=200;i<=1200;i+=200){
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-std/";
				//ScoreRegulation.outPath=HDFS+"/Preprocess/ImpScore/"+i+"/result-LR";
				//ScoreRegulation.outPath1=HDFS+"/Preprocess/ImpScore/"+i+"/result-nstandard";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-std";
				//ScoreRegulation.Combine(conf,i);	//标准化评分
				//ScoreRegulation.Combine1(conf,i);	//未标准化评分
				ScoreRegulation.Combine2(conf,i);	//0-1评分*/
				
				
				ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-std/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-std";
				ScoreRegulation.Combine2(conf,i);
				
				ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-LR_Line/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-LR_Line";
				ScoreRegulation.Combine2(conf,i);

				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-newLR/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-newLR";
				ScoreRegulation.Combine2(conf,i);*/		
				
				//RandomDivide div=new RandomDivide();
				//div.setScale(0.1);
				/*div.setInPath(HDFS+"/Preprocess/ImpScore-new/"+i+"/result-imp");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-imp");
				div.setExamplePath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-imp");
				div.divideHDFS(conf);*/
				/*div.setInPath(HDFS+"/Preprocess/ImpScore-new/"+i+"/result-newLR");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/5levels/scale-"+i+"/trainSet-newLR");
				div.setExamplePath(HDFS+"/CollaborativeFilter/5levels/scale-"+i+"/example-newLR");
				div.divideHDFS(conf);*/
				/*div.divideHDFS_byModel(conf, HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-imp", HDFS+"/Preprocess/ImpScore-new/"+i+"/result-newLR", HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-newLR");
				div.divideHDFS_byModel(conf, HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-imp", HDFS+"/Preprocess/ImpScore-new/"+i+"/result-newLR", HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-newLR");
				
				div.divideHDFS_byModel(conf, HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-imp", HDFS+"/Preprocess/ImpScore-new/"+i+"/result-origin", HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-origin");
				div.divideHDFS_byModel(conf, HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-imp", HDFS+"/Preprocess/ImpScore-new/"+i+"/result-origin", HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-origin");*/
				
				
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
