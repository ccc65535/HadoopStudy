package Frame;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.RandomDivide;
import ImpScore.*;

public class DataPrepare {
	
	public static String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/beta-1";
	//public static ImplicitScore impScore=new ImplicitScore();
	//public static ScoreRegulation sRegualtion=new ScoreRegulation();
	//public static String inPath,outPath,temp;
	
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		conf.set("mapred.textoutputformat.separator",",");
		long startT,endT;
		
		try {
			//隐式评分
			ImplicitScore.input="hdfs://202.113.127.208:9000/RecommendSystem/valuePref/input/";

			/*startT=System.currentTimeMillis();
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-5th/scores-origin";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-5th/outStatus-origin";
			ImplicitScore.doScore_origin(conf);
			endT=System.currentTimeMillis();
			System.out.println("origin:"+(endT-startT)+"ms");*/
			

			
			startT=System.currentTimeMillis();
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-5th/scores-mixedTime-test1";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-5th/outStatus-mixedTime-test1";
			ImplicitScore.doScore_mixedTime(conf);
			endT=System.currentTimeMillis();
			System.out.println("mixedTime:"+(endT-startT)+"ms");
			
			/*startT=System.currentTimeMillis();
			ImplicitScore.input="hdfs://202.113.127.208:9000/RecommendSystem/valuePref/input/";
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-4th/scores-std-0";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-4th/outStatus-std-0";
			ImplicitScore.doScore_std_0(conf);
			endT=System.currentTimeMillis();
			System.out.println("std-0:"+(endT-startT)+"ms");*/
			
			/*startT=System.currentTimeMillis();
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-5th/scores-std";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-5th/outStatus-std";
			ImplicitScore.doScore_std(conf);
			endT=System.currentTimeMillis();
			System.out.println("std:"+(endT-startT)+"ms");*/
			
			
			
			/*startT=System.currentTimeMillis();
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-4th/scores-line";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-4th/outStatus-line";
			ImplicitScore.doScore_LR(conf);
			endT=System.currentTimeMillis();
			System.out.println("line:"+(endT-startT)+"ms");
			
			
			startT=System.currentTimeMillis();
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-4th/scores-square";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-4th/outStatus-square";
			ImplicitScore.doScore_byTime(conf);
			endT=System.currentTimeMillis();
			System.out.println("square:"+(endT-startT)+"ms");
			
			
			
			startT=System.currentTimeMillis();
			ImplicitScore.input="hdfs://202.113.127.208:9000/RecommendSystem/valuePref/input/";
			ImplicitScore.output=HDFS+"/Preprocess/ImpScore-4th/scores-imp";
			ImplicitScore.status=HDFS+"/Preprocess/ImpScore-4th/outStatus-imp";
			ImplicitScore.doScore_imp(conf);
			endT=System.currentTimeMillis();
			System.out.println("imp:"+(endT-startT)+"ms");*/
			
		
			

			
			for(int i=200;i<=1200;i+=200){
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-std/";
				//ScoreRegulation.outPath=HDFS+"/Preprocess/ImpScore/"+i+"/result-LR";
				//ScoreRegulation.outPath1=HDFS+"/Preprocess/ImpScore/"+i+"/result-nstandard";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-std";
				//ScoreRegulation.Combine(conf,i);	//标准化评分
				//ScoreRegulation.Combine1(conf,i);	//未标准化评分
				ScoreRegulation.Combine2(conf,i);	//0-1评分*/
				
				
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-5th/scores-origin/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-5th/"+i+"/result-origin";
				ScoreRegulation.Combine2(conf,i);*/
				
				ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-5th/scores-mixedTime-test1/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-5th/"+i+"/result-mixedTime-test1";
				ScoreRegulation.Combine2(conf,i);

				
				
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-5th/scores-std/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-5th/"+i+"/result-std";
				ScoreRegulation.Combine2(conf,i);*/
				
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-4th/scores-std-0/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-4th/"+i+"/result-std-0";
				ScoreRegulation.Combine2(conf,i);*/
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-4th/scores-square/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-4th/"+i+"/result-square";
				ScoreRegulation.Combine2(conf,i);
				
				ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-4th/scores-line/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-4th/"+i+"/result-line";
				ScoreRegulation.Combine2(conf,i);
				*/


				
				
				/*ScoreRegulation.rootPath=HDFS+"/Preprocess/ImpScore-new/scores-newLR/";
				ScoreRegulation.outPath2=HDFS+"/Preprocess/ImpScore-new/"+i+"/result-newLR";
				ScoreRegulation.Combine2(conf,i);*/		
				
				RandomDivide div=new RandomDivide();
				div.setScale(0.1);
				
				/*div.setInPath(HDFS+"/Preprocess/ImpScore-5th/"+i+"/result-std");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-std");
				div.setExamplePath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-std");
				div.divideHDFS(conf);
				
				div.setInPath(HDFS+"/Preprocess/ImpScore-5th/"+i+"/result-origin");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-origin");
				div.setExamplePath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-origin");
				div.divideHDFS(conf);
				
				div.setInPath(HDFS+"/Preprocess/ImpScore-4th/"+i+"/result-mixedTime-480c10");
				div.setTrainSetPath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-mixedTime-480c10");
				div.setExamplePath(HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-mixedTime-480c10");
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
