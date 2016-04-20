package Frame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.CFtest;
import CollaborativeFilter.SimilarityTest;

public class CF_single {
public static String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/beta-1";
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		
		String trainSet,example,recommend,tmp;
		String recommend_fm,eval;
		
		try {
			for(int i=200;i<=1000;i+=200){
				/*System.out.println("--origin|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-origin";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-origin";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-origin";
				SimilarityTest.CFonHDFS(trainSet, "/project/data/temp/trainSet-origin", recommend, conf,10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-fm-origin";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/eval-origin";
				DataAnalyse.analyse(conf, recommend, recommend_fm, example, eval);
				////////////////////////////////////////////////////////////////////////
				System.out.println("--std|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-std";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-std";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-std";
				SimilarityTest.CFonHDFS(trainSet, "/project/data/temp/trainSet-std", recommend, conf,10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-fm-std";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/eval-std";
				DataAnalyse.analyse(conf, recommend, recommend_fm, example, eval);*/
				////////////////////////////////////////////////////////////////////////
				/*System.out.println("--std-0|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-std-0";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-std-0";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-std-0";
				SimilarityTest.CFonHDFS(trainSet, "/project/data/temp/", recommend, conf,10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-fm-std-0";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/eval-std-0";
				DataAnalyse.analyse(conf, recommend, recommend_fm, example, eval);
				////////////////////////////////////////////////////////////////////////
				System.out.println("--LR|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-LR";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-LR";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-LR";
				SimilarityTest.CFonHDFS(trainSet, "/project/data/temp/", recommend, conf,10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-fm-LR";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/eval-LR";
				DataAnalyse.analyse(conf, recommend, recommend_fm, example, eval);*/
				////////////////////////////////////////////////////////////////////////
				System.out.println("--newLR|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-newLR";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-newLR";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-newLR";
				SimilarityTest.CFonHDFS(trainSet, "/project/data/temp/trainSet-newLR", recommend, conf,10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/recommend-fm-newLR";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re/eval-newLR";
				DataAnalyse.analyse(conf, recommend, recommend_fm, example, eval);

			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
