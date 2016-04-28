package Frame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.CFtest;
import CollaborativeFilter.RandomDivide;

public class CF_mr {
	public static String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/beta-1";
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		
		String trainSet,example,recommend,tmp;
		String recommend_fm,eval;
		
		try {
			for(int i=200;i<=1200;i+=200){
				
				/*System.out.println("--std|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-std";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-std";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-std";
				tmp=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/tmp/std/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-fm-std";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/eval-std";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				
				////////////////////////////////////////////////////////////////////////
				System.out.println("--LR|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-newLR";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-newLR";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-newLR";
				tmp=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/tmp/newLR/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-fm-newLR";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/eval-newLR";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);*/
				////////////////////////////////
				System.out.println("--origin|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-origin";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-origin";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-origin";
				tmp=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/tmp/origin/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-fm-origin";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/eval-origin";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				//////////////////////////////////////////////
				System.out.println("--std|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-std";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-std";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-std";
				tmp=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/tmp/std/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-fm-std";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/eval-std";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				
				//////////////////////////////////////////////
				System.out.println("--mixedTime-480c10|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/trainSet-mixedTime-480c10";
				example=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/example-mixedTime-480c10";
				recommend=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-mixedTime-480c10";
				tmp=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/tmp/mixedTime-480c10/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/recommend-fm-mixedTime-480c10";
				eval=HDFS+"/CollaborativeFilter/6levels/scale-"+i+"/re-mr/eval-mixedTime-480c10";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				
			
				

			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
