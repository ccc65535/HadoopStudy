package Frame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.CFtest;
import CollaborativeFilter.RandomDivide;

public class CF_mr {
	public static String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/beta-1";
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		
		String trainSet,example,recommend,tmp;
		String recommend_fm,eval;
		
		try {
			for(int i=200;i<=1000;i+=200){
				
				System.out.println("--std|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/a/scale-"+i+"/trainSet-std";
				example=HDFS+"/CollaborativeFilter/a/scale-"+i+"/example-std";
				recommend=HDFS+"/CollaborativeFilter/a/scale-"+i+"/recommend-std";
				tmp=HDFS+"/CollaborativeFilter/a/scale-"+i+"/tmp/std/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/a/scale-"+i+"/recommend-fm-std";
				eval=HDFS+"/CollaborativeFilter/a/scale-"+i+"/eval-std";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				
				////////////////////////////////////////////////////////////////////////
				System.out.println("--one|scale:"+i+"------------------");
				trainSet=HDFS+"/CollaborativeFilter/a/scale-"+i+"/trainSet-one";
				example=HDFS+"/CollaborativeFilter/a/scale-"+i+"/example-one";
				recommend=HDFS+"/CollaborativeFilter/a/scale-"+i+"/recommend-one";
				tmp=HDFS+"/CollaborativeFilter/a/scale-"+i+"/tmp/one/";
				CFtest.recommend(conf, trainSet, recommend, tmp, 10);
				
				recommend_fm=HDFS+"/CollaborativeFilter/a/scale-"+i+"/recommend-fm-ones";
				eval=HDFS+"/CollaborativeFilter/a/scale-"+i+"/eval-ones";
				DataAnalyse.analyse(conf, recommend+"/part-r-00000", recommend_fm, example, eval);
				
			
				

			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
