package CollaborativeFilter;

import java.io.*;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.AdjustedCosineSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

public class Evaluate_Mahout {
	public static void main(String[] args) throws Exception {  
        // TODO Auto-generated method stub  

	  	//String inputPath="/home/hadoop/Desktop/files/400/result-origin";
		for(int i=200;i<=1200;i+=200){
			String inputPath="";
			
			/*inputPath="/home/Hadoop/桌面/files/"+i+"/result-std-logSize";
			System.out.println("Level:"+i+",std-LogSize:");
			value(inputPath);*/
			/*inputPath="/home/Hadoop/桌面/files/"+i+"/result-origin";
			System.out.println("origin:");
			value(inputPath);*/
			
			inputPath="/home/Hadoop/桌面/files/"+i+"/result-mixedTime-test1";
			System.out.println("level"+i+", mixedTime:");
			value(inputPath);
			System.out.println();
			
			
			/*inputPath="/home/Hadoop/桌面/files/"+i+"/result-std-0";
			System.out.println("std-0:");
			value(inputPath);
			System.out.println();
			
			inputPath="/home/Hadoop/桌面/files/"+i+"/result-std";
			System.out.println("Level:"+i+",std:");
			value(inputPath);
			
			inputPath="/home/Hadoop/桌面/files/"+i+"/result-square";
			System.out.println("Level:"+i+",square:");
			value(inputPath);
			
			inputPath="/home/Hadoop/桌面/files/"+i+"/result-line";
			System.out.println("Level:"+i+",line:");
			value(inputPath);
			*/
			
		}
         
    }  
	
	public static void value(String inPath) throws Exception{
		 RandomUtils.useTestSeed();  

	        DataModel dataModel = new FileDataModel(new File(inPath));  

	        
	        GenericRecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();  
	        RecommenderEvaluator evaluator1 = new RMSRecommenderEvaluator();
	        //RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();  
	        RecommenderBuilder builder = new RecommenderBuilder() {  
	  
	            public Recommender buildRecommender(DataModel dataModel)  
	                    throws TasteException {  

	                /*ItemSimilarity itemSim = new AdjustedCosineSimilarity(dataModel);  
	                return new GenericItemBasedRecommender(dataModel, itemSim);*/
	            	UserSimilarity userSim=new PearsonCorrelationSimilarity(dataModel);
	            	UserNeighborhood nbh = new NearestNUserNeighborhood(10, userSim, dataModel);
	    			return new GenericUserBasedRecommender(dataModel, nbh, userSim);
	            }  
	        };  
	       
	        IRStatistics stats = evaluator.evaluate(builder, null, dataModel,null,10,GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,1);  
	        System.out.printf("Recommender IR Evaluator: [Precision:%s,Recall:%s,F1:%s,FallOut:%s,nDCG:%s]\n", stats.getPrecision(), stats.getRecall(), stats.getF1Measure(), stats.getFallOut(), stats.getNormalizedDiscountedCumulativeGain());
	        System.out.printf("RSME:%f\n",evaluator1.evaluate(builder, null, dataModel, 0.9, 0.1));
	}
}
