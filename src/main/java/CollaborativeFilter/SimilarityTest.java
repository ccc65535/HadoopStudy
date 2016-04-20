package CollaborativeFilter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystem;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.eval.*;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.AdjustedCosineSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.RandomUtils;


public class SimilarityTest {

	public static String localTempFile="/home/hadoop/Desktop/200/result-standard";
	public static void main(String args[]){
		try
		{
			FileDataModel model=new FileDataModel(new File("/home/hadoop/Desktop/200/result-imp"));
			
			//PearsonCorrelationSimilarity similarity1=new PearsonCorrelationSimilarity(model);
			//AdjustedCosineSimilarity similarity2=new AdjustedCosineSimilarity(model);
			//UncenteredCosineSimilarity similarity3=new UncenteredCosineSimilarity(model);
			
			
			/*BufferedWriter fout1=new BufferedWriter(new FileWriter("./dataFiles/5users/user-PearsonCorrelationSimilarity.txt"));
			for(int i=1;i<=5;i++){
				for(int j=i+1;j<=5;j++){
					System.out.print("S"+i+j+":"+similarity1.userSimilarity(i, j)+",");
					fout1.write("S"+i+j+":"+similarity1.userSimilarity(i, j)+",");
				}
			}
			
			fout1.close();*/
			
			//BufferedWriter fout2=new BufferedWriter(new FileWriter("./dataFiles/5users/item-AdjustedCosineSimilarity.txt"));
			/*BufferedWriter fout2=new BufferedWriter(new FileWriter("./dataFiles/mathTest/item-AdjustedCosineSimilarity.txt"));
			LongPrimitiveIterator items=model.getItemIDs();
			 while(items.hasNext()){
	            	long item=items.next();
	            	long [] simItems=similarity2.allSimilarItemIDs(item);
	            	System.out.print(item+" :");
	            	fout2.write(item+" :");
	            	for(int i=0;i<simItems.length;i++){
	            		System.out.print("["+simItems[i]+":"+similarity2.itemSimilarity(item, simItems[i])+"]  ");
	            		fout2.write("["+simItems[i]+":"+similarity2.itemSimilarity(item, simItems[i])+"]  ");
	            	}
	            	System.out.println("");
	            	fout2.newLine();
		        }
			 fout2.close();*/
			 
			//System.out.print("S(52,69)="+similarity3.itemSimilarity(52, 69));
			/*BufferedWriter fout=new BufferedWriter(new FileWriter("./dataFiles/mathTest/itemSimilarity.txt"));
			LongPrimitiveIterator items=model.getItemIDs();
			 while(items.hasNext()){
	            	long item=items.next();
	            	long [] simItems=similarity3.allSimilarItemIDs(item);
	            	System.out.print(item+" :");
	            	fout.write(item+" :");
	            	for(int i=0;i<simItems.length;i++){
	            		System.out.print("["+simItems[i]+":"+similarity3.itemSimilarity(item, simItems[i])+"]  ");
	            		fout.write("["+simItems[i]+":"+similarity3.itemSimilarity(item, simItems[i])+"]  ");
	            	}
	            	System.out.println("");
	            	fout.newLine();
		        }
			 fout.close();*/
			

			 
			 
			 
			 
			 

			// UserNeighborhood nbh = new NearestNUserNeighborhood(5, similarity1, model);
			// Recommender rec1 = new GenericUserBasedRecommender(model, nbh, similarity1);  
			 LongPrimitiveIterator list=model.getUserIDs();
			 Long user;
			 
			 
			 
			/* BufferedWriter out1=new BufferedWriter(new FileWriter("E:/RATE/movielens/userBasedRecommend.txt"));
			 while(list.hasNext()){
				 user=list.next();
				 List<RecommendedItem> recItemList = rec1.recommend(user,10);
				 int n=0;
				 String line="";
				 //System.out.print(user+"\t");
				 //out1.write(user+"\t");
				 for(RecommendedItem item : recItemList) {  
					 if(n==0){
		        			//System.out.print("["+item.getItemID()+":"+item.getValue());
			        		line+="["+item.getItemID()+":"+item.getValue();
		        		}
		        		else{
			        		//System.out.print(","+item.getItemID()+":"+item.getValue());
			        		line+=","+item.getItemID()+":"+item.getValue();
		        		}
					 n++;
				 }
				 if(n!=0){
					 System.out.println(user+"\t"+line+"]");
					 out1.write(user+"\t"+line+"]");
					 out1.newLine();
				 }
			 }
			 out1.close();*/
			 
			 
			 //Recommender rec2 = new GenericItemBasedRecommender(model, similarity3);  
			 Recommender rec2 = new SVDRecommender(model,new SVDPlusPlusFactorizer(model,10,5));
			 list=model.getUserIDs();
			 //BufferedWriter out2=new BufferedWriter(new FileWriter("./dataFiles/mathTest/itemBasedRecommend.txt"));
			 BufferedWriter out2=new BufferedWriter(new FileWriter("/home/hadoop/Desktop/200/Recommend-svd-imp.txt"));
			 while(list.hasNext()){
				 user=list.next();
				 List<RecommendedItem> recItemList = rec2.recommend(user,10);
				 int n=0;
				 String line="";
				 //System.out.print(user+"\t");
				 //out1.write(user+"\t");
				 for(RecommendedItem item : recItemList) {  
					 if(n==0){
		        			//System.out.print("["+item.getItemID()+":"+item.getValue());
			        		line+="["+item.getItemID()+":"+item.getValue();
		        		}
		        		else{
			        		//System.out.print(","+item.getItemID()+":"+item.getValue());
			        		line+=","+item.getItemID()+":"+item.getValue();
		        		}
					 n++;
				 }
				 if(n!=0){
					 System.out.println(user+"\t"+line+"]");
					 out2.write(user+"\t"+line+"]");
					 out2.newLine();
				 }
			 }
			 out2.close();
			 //rec2.recommend(1, 10);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main_new(String args[]) throws Exception{
		try {
			String inputPath="/home/hadoop/Desktop/200/result-standard";
	        DataModel dataModel = new FileDataModel(new File(inputPath));
	        
	        RandomUtils.useTestSeed();
	        
  	        //RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
	        GenericRecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();  

	        //RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();  
	        /*RecommenderBuilder builder = new RecommenderBuilder() {  
	  
	            public Recommender buildRecommender(DataModel dataModel)  
	                    throws TasteException {  

	                ItemSimilarity itemSim = new PearsonCorrelationSimilarity(  
	                        dataModel);  
	                return new GenericItemBasedRecommender(dataModel, itemSim);  
	            }  
	        };  
	        RecommenderBuilder builder = new RecommenderBuilder() {  
	      	  
	            public Recommender buildRecommender(DataModel dataModel)  
	                    throws TasteException {  
	            	PearsonCorrelationSimilarity similarity1=new PearsonCorrelationSimilarity(dataModel);
	            	UserNeighborhood nbh = new NearestNUserNeighborhood(10, similarity1, dataModel);
	    			return new GenericUserBasedRecommender(dataModel, nbh, similarity1); 
	    
	            }  
	        };  */
	        RecommenderBuilder builder = new RecommenderBuilder() {  
	      	  
	            public Recommender buildRecommender(DataModel dataModel)  
	                    throws TasteException {  

	                return new SVDRecommender(dataModel,new SVDPlusPlusFactorizer(dataModel,10,5));
	            }  
	        };  
	       
	        IRStatistics stats = evaluator.evaluate(builder, null, dataModel,null,10,GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,0.9);  
	        System.out.printf("Recommender IR Evaluator: [Precision:%s,Recall:%s,F1:%s,FallOut:%s,nDCG:%s]\n", stats.getPrecision(), stats.getRecall(), stats.getF1Measure(), stats.getFallOut(), stats.getNormalizedDiscountedCumulativeGain());
	          
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void CFonHDFS(String inFile,String localtmp,String outPath,Configuration conf,int recomNums){
		try
		{
			Path inPath=new Path(inFile);
			org.apache.hadoop.fs.FileSystem fs=inPath.getFileSystem(conf);			
			Path localFile=new Path(localtmp);			
			File data=new File(localtmp);
			
			if(data.exists())
				data.delete();
			fs.copyToLocalFile(inPath, localFile);
			OutputStream ostream=fs.create(new Path(outPath));
			BufferedWriter out2=new BufferedWriter(new OutputStreamWriter(ostream));
			

			FileDataModel model=new FileDataModel(new File(localtmp));			
			PearsonCorrelationSimilarity similarity1=new PearsonCorrelationSimilarity(model);
			AdjustedCosineSimilarity similarity2=new AdjustedCosineSimilarity(model);
			UncenteredCosineSimilarity similarity3=new UncenteredCosineSimilarity(model);
			
			UserNeighborhood nbh = new NearestNUserNeighborhood(5, similarity1, model);
			Recommender rec1 = new GenericUserBasedRecommender(model, nbh, similarity1);  
			Recommender rec2 = new GenericItemBasedRecommender(model, similarity2);  
			
			LongPrimitiveIterator list=model.getUserIDs();
			Long user;
			list=model.getUserIDs();
			while(list.hasNext()){
				user=list.next();
				List<RecommendedItem> recItemList = rec2.recommend(user,recomNums);
				int n=0;
				String line="";
				for(RecommendedItem item : recItemList) {  
					if(n==0){
						line+="["+item.getItemID()+":"+item.getValue();
					}
					else{
						line+=","+item.getItemID()+":"+item.getValue();
					}					
					n++;
				}
				if(n!=0){
					//System.out.println(user+"\t"+line+"]");
					out2.write(user+"\t"+line+"]");
					out2.newLine();
				}
			}
			out2.close();	
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
