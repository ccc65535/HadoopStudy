package CollaborativeFilter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.AdjustedCosineSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;


public class SimilarityTest {

	public static String localTempFile="/home/hadoop/temp/infile";
	public static void main_old(String args[]){
		try
		{
			FileDataModel model=new FileDataModel(new File("E:\\RATE\\movielens\\trainSet.txt"));
			
			PearsonCorrelationSimilarity similarity1=new PearsonCorrelationSimilarity(model);
			AdjustedCosineSimilarity similarity2=new AdjustedCosineSimilarity(model);
			UncenteredCosineSimilarity similarity3=new UncenteredCosineSimilarity(model);
			
			
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
			

			 
			 
			 
			 
			 

			 UserNeighborhood nbh = new NearestNUserNeighborhood(5, similarity1, model);
			 Recommender rec1 = new GenericUserBasedRecommender(model, nbh, similarity1);  
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
			 
			 
			 Recommender rec2 = new GenericItemBasedRecommender(model, similarity3);  
			 list=model.getUserIDs();
			 //BufferedWriter out2=new BufferedWriter(new FileWriter("./dataFiles/mathTest/itemBasedRecommend.txt"));
			 BufferedWriter out2=new BufferedWriter(new FileWriter("E:/RATE/movielens/itemBasedRecommend.txt"));
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

	public static void main(String args[]){
		try
		{
			/*String HDFS="hdfs://192.168.32.10:9000/RecommendSystem/";
			Configuration conf=new Configuration();
			conf.addResource(new Path("./bin/core-site.xml"));
			conf.addResource(new Path("./bin/hdfs-site.xml"));
			String inputFile=HDFS+"CF/imp/in/trainSet.txt";
			Path inPath=new Path(inputFile);
			org.apache.hadoop.fs.FileSystem fs=inPath.getFileSystem(conf);
			
			Path localFile=new Path(localTempFile);
			
			File data=new File(localTempFile);
			if(data.exists())
				data.delete();
			
			fs.copyToLocalFile(inPath, localFile);*/
		
			
			
			FileDataModel model=new FileDataModel(new File(localTempFile));
			
			PearsonCorrelationSimilarity similarity1=new PearsonCorrelationSimilarity(model);
			AdjustedCosineSimilarity similarity2=new AdjustedCosineSimilarity(model);
			UncenteredCosineSimilarity similarity3=new UncenteredCosineSimilarity(model);
			
			LongPrimitiveIterator list=model.getUserIDs();
			Long user;		 
			 

			 /*UserNeighborhood nbh = new NearestNUserNeighborhood(5, similarity1, model);
			 Recommender rec1 = new GenericUserBasedRecommender(model, nbh, similarity1);  
			 BufferedWriter out1=new BufferedWriter(new FileWriter("E:/RATE/movielens/userBasedRecommend.txt"));
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
			 
			 
			 Recommender rec2 = new GenericItemBasedRecommender(model, similarity3);  
			 list=model.getUserIDs();
			// BufferedWriter out2=new BufferedWriter(new FileWriter("E:/RATE/movielens/itemBasedRecommend.txt"));
			 while(list.hasNext()){
				 user=list.next();
				 List<RecommendedItem> recItemList = rec2.recommend(user,3);
				 int n=0;
				 String line="";
				 //System.out.print(user+"\t");
				 //out1.write(user+"\t");
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
					 System.out.println(user+"\t"+line+"]");
					 //out2.write(user+"\t"+line+"]");
					// out2.newLine();
				 }
			 }
			 //out2.close();
			 //rec2.recommend(1, 10);
			 
			 
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
