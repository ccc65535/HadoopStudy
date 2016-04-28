package CollaborativeFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import Frame.DataAnalyse;

public class CFtest {
	private static final String HDFS = "hdfs://202.113.127.208:9000";
	public static void main(String[] args) throws Exception {
		/*String inPath = HDFS + "/RecommendSystem/CF/imp/in/trainSet-2.txt";
	    String inFile = inPath + "";	    
	    String outPath = HDFS + "/RecommendSystem/CF/imp/output-2";
	    String outFile = outPath + "/part-r-00000";	    
	    String tmpPath = HDFS + "/RecommendSystem/CF/tmp/"+System.currentTimeMillis();
		String inPath = HDFS + "/RecommendSystem/CF/ClusterBased/in/c5Set.txt";  
	    String outPath = HDFS + "/RecommendSystem/CF/ClusterBased/output5";    
	    String tmpPath = HDFS + "/RecommendSystem/CF/ClusterBased/tmp/"+System.currentTimeMillis();
*/
		String inPath = HDFS + "/RecommendSystem/CF/test-a/trainSet.txt";  
	    String outPath = HDFS + "/RecommendSystem/CF/test-a/out/";
	    String outFile=outPath+"part-r-00000";
	    String tmpPath = HDFS + "/RecommendSystem/CF/ClusterBased/tmp/"+System.currentTimeMillis();

        //Configuration conf = new Configuration();


        /*StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        //sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        //job.setConf(conf);
        job.run(args);*/
	    
	    Configuration conf=new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		recommend(conf,inPath,outPath,tmpPath,8);
		
		String resOut=HDFS + "/RecommendSystem/CF/test-a/final";
		String example=HDFS + "/RecommendSystem/CF/test-a/example.txt";
		String output=HDFS + "/RecommendSystem/CF/test-a/eval";

		DataAnalyse.analyse(conf, outFile, resOut, example, output);
		
		
		
		
    }
	
	public static void recommend(Configuration conf,String inPath,String outPath,String tmpPath,int recNums) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        //sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        sb.append(" --numRecommendations ").append(recNums);
        String args[] = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        job.setConf(conf);
        job.run(args);
    }

}
