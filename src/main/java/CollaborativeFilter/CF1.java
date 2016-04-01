package CollaborativeFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

public class CF1 {
	private static final String HDFS = "hdfs://192.168.32.10:9000";
	public static void main(String[] args) throws Exception {
		/*String inPath = HDFS + "/RecommendSystem/CF/imp/in/trainSet-2.txt";
	    String inFile = inPath + "";	    
	    String outPath = HDFS + "/RecommendSystem/CF/imp/output-2";
	    String outFile = outPath + "/part-r-00000";	    
	    String tmpPath = HDFS + "/RecommendSystem/CF/tmp/"+System.currentTimeMillis();*/
		String inPath = HDFS + "/RecommendSystem/CF/ClusterBased/in/c5Set.txt";
	    String inFile = inPath + "";	    
	    String outPath = HDFS + "/RecommendSystem/CF/ClusterBased/output5";
	    String outFile = outPath + "/part-r-00000";	    
	    String tmpPath = HDFS + "/RecommendSystem/CF/ClusterBased/tmp/"+System.currentTimeMillis();

        //Configuration conf = new Configuration();


        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        //sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        //job.setConf(conf);
        job.run(args);
    }

}
