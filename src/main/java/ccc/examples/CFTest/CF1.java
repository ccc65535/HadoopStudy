package ccc.examples.CFTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

public class CF1 {
	private static final String HDFS = "hdfs://202.113.127.209:9000";
	public static void main(String[] args) throws Exception {
		 String inPath = HDFS + "/CollaborativeFilter/test1/input";
	    String inFile = inPath + "";	    
	    String outPath = HDFS + "/CollaborativeFilter/test1/output-boolean";
	    String outFile = outPath + "/part-r-00000";	    
	    String tmpPath = HDFS + "/CollaborativeFilter/test1/tmp-boolean/";
		/* String inPath = HDFS + "/CollaborativeFilter/valuePref/output";
	    String inFile = inPath + "";	    
	    String outPath = HDFS + "/CollaborativeFilter/test2/output";
	    String outFile = outPath + "/part-r-00000";	    
	    String tmpPath = HDFS + "/CollaborativeFilter/test2/tmp/";*/

        //Configuration conf = new Configuration();


        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        //job.setConf(conf);
        job.run(args);
    }

}
