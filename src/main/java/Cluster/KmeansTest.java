package Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KmeansTest {
	private static final String HDFS = "hdfs://192.168.32.10:9000";
	private static final Logger log = LoggerFactory.getLogger(KmeansTest.class);
	
	public static void main(String[] args) throws Exception {
        
		PropertyConfigurator.configure( "./log4j.properties");

        Configuration conf = new Configuration();
        String inPath=HDFS+"/KmeansTest/input/studentFeature.txt";
        String outPath=HDFS+"/KmeansTest/output";
        
        DistanceMeasure measure =new EuclideanDistanceMeasure();
        int k=5;
        Path input=new Path(inPath);
        Path output=new Path(outPath);
        
        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        System.out.println("Running random seed to get initial clusters");
        Path clusters = new Path(output, "random-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        log.info("Running KMeans with k = {}", k);
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, 0.01,
            100, true, 0.0, false);
        // run ClusterDumper
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output,"clusteredPoints");
        log.info("Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob,clusteredPoints);
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        clusterDumper.printClusters(null);
        //log.info(clusterDumper.toString());
    }
}
