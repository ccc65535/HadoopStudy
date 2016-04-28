package Frame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import CollaborativeFilter.*;

public class DataAnalyse {
	
	
	public static void main(String args[]){
		ResultFormation resFormation=new ResultFormation();
		Evaluate eval=new Evaluate();
		String HDFS="hdfs://202.113.127.208:9000/RecommendSystem/";
		Configuration conf=new Configuration();
		conf.addResource(new Path("./bin/core-site.xml"));
		conf.addResource(new Path("./bin/hdfs-site.xml"));
		/*try {
			resFormation.setInPath(HDFS+"/CF/imp/output/part-r-00000");
			resFormation.setOutPath(HDFS+"/CF/final");
			resFormation.formationHDFS(conf);
			
			
			Evaluate val=new Evaluate();

			
			val.setResultPath(HDFS+"/CF/final");
			val.setExamplePath(HDFS+"/CF/example.txt");
			val.setOutPath(HDFS+"/CF/eval");
			val.generateDataHDFS(conf);
			val.evalHDFS(conf);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		try {
			analyse(conf,HDFS+"/CF/imp/output/part-r-00000",HDFS+"/CF/final",HDFS+"/CF/example.txt",HDFS+"/CF/eval");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void analyse(Configuration conf,String resIn,	String resOut,String example,String output) throws Exception{
		ResultFormation resFormation=new ResultFormation();
		Evaluate eval=new Evaluate();
		resFormation.setInPath(resIn);
		resFormation.setOutPath(resOut);
		resFormation.formationHDFS(conf);
			
			
		Evaluate val=new Evaluate();

			
		val.setResultPath(resOut);
		val.setExamplePath(example);
		val.setOutPath(output);
		val.generateDataHDFS(conf);
		val.evalHDFS(conf);
			

	}
}
