package figured;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static class TokenizerMapper 
   extends Mapper<Object, Text, Text, IntWritable>{
		//private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			/*StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}*/
			/*String [] words=value.toString().split("\\s+");
			for(int i=0;i<words.length;i++){
				//word.set(words[i]);
				//context.write(word, one);
				context.write(new Text(words[i]),new IntWritable(1));
			}*/
			
			StringTokenizer lines=new StringTokenizer(value.toString(),"\n");
			while(lines.hasMoreTokens()){
				String line=lines.nextToken();
				context.write(new Text(line),new IntWritable(1));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			//result.set(sum);
			//context.write(key, result);
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//String input="/home/Hadoop/data/words";
		//String output="/home/Hadoop/data/result";
		String HDFS="hdfs://202.113.127.209:9000/CollaborativeFilter/";
		String input=HDFS+"testInput/0220000011/20141201";
		String output=HDFS+"testOutput";
		/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}*/
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
