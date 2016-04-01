package HDFS;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSProcess {
	
	//public static String HDFS="hdfs://202.113.127.209:9000/";
	public static String HDFS="hdfs://192.168.32.10:9000/";
	
	
	
	public static boolean put2HDfS(String src,String dst,Configuration conf){
		Path dstPath=new Path(dst);
		try {
			FileSystem hdfs=dstPath.getFileSystem(conf);
			hdfs.copyFromLocalFile(new Path(src), dstPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static boolean checkAndDel(String path,Configuration conf){
		Path dstPath=new Path(path);
		try {
			FileSystem dfs=dstPath.getFileSystem(conf);
			if(dfs.exists(dstPath)){
				dfs.delete(dstPath, true);
			}			
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	public static void main(String args[]){
		String dest=HDFS+"RecommendSystem/valuePref/input";
		String src="/project/newdata";
		Configuration conf=new Configuration();
		Path path=new Path(src);
	
			/*FileSystem fs=path.getFileSystem(conf);
			if(fs.isDirectory(path)){
				FileSystem[] files=fs.getChildFileSystems();
				for(int i=0;i<files.length;i++){

				}
			}*/
			File fs=new File(src);
			//fs.isFile()
			if(fs.isDirectory()){
				File [] files=fs.listFiles();
				for(int i=0;i<files.length;i++){
					put2HDfS(src+"/"+files[i].getName(),dest+"/"+files[i].getName(),conf);
				}
			}
			//put2HDfS(src,dest,conf);
	}
}
