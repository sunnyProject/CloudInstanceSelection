package com.guoty.kmeans;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * KmeansAssistance
 * Parallel K-means Tool Class
 * 
 * getCenters(String inputpath)
 * deleteLastResult(String path)
 * isFinished(String oldpath, String newpath, int k, float threshold)
 * 
 * */

public class KmeansAssistance {	
	//get the  coordinate of cluster center
	public static List<ArrayList<Float>> getCenters(String inputpath){
		List<ArrayList<Float>> result = new ArrayList<ArrayList<Float>>();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			Path in = new Path(inputpath);
			FSDataInputStream fsIn = fs.open(in);
			LineReader lineIn = new LineReader(fsIn, conf); 
			Text line = new Text(); 
			while (lineIn.readLine(line) > 0) {
				String record = line.toString();  
	            //replace tab
				String[] fields = record.replace("\t", " ").split(" ");  
				List<Float> tmplist = new ArrayList<Float>(); 
                for (int i = 0; i < fields.length; ++i){  
                    tmplist.add(Float.parseFloat(fields[i]));  
                } 
                result.add((ArrayList<Float>) tmplist); 
			}
			fsIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return result; 
	}
	
	//Delete the last result of MapReduce
    public static void deleteLastResult(String path){  
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path1 = new Path(path);
            fs.delete(path1, true);  
        } catch (IOException e){  
            e.printStackTrace();  
        }  
    }
	
    //Calculate the distance between the new center and the old center
	 public static boolean isFinished(String oldpath, String newpath, int k, float threshold)throws IOException{
	     List<ArrayList<Float>> oldcenters = KmeansAssistance.getCenters(oldpath);  
	     List<ArrayList<Float>> newcenters = KmeansAssistance.getCenters(newpath);
	     float distance = 0;
	     for (int i = 0; i < k; ++i){
	    	 for (int j = 1; j < oldcenters.get(i).size(); ++j){
	    		 float tmp = Math.abs(oldcenters.get(i).get(j) - newcenters.get(i).get(j));  
	    		 distance += Math.pow(tmp, 2);  
	    	 }
	     }
	      System.out.println("Distance = " + distance + " Threshold = " + threshold);  
	      if (distance < threshold)  
	    	  return true;	      
	      	// replace the old center with the new center
	        KmeansAssistance.deleteLastResult(oldpath); 
	        
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
	        FileSystem fs = FileSystem.get(conf);
	        fs.copyToLocalFile(new Path(newpath), new Path("/old/oldcenter.data"));  
	        fs.delete(new Path(oldpath), true);  
			fs.moveFromLocalFile(new Path("/old/oldcenter.data"), new Path(oldpath));
	        return false;  
	 }
}
