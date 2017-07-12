package com.guoty.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Driver
 * 
 * Continuously perform MapReduce jobs until 
 * the distance between adjacent two iterations of the cluster center is less than the threshold 
 * or reaches the specified number of iterations
 * 
 * Input:
 * 	args[0]input path
 *	args[1]output path 
 * 	args[2]initial centers file path 
 * 	args[3]new centers file path 
 * 	args[4]the number of k 
 * 	args[5]the threshold of distance between the old center and the new center
 * 
 * */

public class KmeansDriver {
	public static void main(String[] args) throws Exception{
		int repeated = 0; 
		do{
            if (args.length != 6){  
                System.err.println("There are some errors in args");  
                System.exit(2);  
            }
            
			Configuration conf = new Configuration();
            conf.set("centerpath", args[2]);  
            conf.set("kpath", args[4]);
			conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
	        FileSystem fs = FileSystem.get(conf);
	         
			//Create MapReduce job
			Job job = Job.getInstance(conf);
			//Set Jar for MapReduce
			job.setJarByClass(KmeansDriver.class);
			//Set Mapper and Reducer
			job.setMapperClass(KmeansMapper.class);
			job.setReducerClass(KmeansReducer.class);
			//Set Output format of Mapper
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			//Set Output format of Reducer
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			//Set input and output path
            Path in = new Path(args[0]);  
            Path out = new Path(args[1]);  
        
			FileInputFormat.setInputPaths(job, in);
			//If the output path already exists, delete the output path
			if (fs.exists(out))
	            fs.delete(out, true);
			FileOutputFormat.setOutputPath(job, out);
			
			job.waitForCompletion(true);
	        ++repeated;  
	        System.out.println(repeated + " times repeated.");  
		} while(repeated<10 && (KmeansAssistance.isFinished(args[2], args[3], Integer.parseInt(args[4]), Float.parseFloat(args[5])) == false));
		
		//Clustering the points according to the last cluster center  	    
		Cluster(args); 
	}
	
	 public static void Cluster(String[] args)
     throws IOException, InterruptedException, ClassNotFoundException{
		 if (args.length != 6){
			 System.err.println("There are some errors in args");
			 System.exit(2);
		 }
		 
		 Configuration conf = new Configuration();
		 conf.set("centerpath", args[2]);
		 conf.set("kpath", args[4]);
         conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
	     FileSystem fs = FileSystem.get(conf);

		 //Only execute Mapper
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(KmeansDriver.class);
		 job.setMapperClass(KmeansMapper.class);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
         Path in = new Path(args[0]);  
         Path out = new Path(args[1]);  
		 FileInputFormat.addInputPath(job, in);
		 if (fs.exists(out)){
			 fs.delete(out, true);
		 }
		 FileOutputFormat.setOutputPath(job, out);
		 
		 job.waitForCompletion(true);
	 }
}