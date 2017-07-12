package com.guoty.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper 
 * Input key:(LongWritable) Starting offset
 * 		 value:(Text) The coordinate of this point  
 * Output key:(IntWritable) The Index of the center closest to the point  
 * 		  value:(Text) The coordinate of this point  
 * */

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	protected void map(LongWritable key, Text value, Context context) 
	throws IOException ,InterruptedException {

        String line = value.toString();  
        String[] fields = StringUtils.split(line, " ");
        //Get the number of k from Configuration
        int k = Integer.parseInt(context.getConfiguration().get("kpath"));  
        //Get the list of centers from Configuration
        List<ArrayList<Float>> centers = KmeansAssistance.getCenters(context.getConfiguration().get("centerpath"));

        float minDist = Float.MAX_VALUE;  
        int centerIndex = k;  
        
        //Calculate the Euclidean distance between the point and each center
        for (int i = 0; i < k; ++i){  
            float currentDist = 0;  
            for (int j = 0; j < fields.length; ++j){  
                float tmp = Math.abs(centers.get(i).get(j + 1) - Float.parseFloat(fields[j]));  
                currentDist += Math.pow(tmp, 2);  
            }  
            //finds the Index of the center closest to the point 
            if (minDist > currentDist){  
                minDist = currentDist;  
                centerIndex = i;  
            }  
        } 
        //Output the result of mapper
        context.write(new IntWritable(centerIndex), new Text(value));
	}
}
