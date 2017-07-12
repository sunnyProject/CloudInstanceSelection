package com.guoty.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 * Input key:(IntWritable) The Index of the center closest to the point  
 * 		 value:(Text) The coordinate of  point  
 * Output key:(IntWritable) The Index of the center
 * 		  value:(Text) The new coordinate of new center 
 * */

public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    public void reduce(IntWritable key, Iterable<Text> value, Context context)  
    throws IOException, InterruptedException{
    	List<ArrayList<Float>> assistList = new ArrayList<ArrayList<Float>>();
    	String tmpResult = "";
    	//Get the list of points,according to the index
    	for (Text val : value){
            String line = val.toString();
            String[] fields = StringUtils.split(line, " ");
            List<Float> tmpList = new ArrayList<Float>();
            for (int i = 0; i < fields.length; ++i){
                tmpList.add(Float.parseFloat(fields[i]));
            }
            assistList.add((ArrayList<Float>) tmpList);
        }
        //Calculate the new center of cluster
    	for (int i = 0; i < assistList.get(0).size(); ++i){
            float sum = 0;
            for (int j = 0; j < assistList.size(); ++j){
                sum += assistList.get(j).get(i);
            }
            float tmp = sum / assistList.size();
            if (i == 0){
                tmpResult += tmp;
            }
            else{
                tmpResult += " " + tmp;
            }
        }
        //Output the result of reducer
        context.write(key, new Text(tmpResult));
    }
}
