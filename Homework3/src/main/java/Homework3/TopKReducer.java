package Homework3;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int k = 100;
		HashMap<String, Double> topk = new HashMap<String, Double>();
		
		// for every page and rank Text object, get the page name, the rank and put it into the map
		for (Text value : values) {
			String v = value.toString();
			double rank = Double.parseDouble(v.split(":")[0]);
			topk.put(v, rank);
			
		}
		
		topk = (HashMap<String, Double>) SortUtility.sortByValue(topk);
		int count = 0;
		
		for (String t : topk.keySet()) {
			context.write(NullWritable.get(), new Text(t));
			count++;
			if (count >= k) break;
		}
		
	}
	
}