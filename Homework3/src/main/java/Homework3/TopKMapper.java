package Homework3;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	private HashMap<String, Double> pageRanks = new HashMap<String, Double>();
	private int k = 100;
	private double dampingFactor = 0.15;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Get values of TOTAL_NUMBER_OF_NODES and DANGLING_NODES_PAGERANK
		Configuration conf = context.getConfiguration();
		double NumberOfNodes = 	Double.parseDouble(conf.get("TOTAL_NUMBER_OF_NODES"));
		double PageRankOfDanglingNodes = Double.parseDouble(conf.get("DANGLING_NODES_PAGERANK"));

		String record = value.toString().replaceAll(" ", "");

		String[] tempValues = record.split(":");
		String pageName = tempValues[0].trim();
		double pageRank;

		// PR calculation of present Page
		if(tempValues[1].equals("dummyPageRank")){
			pageRank = 1.0/NumberOfNodes;
		} else {
			pageRank = Double.parseDouble(tempValues[1]);
		}

		// Add delta contribution
		double tempPageRank = (1.0 - dampingFactor) * ((PageRankOfDanglingNodes/Math.pow(10,12))) / NumberOfNodes;
		pageRank = pageRank + tempPageRank;
		
		pageRanks.put(pageName, pageRank);
        
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {

		pageRanks = (HashMap<String, Double>) SortUtility.sortByValue(pageRanks);
		int count = 0;
		
		// Output only first 100. Idea behind this is if it is overall top 100, it is top 100 here as well.
		for (Map.Entry<String, Double> entry : pageRanks.entrySet()) {
			context.write(NullWritable.get(), new Text(entry.getValue() + ":" + entry.getKey()));
			count++;
			if (count >= k) break;
		}
	}
}