package Homework3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("unused")
public class Homework3 {
	/*
	 * Driver Program for all tasks and executions
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*
		 * Parser job
		 */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Parsing documents and getting total number of pages");
		job.setJarByClass(Homework3.class);
		job.setMapperClass(ParserJob.class);
		job.setOutputKeyClass(Text.class); //set output class
		job.setOutputValueClass(Text.class); //set output class
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/parserOutput"));
		job.waitForCompletion(true);

		// Set global counter
		// TOTAL_NUMBER_OF_NODES
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(nodecounter.TOTAL_NUMBER_OF_NODES);
		conf.set(c1.getDisplayName().toString(), Double.toString(c1.getValue()));

		for(int i= 1; i<= 10 ; i++){
			Counter c2 = counters.findCounter(nodecounter.DANGLING_NODES_PAGERANK);
			conf.set(c2.getDisplayName().toString(), Double.toString(c2.getValue()));
			
			/*
			 * Page Rank Job
			 */
			Job prjob = Job.getInstance(conf, "Calculating Page Ranks");
			prjob.setJarByClass(Homework3.class);

			prjob.setMapperClass(PageRankMapper.class);
			prjob.setReducerClass(PageRankReducer.class);

			prjob.setMapOutputKeyClass(Text.class);
			prjob.setMapOutputValueClass(Text.class);

			prjob.setOutputKeyClass(Text.class);
			prjob.setOutputValueClass(Text.class);
			if(i == 1){
				FileInputFormat.addInputPath(prjob,new Path(args[1] + "/parserOutput"));
				FileOutputFormat.setOutputPath(prjob, new Path(args[1] + "/iteration-" + i ));
			} else {
				FileInputFormat.addInputPath(prjob, new Path(args[1] + "/iteration-" + (i-1)));
				FileOutputFormat.setOutputPath(prjob, new Path(args[1] + "/iteration-" + i));
			}
			// Wait till the job finishes.
			prjob.waitForCompletion(true);
		}
		
		/*
		 * Getting top 100 Job
		 */
		Job finalJob = Job.getInstance(conf, "Calculating Top 100");
		finalJob.setJarByClass(Homework3.class);
		finalJob.setMapperClass(TopKMapper.class);
		finalJob.setReducerClass(TopKReducer.class);
		finalJob.setOutputKeyClass(NullWritable.class);
		finalJob.setOutputValueClass(Text.class);
		finalJob.setNumReduceTasks(1);
		FileInputFormat.addInputPath(finalJob, new Path(args[1] + "/iteration-" + 10));
		FileOutputFormat.setOutputPath(finalJob, new Path(args[1] + "/top100"));
		System.exit(finalJob.waitForCompletion(true) ? 0 : 1);
		
	}

}
