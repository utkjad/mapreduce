package homework2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Homework2 {
	
	// To write temperature string and associated value
	public static class MyWritable implements Writable{
		private Double temperature;
		private String typeOfTemp;
		
		// Get and set temperature of the current record
		public Double getTemperature(){
			return temperature;
		}
		
		public void setTemperature(Double temp){
			temperature = temp;
		}
		
		// Get and set type of temperature for the current record
		public String getTypeOfTemperature(){
			return typeOfTemp;
		}
		
		public void setTypeOfTemperature(String str){
			typeOfTemp = str;
		}
		
		@Override
		public void write(DataOutput out)throws IOException{
			out.writeDouble(temperature);
			WritableUtils.writeString(out, typeOfTemp);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			temperature = in.readDouble();
			typeOfTemp = WritableUtils.readString(in);
		}
		
		public static MyWritable read(DataInput in) throws IOException {
			MyWritable w = new MyWritable();
			w.readFields(in);
			return w;
		}
	}
	
	// Mapper class
	public static class TempretureMapper
    extends Mapper<Object, Text, Text, MyWritable>{
		
		public void map(Object key, Text value, Context context) 
	    		throws IOException, InterruptedException {
			MyWritable dataDump = new MyWritable();
			String record = value.toString();
			if(record.contains("TMAX")|| record.contains("TMIN")){
				String[] collection = record.split(",");
				if(!collection[3].equals("")){
					Text stationID = new Text(collection[0]);
					String typeOftemperature =  new String(collection[2]);
					Double temperature = new Double(Double.parseDouble(collection[3]));
					
					// Write it to MyWritable DataDump
					dataDump.setTemperature(temperature);
					dataDump.setTypeOfTemperature(typeOftemperature);
					
					//Dump the data
					context.write(stationID, dataDump);
				}
			}
		}
	}
	
	public static class TempretureReducer
    extends Reducer<Text, MyWritable, Text, Text>{
		Text combinationOfTMAXAndTMIN = new Text();
		public void reduce(Text key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
			Double sumMeanMinTemp = 0.0;
			Double sumMeanMaxTemp = 0.0;
			
			Double meanMinTemp = 0.0;
			Double meanMaxTemp = 0.0;
			
			Double counterforMin = 0.0;
			Double counterforMax =  0.0;
			
			for (MyWritable data: values){
				if(data.getTypeOfTemperature().equals("TMAX")){
					sumMeanMaxTemp += data.getTemperature();
					counterforMax += 1;
				}else{
					sumMeanMinTemp += data.getTemperature();
					counterforMin += 1;
				}
			}
			
			meanMaxTemp = sumMeanMaxTemp/counterforMax;
			meanMinTemp = sumMeanMinTemp/counterforMin;
			
			combinationOfTMAXAndTMIN.set(new String(key.toString() + " " + meanMaxTemp + " " + meanMinTemp));
			context.write(key, combinationOfTMAXAndTMIN);
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "weather temp data");
	    job.setJarByClass(Homework2.class);
	    job.setMapperClass(TempretureMapper.class);
	    job.setReducerClass(TempretureReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MyWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
