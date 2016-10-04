package homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Combiner {
		// To write temperature string and associated value and counts for the same
		public static class MyWritable implements Writable{
			private Double temperature = 0.0;
			private String typeOfTemp = "";
			private Double counts = 0.0;
			
			// get and set count of the StationID
			public Double getCounts(){
				return counts;
			}
			
			public void setCounts(Double inputCounts){
				counts = inputCounts;
			}
			
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
				out.writeDouble(counts);
			}
			
			@Override
			public void readFields(DataInput in) throws IOException{
				temperature = in.readDouble();
				typeOfTemp = WritableUtils.readString(in);
				counts = in.readDouble();
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
		
		// B. Combiner class
		public static class TemperatureCombiner
	    extends Reducer<Text, MyWritable, Text, MyWritable>{
			MyWritable stationRecord = new MyWritable();
			public void reduce(Text key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
				Double sumMeanMinTemp = 0.0;
				Double sumMeanMaxTemp = 0.0;
				
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
//				System.out.println(sumMeanMaxTemp);
//				System.out.println(counterforMax);
				// Update local Max sum, add count
				stationRecord.setTemperature(sumMeanMaxTemp);
				stationRecord.setTypeOfTemperature("TMAX");
				stationRecord.setCounts(counterforMax);
				context.write(key, stationRecord);
				
//				System.out.println(sumMeanMinTemp);
//				System.out.println(counterforMin);
				// Update local Min sum, add count
				stationRecord.setTemperature(sumMeanMinTemp);
				stationRecord.setTypeOfTemperature("TMIN");
				stationRecord.setCounts(counterforMin);
				context.write(key, stationRecord);
			}
		}
		
		public static class TemperatureReducer
	    extends Reducer<Text, MyWritable, Text, Text>{
			Text outputRecord = new Text();
			public void reduce(Text key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
				
				Double sumMeanMinTemp = 0.0;
				Double sumMeanMaxTemp = 0.0;
				
				Double counterforMax = 0.0;
				Double counterforMin = 0.0;
				
				Double meanMinTemp = 0.0;
				Double meanMaxTemp = 0.0;
				
				for (MyWritable data: values){
					if(data.getTypeOfTemperature().equals("TMAX")){
						sumMeanMaxTemp += data.getTemperature();
						counterforMax += data.getCounts();
					}else{
						sumMeanMinTemp += data.getTemperature();
						counterforMin += data.getCounts();
					}
				}
				System.out.println(sumMeanMinTemp);
				System.out.println(counterforMin);
				
				System.out.println(sumMeanMaxTemp);
				System.out.println(counterforMax);
				// Calculate the mean in Reducer.
				meanMinTemp = sumMeanMinTemp/counterforMin;
				meanMaxTemp = sumMeanMaxTemp/counterforMax;
				
				outputRecord.set(" Mean Minimum Temperature " + meanMinTemp +
						" Mean Maximum Temperature " + meanMaxTemp);
				context.write(key, outputRecord);
			}
		}
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "weather temp data with combiner");
	    job.setJarByClass(Homework2.class);
	    
	    job.setMapperClass(TempretureMapper.class);
	    job.setCombinerClass(TemperatureCombiner.class);
	    job.setReducerClass(TemperatureReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MyWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
