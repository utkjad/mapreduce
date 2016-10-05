package homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombiner {
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
		
		HashMap<Text, double[]> stationRecord;
		MyWritable dataDump = new MyWritable();
		
		protected void setup(Context context)
			throws IOException, InterruptedException {
			 // Create HashMap
			stationRecord =  new HashMap<Text, double[]>();
		}
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String record = value.toString();
			if(record.contains("TMAX")|| record.contains("TMIN")){
				String[] collection = record.split(",");
				if(!collection[3].equals("")){
					Text stationID = new Text(collection[0]);
					String typeOftemperature =  new String(collection[2]);
					Double temperature = new Double(Double.parseDouble(collection[3]));
					
					// Now, update over stationRecords and find the local sum
					if (stationRecord.containsKey(stationID)) {
						if(typeOftemperature.equals("TMAX")){
							stationRecord.put(stationID, new double[]{stationRecord.get(stationID)[0],
									stationRecord.get(stationID)[1],
									stationRecord.get(stationID)[2] + 1.0,
									stationRecord.get(stationID)[3] + temperature});
						}else{
							stationRecord.put(stationID, new double[]{stationRecord.get(stationID)[0] + 1.0,
									stationRecord.get(stationID)[1] + temperature ,
									stationRecord.get(stationID)[2],
									stationRecord.get(stationID)[3]});
						}
					} else {
						if(typeOftemperature.equals("TMAX")){
							stationRecord.put(stationID, new double[]{0.0,
									0.0,
									1.0,
									temperature});
						}else{
							stationRecord.put(stationID, new double[]{1.0,
									temperature ,
									0.0,
									0.0});
						}
					}
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(Entry<Text, double[]> entry : stationRecord.entrySet()) {
				System.out.println(entry.getKey() + " " +
						entry.getValue()[0] + " " +
						entry.getValue()[1] + " " +
						entry.getValue()[2] + " " +
						entry.getValue()[3] + " " );
				// Add local min
				dataDump.setTemperature(entry.getValue()[1]);
				dataDump.setCounts(entry.getValue()[0]);
				dataDump.setTypeOfTemperature("TMIN");
				context.write(entry.getKey(), dataDump);
				
				// Add local max
				dataDump.setTemperature(entry.getValue()[3]);
				dataDump.setCounts(entry.getValue()[2]);
				dataDump.setTypeOfTemperature("TMAX");
				context.write(entry.getKey(), dataDump);
			}
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
					System.out.println(data.getCounts());
					System.out.println(data.getTemperature());
					sumMeanMaxTemp += data.getTemperature();
					counterforMax += data.getCounts();
				}else{
					sumMeanMinTemp += data.getTemperature();
					counterforMin += data.getCounts();
				}
			}
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
