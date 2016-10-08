package homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SecondarySort {
	
	
	/*
	 * Custom Writable class
	 */
	public static class MyWritable implements Writable {

		private String year;
		private Double temperature = 0.0;
		private String typeOfTemp = "";
		private Double counts = 0.0;

		// Get and set year of the stationID
		public String getYear(){
			return year;
		}

		public void setYear(String inputYear){
			year = inputYear;
		}

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
			WritableUtils.writeString(out, year);
		}

		@Override
		public void readFields(DataInput in) throws IOException{
			temperature = in.readDouble();
			typeOfTemp = WritableUtils.readString(in);
			counts = in.readDouble();
			year = WritableUtils.readString(in);
		}

		public static MyWritable read(DataInput in) throws IOException {
			MyWritable w = new MyWritable();
			w.readFields(in);
			return w;
		}

	}

	
	/*
	 * Mapper Class
	 * 
	 * Key is comma separated stationID and year Text. 
	 * Mapper dumps the key and the MyWritable objects as value 
	 * 
	 */
	public static class SecondarySortMapper
	extends Mapper<Object, Text, Text, MyWritable>{
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			MyWritable dataDump = new MyWritable();
			String record = value.toString();
			if(record.contains("TMAX")|| record.contains("TMIN")){
				String[] collection = record.split(",");
				if(!collection[3].equals("")){
					String stationID = new String(collection[0]);
					String year = new String(collection[1].substring(0, 4));
					String typeOftemperature =  new String(collection[2]);
					Double temperature = new Double(Double.parseDouble(collection[3]));
					
					// Note that the key is comma separated stationID and year
					Text keyOfStationIDAndYear = new Text(stationID + "," + year);

					// Write it to MyWritable DataDump
					dataDump.setTemperature(temperature);
					dataDump.setTypeOfTemperature(typeOftemperature);
					dataDump.setYear(year);
					
					//Dump the data
					context.write(keyOfStationIDAndYear, dataDump);
				}
			}
		}
	}


	/*
	 *  Partitioner Class
	 *  
	 *  partitions according to Station IDs.
	 */
	public static class SecondarySortPartitioner extends Partitioner<Text, MyWritable> {
		@Override
		public int getPartition(Text key, MyWritable value, int numReduceTasks) {
			String stationID = key.toString().split(",")[0];
			return Math.abs((stationID.hashCode() % numReduceTasks));
		}
	}

	
	/*
	 * KeySort Comparator Class
	 * 
	 * KeySort comparison is done both on station ID and the Year, by
	 * splitting the keys.
	 */
	public static class SecondarySortKeySortComparator extends WritableComparator{
		
		protected SecondarySortKeySortComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			Text key1 = (Text) w1;
			Text key2 = (Text) w2;

			String stationID1 = key1.toString().split(",")[0];
			String year1 = key1.toString().split(",")[1];

			String stationID2 = key2.toString().split(",")[0];
			String year2 = key2.toString().split(",")[1];
			
			// Compare both station ID and year
			int result = stationID1.compareTo(stationID2);
			if(result == 0){
				return year1.compareTo(year2);
			}
			return result;
		}
	}
	
	
	/*
	 * Group Comparator Class
	 * 
	 * Unlike KeySort Comparator, grouping comparator class compares only the stationID
	 * This is because we have to group years by stationIDs
	 */
	public static class SecondarySortGroupingComparator extends WritableComparator {
		protected SecondarySortGroupingComparator(){
			super(Text.class,true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			// Check only StationID
			Text key1 = (Text) w1;
			Text key2 = (Text) w2;

			String stationID1 = key1.toString().split(",")[0];
			String stationID2 = key2.toString().split(",")[0];
			/*
			 * Grouping comparator uses only station ID
			 */
			return stationID1.compareTo(stationID2);
		}
	}

	
	/*
	 * Reducer Class
	 * 
	 * Aggregates results by year, by station ID using a local HashMap. 
	 * Because of the grouping comparator, we obviously would get year wise records in ascending order.That is
	 * there will be no mention about the current stationID from next Iteration.
	 * We can take benefit of that and save space. 
	 * 
	 * The local hashmap will take care of year aggregations and reducer then will dump the output after calculating the mean
	 */
	public static class SecondarySortReducer
	extends Reducer<Text, MyWritable, Text, Text>{
		Text outputRecord = new Text();
		
		public void reduce(Text key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
			// Hashmap for year wise TMAX And TMIN for a specific year
			LinkedHashMap<String, double[]> yearwiseTemperature = new LinkedHashMap<String, double[]>();
			for (MyWritable data: values){
				if (yearwiseTemperature.containsKey(data.getYear())){
					if (data.getTypeOfTemperature().equals("TMAX")) {
						double[] temp = new double[] {
								yearwiseTemperature.get(data.getYear())[0],
								yearwiseTemperature.get(data.getYear())[1],
								yearwiseTemperature.get(data.getYear())[2] + data.getTemperature(),
								yearwiseTemperature.get(data.getYear())[3] + 1.0
						};
						yearwiseTemperature.put(data.getYear(), temp);
					} else {
						double[] temp = new double[] {
								yearwiseTemperature.get(data.getYear())[0]+ data.getTemperature(),
								yearwiseTemperature.get(data.getYear())[1]+ 1.0,
								yearwiseTemperature.get(data.getYear())[2],
								yearwiseTemperature.get(data.getYear())[3]
						};
						yearwiseTemperature.put(data.getYear(), temp);
					}
				} else {
					
					if (data.getTypeOfTemperature().equals("TMAX")){
						double[] temp = new double[] {0.0, 0.0, data.getTemperature(), 1.0};
						yearwiseTemperature.put(data.getYear(), temp);
					} else {
						double[] temp = new double[] {data.getTemperature(), 1.0, 0.0, 0.0};
						yearwiseTemperature.put(data.getYear(),temp);
					}
				}
				
			}
			
			
			// Calculate mean max, mean min for a year and for the given specific StationID
			double tempMeanMin = 0.0;
			double tempMeanMax = 0.0;
			StringBuilder outputReduceRecord = new StringBuilder();
			outputReduceRecord.append(" - [");
			
			for(Entry<String, double[]> entry : yearwiseTemperature.entrySet()) {
				tempMeanMin = Math.round((entry.getValue()[0]/entry.getValue()[1])*100.0)/100.0;
				tempMeanMax = Math.round((entry.getValue()[2]/entry.getValue()[3])*100.0)/100.0;
				
				outputReduceRecord.append("(" + entry.getKey() + ", " + tempMeanMin + ", " + tempMeanMax  + "), ");
			}
			outputReduceRecord.append("]");
			
			outputRecord.set(outputReduceRecord.toString());
			Text stationName = new Text(key.toString().split(",")[0]);
			context.write(stationName, outputRecord);
		}

	}

	
	/*
	 * Entry Level method
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length != 2){
			System.out.println("Received bad input. Two parameters are reuired for the program!");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "secondary sort");

		job.setJarByClass(SecondarySort.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyWritable.class);
		
		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(SecondarySortKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		
		
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
