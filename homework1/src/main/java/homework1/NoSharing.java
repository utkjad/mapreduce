package homework1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TimeRecords{
	static double startTime, endTime;
}

/*
 *  Class taking care of Threading and execution of average
 */
class ProcessNoLock implements Runnable{
	List<String> records = new ArrayList<String>();
	HashMap<String, double []> accumulationRecord = new HashMap<String, double[]>();
	Boolean withFibonacciHuh;

	public ProcessNoLock(List<String> record, Boolean withFibonacciHuh) throws IOException{
		this.records = record;
		this.withFibonacciHuh = withFibonacciHuh;
	}

	public HashMap<String, double []> getAccumulatedRecord(){
		return this.accumulationRecord;
	}
	public void calculateAverage() throws IOException{
		// Start Time
		TimeRecords.startTime = System.currentTimeMillis();
		for (String record: this.records){
			if (record.contains("TMAX")){

				String stationID = new String(record.split(",")[0]);
				Double tmaxCurrentRecord = new Double(Double.parseDouble(record.split(",")[3]));
				synchronized (accumulationRecord) {
					if (accumulationRecord.containsKey(stationID)){
						double [] tempValues = new double[2];
						tempValues = accumulationRecord.get(stationID);

						double tempCount =  tempValues[0];
						double averageSoFar =  tempValues[1];

						// Now, update count and Average
						double sumSoFar = tempCount * averageSoFar;
						double newSum = sumSoFar + tmaxCurrentRecord;

						tempValues[0] = tempCount + 1;
						tempValues[1] = newSum/ tempValues[0]; // New Average
						
						if(this.withFibonacciHuh){
						// Calculate the Fibonacci (part C)
						LoadDataStructure.fibonacci(17);
						}
						accumulationRecord.put(stationID, tempValues);
						

					}else{
						double [] tempValues = new double[2];
						// Add Count, and average
						tempValues[0] = 1;
						tempValues[1] = tmaxCurrentRecord; // Average will also be same here

						if(this.withFibonacciHuh){
						// Calculate the Fibonacci (part C)
						LoadDataStructure.fibonacci(17);
						}

						// Put in Data Structure
						accumulationRecord.put(stationID, tempValues);
					}
				}
			}
		}
		// End Time
		TimeRecords.endTime = System.currentTimeMillis();
	}

	public void run(){
		try {
			this.calculateAverage();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}  
}


public class  NoSharing{
	List<String> records = new ArrayList<String>();
	HashMap<String, double []> accumulationRecord = new HashMap<String, double[]>();

	public NoSharing() throws IOException{
		// Get records in List format
		this.records = LoadDataStructure.records;
	}
	public void executeNoSharingHelper(Boolean withFibonacciHuh) throws IOException{
		// Get total number of running cores at present time
		int cores = (int) Runtime.getRuntime().availableProcessors();
		ProcessNoLock[] pnl = new ProcessNoLock[cores];
		Thread[] threads = new Thread[cores];


		int size = this.records.size();

		for (int i = 0; i < cores; i++) {
			List<String> recordsToBePassed = new ArrayList<String>();
			recordsToBePassed = this.records.subList((size*i)/cores, (size * (i+1))/ cores);
			pnl[i] = new ProcessNoLock(recordsToBePassed, withFibonacciHuh);
			threads[i] =  new Thread(pnl[i]);
			threads[i].start();
		}


		for (int i = 0; i < cores; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		/*
		 * After finishing threads combine everything
		 */
		for (int i = 0; i < cores; i++) {
			if(i==0){
				this.accumulationRecord.putAll((pnl[i].getAccumulatedRecord()));
			} else {

				for(Map.Entry<String, double[]> record: pnl[i].getAccumulatedRecord().entrySet()){
					if(this.accumulationRecord.containsKey(record.getKey())){
						double [] tempValues = new double[2];
						tempValues = this.accumulationRecord.get(record.getKey());

						double previous_count =  tempValues[0];
						double  previous_sum = tempValues[0] * tempValues[1];

						// Update count
						tempValues[0] = record.getValue()[0] + previous_count;
						tempValues[1] = ((record.getValue()[1] * record.getValue()[0]) + previous_sum)/tempValues[0];
					}else{
						this.accumulationRecord.put(record.getKey(), record.getValue());
					}
				}
			}
		}
	}
	public void executeNoSharing() throws IOException{
		List<Double> runningTime = new ArrayList<Double>();

		System.out.println("\nWithout Fibonacci 10 times\n");

		for(int i=0;i<10; i++){
			System.out.print("Run #" + i);
			this.executeNoSharingHelper(false);
			System.out.print(" Time -> " + (TimeRecords.endTime - TimeRecords.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(TimeRecords.endTime - TimeRecords.startTime);
		}
		LoadDataStructure.printStats(runningTime);

		runningTime = new ArrayList<Double>();
		System.out.println("\nWith Fibonacci 10 times\n");
		for(int i=0;i<10; i++){
			System.out.print("Run #" + i);
			this.executeNoSharingHelper(true);
			System.out.print(" Time -> " + (TimeRecords.endTime - TimeRecords.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(TimeRecords.endTime - TimeRecords.startTime);
		}
		LoadDataStructure.printStats(runningTime);
	}

}
