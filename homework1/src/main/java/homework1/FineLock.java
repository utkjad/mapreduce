package homework1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class  FineLock{
	List<String> records = new ArrayList<String>();
	Map <String, double []> accumulationRecord;
	double startTime, endTime;

	/*
	 *  Class taking care of Threading and execution of average
	 */
	class ProcessFineLock implements Runnable{
		List<String> records = new ArrayList<String>();
		Boolean withFibonacciHuh;

		public ProcessFineLock(List<String> record, Boolean withFibonacciHuh) throws IOException{
			this.records = record;
			this.withFibonacciHuh = withFibonacciHuh;
		}

		public void calculateAverage() throws IOException{
			// Start Time
			startTime = System.currentTimeMillis();
			for (String record: this.records){
				if (record.contains("TMAX")){
					String stationID = new String(record.split(",")[0]);
					Double tmaxCurrentRecord = new Double(Double.parseDouble(record.split(",")[3]));
					
					// Note the synchronized block on the data structure accumulationRecord here.
					// this is due to the dependency of put on the containsKey
					
						if (accumulationRecord.containsKey(stationID)){
							synchronized(accumulationRecord.get(stationID)){
								double [] tempValues = new double[2];
								tempValues = accumulationRecord.get(stationID);
		
								double tempCount =  tempValues[0];
								double averageSoFar =  tempValues[1];
		
								// Now, update count and Average
								double sumSoFar = tempCount * averageSoFar;
								double newSum = sumSoFar + tmaxCurrentRecord;
		
								tempValues[0] = tempCount + 1;
								tempValues[1] = newSum/ tempValues[0]; // New Average
		
		
								if (withFibonacciHuh) {
									// Calculate the Fibonacci (part C)
									LoadDataStructure.fibonacci(17);
								}
		
								accumulationRecord.put(stationID, tempValues);
							}
	
						}else{
							double [] tempValues = new double[2];
							// Add Count, and average
							tempValues[0] = 1;
							tempValues[1] = tmaxCurrentRecord; // Average will also be same here

	
							if (withFibonacciHuh) {
								// Calculate the Fibonacci (part C)
								LoadDataStructure.fibonacci(17);
							}
	
							// Put in Data Structure
							double [] tempValues2 = new double[2];
							tempValues2 = accumulationRecord.put(stationID, tempValues);
							if (tempValues2!=null){
								synchronized(accumulationRecord.get(stationID)){
									double [] tempValues3 = new double[2];
									tempValues3 = accumulationRecord.get(stationID);
			
									double tempCount =  tempValues3[0];
									double averageSoFar =  tempValues3[1];
			
									// Now, update count and Average
									double sumSoFar = tempCount * averageSoFar;
									double newSum = sumSoFar + tmaxCurrentRecord;
			
									tempValues3[0] = tempCount + 1;
									tempValues3[1] = newSum/ tempValues[0]; // New Average
			
			
									if (withFibonacciHuh) {
										// Calculate the Fibonacci (part C)
										LoadDataStructure.fibonacci(17);
									}
			
									accumulationRecord.put(stationID, tempValues3);
								}
							}
						}
				}
			}
			// End Time
			endTime = System.currentTimeMillis();
		}

		public void run(){
			try {
				this.calculateAverage();
			} catch (IOException e) {
				e.printStackTrace();
			}  
		}  
	}


	/*
	 * Class Body of NoLock
	 */
	public FineLock() throws IOException{
		// Get records in List format
		this.records = LoadDataStructure.records;
		// Note the use of ConcurrentHashMap here. It is perfect example of fine lock.
		this.accumulationRecord = new ConcurrentHashMap <String, double []> ();
	}
	
	public void executeFineLockHelper(Boolean withFibonacciHuh) throws IOException{
		// Get total number of running cores at present time
		int cores = (int) Runtime.getRuntime().availableProcessors();
		ProcessFineLock[] pnl = new ProcessFineLock[cores];
		Thread[] threads = new Thread[cores];
		int size = this.records.size();

		for (int i = 0; i < cores; i++) {
			List<String> recordsToBePassed = new ArrayList<String>();
			recordsToBePassed = this.records.subList((size*i)/cores, (size * (i+1))/ cores);
			pnl[i] = new ProcessFineLock(recordsToBePassed, withFibonacciHuh);
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
		
	}
	
	/*
	 * Entry method for Fine Lock execution
	 */
	public void executeFineLock() throws IOException{
		List<Double> runningTime = new ArrayList<Double>();

		System.out.println("\nWithout Fibonacci 10 times\n");
		for(int i=0;i<10; i++){
			System.out.println();
			System.out.print("Run #" + i);
			this.executeFineLockHelper(false);
			System.out.print(" Time -> " + (this.endTime - this.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(this.endTime - this.startTime);
		}
		LoadDataStructure.printStats(runningTime);

		runningTime = new ArrayList<Double>();
		System.out.println("\nWith Fibonacci 10 times\n");
		for(int i=0;i<10; i++){
			System.out.println();
			System.out.print("Run #" + i);
			this.executeFineLockHelper(true);
			System.out.print(" Time -> " + (this.endTime - this.startTime)+ "\n");
			LoadDataStructure.printAverage(this.accumulationRecord);
			System.out.println();
			runningTime.add(this.endTime - this.startTime);
		}
		LoadDataStructure.printStats(runningTime);

	}
}

